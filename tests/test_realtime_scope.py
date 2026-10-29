"""Capturas alternas por núcleo: fecha explícita y ningún retroceso por calidad."""
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest
from src.dashboard_data import FEEDS,build_dashboard_connection,recent_files
from src.query import snapshot_summary,delay_audit,delays_by_trip,nucleus_current,nucleus_top_delays
from tests.test_demo_adjustments import connection
from tests.test_dashboard_views import seed


def alternating_connection():
    c=connection()
    c.register('trip_updates',pd.DataFrame({
        'snapshot_ts':[1000,1000,1025], 'entity_id':['m1','m2','a'],
        'trip_id':['M','M','A'],'stop_id':['s']*3,'arrival_delay':[180,0,120]}))
    c.register('vehicle_positions',pd.DataFrame({
        'snapshot_ts':[1000,1025],'entity_id':['m','a'],'trip_id':['M','A']}))
    return c


def test_interleaved_nuclei_keep_their_own_snapshot_and_audit():
    c=alternating_connection()
    try:
        m=snapshot_summary(c,'10').iloc[0]
        assert (m.snapshot_ts,m.total_observaciones,m.trenes,m.con_retraso)==(1000,2,2,1)
        assert snapshot_summary(c,'20').iloc[0].snapshot_ts==1025
        assert snapshot_summary(c).iloc[0].total_observaciones==1
        assert delay_audit(c,ncode='10')['total']==2
        assert delays_by_trip(c,ncode='10').retraso_seg.tolist()==[180]
        assert nucleus_top_delays(c,'10').trip_id.tolist()==['M']
        current=nucleus_current(c).set_index('ncode')
        assert current.loc['10','validas']==2
        assert current.loc['10','circulando']==1
        assert current.loc['10','positions_ts']==1000
        assert current.loc['10','delays_ts']==1000
        assert current.loc['20','delays_ts']==1025
    finally:c.close()


def test_latest_scoped_snapshot_is_selected_before_quality_filter():
    c=alternating_connection()
    c.register('trip_updates',pd.DataFrame({
        'snapshot_ts':[1000,1010,1010,1025],'entity_id':list('abcd'),
        'trip_id':['M','M','M','A'],'stop_id':['s']*4,'arrival_delay':[180,90000,None,120]}))
    try:
        m=snapshot_summary(c,'10').iloc[0]
        assert (m.snapshot_ts,m.total_observaciones,m.trenes,m.fuera_rango,m.sin_delay)==(1010,2,0,1,1)
        assert delays_by_trip(c,ncode='10').empty
        assert nucleus_top_delays(c,'10').empty
        assert nucleus_current(c).set_index('ncode').loc['10','validas']==0
        assert delay_audit(c,ncode='10')['total']==2
    finally:c.close()


@pytest.mark.parametrize('gap,available',[(600,True),(601,False)])
def test_scope_cannot_reuse_an_unbounded_old_capture(gap,available):
    c=alternating_connection()
    for table in ('trip_updates','vehicle_positions'):
        df=c.execute(f'SELECT * FROM {table}').df()
        df.loc[df.trip_id=='A','snapshot_ts']=1000+gap
        c.unregister(table);c.register(table,df)
    try:
        m=snapshot_summary(c,'10').iloc[0]
        assert bool(m.total_observaciones)==available
        assert ('10' in nucleus_current(c).ncode.tolist())==available
        if not available:
            assert pd.isna(m.snapshot_ts)
            assert delay_audit(c,ncode='10')['total']==0
    finally:c.close()


def test_recent_window_crosses_midnight_without_loading_whole_archive(tmp_path):
    latest=datetime(2026,10,29,0,0,25,tzinfo=timezone.utc)
    for feed in FEEDS:
        for seconds in (0,25,600,601):
            ts=latest-timedelta(seconds=seconds)
            folder=tmp_path/feed/('date='+ts.strftime('%Y-%m-%d'));folder.mkdir(parents=True,exist_ok=True)
            pd.DataFrame({'snapshot_ts':[int(ts.timestamp())]}).to_parquet(folder/ts.strftime('part-%Y%m%dT%H%M%SZ.parquet'))
    cfg={'storage':{'bronze_dir':str(tmp_path)}}
    c=build_dashboard_connection(cfg,None)
    try:
        expected=[int((latest-timedelta(seconds=s)).timestamp()) for s in (600,25,0)]
        assert c.execute('SELECT snapshot_ts FROM trip_updates ORDER BY snapshot_ts').fetchnumpy()['snapshot_ts'].tolist()==expected
        assert c.execute('SELECT count(*) FROM alerts').fetchone()[0]==1
    finally:c.close()


@pytest.mark.parametrize('offset,expected',[(30,'2'),(640,'Sin datos')])
def test_dashboard_reports_latest_madrid_capture_when_next_feed_omits_it(tmp_path,offset,expected):
    cfg=seed(tmp_path)
    folder=Path(cfg['storage']['bronze_dir'])/'trip_updates/date=2026-10-27'
    latest=pd.read_parquet(next(folder.glob('*.parquet'))).iloc[[2]].copy()
    latest['trip_id']='B';latest['snapshot_ts']=1793086210+offset
    name=datetime.fromtimestamp(1793086210+offset,timezone.utc).strftime('part-%Y%m%dT%H%M%SZ.parquet')
    latest.to_parquet(folder/name,index=False)
    script=Path(__file__).resolve().parents[1]/'dashboard.py'
    st.cache_resource.clear()
    with patch('src.config.load',return_value=cfg):
        app=AppTest.from_file(str(script),default_timeout=15).run()
        app.radio(key='selected_view').set_value('🔴 Red en vivo').run()
        assert not app.exception
        assert any(m.label=='Observaciones evaluadas' and m.value==expected for m in app.metric)
        if offset==30:
            assert any('no incluye este núcleo' in info.value for info in app.info)
        else:
            assert any('Sin datos' in info.value for info in app.info)
        app.selectbox(key='dashboard_nucleus').set_value('Asturias').run()
        assert not app.exception
        assert any(m.label=='Observaciones evaluadas' and m.value=='1' for m in app.metric)
    st.cache_resource.clear()
