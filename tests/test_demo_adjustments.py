from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import duckdb
import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2
from src.query import (snapshot_summary,delay_audit,delays_by_trip,punctuality_over_time,
                       network_heatmap,nucleus_current,nucleus_history,nucleus_top_delays,active_alerts)
from src.presentation import MADRID,period_bounds
from src.downloader import FeedDownloader
from src.capture_status import read_status
from src.quality_data import operational_gaps,audit_unpaired


def connection():
    c=duckdb.connect();c.execute("SET TimeZone='Europe/Madrid'")
    c.register('trip_updates',pd.DataFrame({'snapshot_ts':[100]*7,
        'entity_id':list('abcdefg'),'trip_id':['M']*5+['A','A'],
        'stop_id':['s']*7,'arrival_delay':[-90000,-300,0,180,7200,180,7201]}))
    c.register('trips',pd.DataFrame({'trip_id':['M','A'],'route_id':['10T001C1','20T001C1']}))
    c.register('routes',pd.DataFrame({'route_id':['10T001C1','20T001C1'],
        'route_short_name':['C1','C1'],'nucleus_code':['10','20']}))
    c.register('stops',pd.DataFrame({'stop_id':['s'],'stop_name':['Parada'],'stop_lat':['40'],'stop_lon':['-3']}))
    c.register('vehicle_positions',pd.DataFrame({'snapshot_ts':[100,100],'entity_id':['v1','v2'],'trip_id':['M','A']}))
    return c


def test_filter_boundaries_denominators_and_nucleus():
    c=connection()
    try:
        s=snapshot_summary(c,'10').iloc[0]
        assert (s.total_observaciones,s.trenes,s.fuera_rango,s.con_retraso,s.a_tiempo)==(5,4,1,2,2)
        assert s.retraso_medio_min==61.5
        assert snapshot_summary(c,filtered=False).iloc[0].trenes==7
        assert delay_audit(c,ncode='10')['excluidas']==1
        assert delay_audit(c,ncode=['10','20'])['excluidas']==2
        assert delay_audit(c,ncode=[])['total']==0
        assert delays_by_trip(c,ncode='10').trip_id.tolist()==['M','M']
        trend=punctuality_over_time(c,0,200,'10').iloc[0]
        assert trend.trenes==4 and trend.pct_puntual==50
        assert network_heatmap(c,0,200,'10').iloc[0].n_obs==4
        current=nucleus_current(c).set_index('ncode')
        assert current.loc['10','validas']==4 and current.loc['20','excluidas']==1
        assert nucleus_history(c,0,200).n_obs.sum()==5
        assert nucleus_top_delays(c,'10').trip_id.eq('M').all()
    finally:c.close()


def test_latest_snapshot_with_no_valid_delay_does_not_fall_back():
    c=duckdb.connect()
    c.register('trip_updates',pd.DataFrame({'snapshot_ts':[1,2,2],'trip_id':['M']*3,'arrival_delay':[180,90000,None]}))
    try:
        s=snapshot_summary(c).iloc[0]
        assert s.snapshot_ts==2 and s.trenes==0 and s.total_observaciones==2
        assert pd.isna(s.retraso_medio_min) and pd.isna(s.retraso_max_min)
        assert (s.fuera_rango,s.sin_delay)==(1,1)
    finally:c.close()


def test_calendar_bounds_respect_madrid_and_dst():
    for day,hours in ((datetime(2026,3,30,12,tzinfo=MADRID),23),
                      (datetime(2026,10,26,12,tzinfo=MADRID),25)):
        start,end=period_bounds('Ayer',now=day)
        assert end-start+1==hours*3600
        assert datetime.fromtimestamp(start,MADRID).hour==0
        a,b=period_bounds('Últimas 24h',now=day)
        assert b-a==86400


def test_window_includes_gap_that_starts_before_service():
    def ts(value):return int(datetime.fromisoformat(value).replace(tzinfo=MADRID).timestamp())
    gap={'feed':'vehicle_positions','start':ts('2026-10-28T05:00'),'end':ts('2026-10-28T08:00'),'seconds':10800}
    assert operational_gaps([gap])[0]['operational_seconds']==9000
    gap.update(start=ts('2026-10-28T01:00'),end=ts('2026-10-28T05:00'),seconds=14400)
    assert operational_gaps([gap])[0]['operational_seconds']==0


def test_alerts_exclude_old_future_and_expired():
    c=duckdb.connect()
    c.register('alerts',pd.DataFrame({'snapshot_ts':[1000]*4,'entity_id':['live','future','expired','open'],
        'informed_routes':['10TC1']*4,'active_start':[900,1100,800,None],
        'active_end':[1100,None,1000,None],'description_text':['text']*4}))
    try:
        assert set(active_alerts(c,1000).entity_id)=={'live','open'}
        assert active_alerts(c,2000).empty
    finally:c.close()


def test_download_status_distinguishes_fetch_and_source(tmp_path,vp_pb):
    dl=FeedDownloader('vehicle_positions','https://example.test/feed',tmp_path/'raw',{})
    response=MagicMock();response.content=vp_pb.read_bytes()
    with patch.object(dl._session,'get',return_value=response):
        assert dl.fetch() is not None
        first=read_status(tmp_path/'raw','vehicle_positions')
        assert dl.fetch() is None
    second=read_status(tmp_path/'raw','vehicle_positions')
    assert first['source_timestamp']==second['source_timestamp']
    assert second['content_sha256']==first['content_sha256'] and second['last_success']>=first['last_success']
    with patch.object(dl._session,'get',side_effect=requests.ConnectionError('fallo')):
        assert dl.fetch() is None
    failed=read_status(tmp_path/'raw','vehicle_positions')
    assert failed['last_success']==second['last_success'] and failed['error']=='fallo'
    dl.close()


def test_unpaired_audit_reads_content_and_preserves_originals(tmp_path,vp_pb):
    empty=gtfs_realtime_pb2.FeedMessage();empty.header.gtfs_realtime_version='2.0';empty.header.timestamp=1
    a=tmp_path/'empty.pb';a.write_bytes(empty.SerializeToString())
    b=tmp_path/'broken.pb';b.write_bytes(b'broken')
    paths=[a,b,vp_pb];before=[p.read_bytes() for p in paths]
    result=audit_unpaired(paths,'vehicle_positions')
    assert (result['empty'],result['with_records'],result['invalid'])==(1,1,1)
    assert [p.read_bytes() for p in paths]==before
