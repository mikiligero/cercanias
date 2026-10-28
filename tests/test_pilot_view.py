from pathlib import Path
from unittest.mock import patch
import pandas as pd
from streamlit.testing.v1 import AppTest
from src.pilot_inventory import build_inventory


def _inventory(tmp_path):
    cfg={'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}
    for day, stamp, rows in [('2026-05-17','20260517',3),('2026-07-03','20260703',5),('2026-10-28','20261028',7)]:
        raw=tmp_path/'raw'/'vehicle_positions'/day
        bronze=tmp_path/'bronze'/'vehicle_positions'/('date='+day)
        raw.mkdir(parents=True);bronze.mkdir(parents=True)
        (raw/f'vehicle_positions_{stamp}T120000Z.pb').write_bytes(b'original')
        pd.DataFrame({'snapshot_ts':range(rows)}).to_parquet(bronze/f'part-{stamp}T120000Z.parquet',index=False)
    return build_inventory(cfg)


def test_pilot_scopes_and_chart_choices(tmp_path):
    data=_inventory(tmp_path)
    with patch('src.pilot_view.read_inventory',return_value=data):
        app=AppTest.from_string('from src.pilot_view import render_pilot\nrender_pilot({})').run()
        assert not app.exception
        assert [m.value for m in app.metric][:3]==['3','3','15']
        app.radio(key='pilot_scope').set_value('Período del EDA de la memoria').run()
        assert not app.exception
        assert [m.value for m in app.metric][:3]==['2','2','8']
        for measure in ('Filas procesadas','Volumen acumulado'):
            app.radio(key='pilot_measure').set_value(measure).run()
            assert not app.exception
        app.radio(key='pilot_scope').set_value('Elegir fechas').run()
        assert not app.exception
        app.date_input(key='pilot_dates').set_value((pd.Timestamp('2026-10-28').date(),)*2).run()
        assert not app.exception
        assert [m.value for m in app.metric][:3]==['1','1','7']


def test_pending_inventory_does_not_block_page():
    with patch('src.pilot_view.read_inventory',return_value=None):
        app=AppTest.from_string('from src.pilot_view import render_pilot\nrender_pilot({})').run()
        assert not app.exception
        assert 'preparación' in app.info[0].value


def test_manual_refresh_updates_inventory_once(tmp_path):
    data=_inventory(tmp_path)
    script='import streamlit as st\nfrom src.pilot_view import render_pilot\nst.sidebar.button("Actualizar datos",key="refresh_data")\nrender_pilot({})'
    with patch('src.pilot_view.read_inventory',return_value=data), patch('src.pilot_view.build_inventory',return_value=data) as update:
        app=AppTest.from_string(script).run()
        update.assert_not_called()
        app.button(key='refresh_data').click().run()
        assert not app.exception
        assert update.call_count==1
        app.radio(key='pilot_measure').set_value('Volumen acumulado').run()
        assert not app.exception
        assert update.call_count==1
