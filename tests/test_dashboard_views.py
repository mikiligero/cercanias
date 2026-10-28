"""La navegación completa respeta el ámbito compartido y tolera capturas vacías."""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import zipfile
import pandas as pd
from streamlit.testing.v1 import AppTest
from tests.test_trace_data import seed_trace
from src.pilot_inventory import build_inventory
from src.trace_data import freeze_demo


def seed(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path)
    with zipfile.ZipFile(gtfs,'w') as z:
        z.writestr('trips.txt','trip_id,route_id,service_id\nA,10T001C1,WK\nB,20T001C1,WK\n')
        z.writestr('routes.txt','route_id,route_short_name,route_long_name\n10T001C1,C1,Madrid\n20T001C1,C1,Asturias\n')
        z.writestr('stops.txt','stop_id,stop_name,stop_lat,stop_lon\nS,Estacion,40,-3\nS2,Otra,40.1,-3.1\n')
    for feed in ('vehicle_positions','alerts'):
        folder=tmp_path/'bronze'/feed/'date=2026-10-27';folder.mkdir(parents=True)
        if feed=='vehicle_positions':
            df=pd.DataFrame({'snapshot_ts':[1793086210]*2,'trip_id':['A','B'],'entity_id':['v1','v2']})
        else:
            df=pd.DataFrame({'snapshot_ts':[1791358372],'entity_id':['old'],'informed_routes':['10T001C1'],
                             'active_start':[1791358000],'active_end':[None],'description_text':['Antiguo']})
        df.to_parquet(folder/'part-20261027T073010Z.parquet',index=False)
    freeze_demo(cfg,raw,parquet)
    build_inventory(cfg)
    return cfg


def test_dashboard_navigation_shared_nucleus_and_stale_alerts(tmp_path):
    cfg=seed(tmp_path)
    script=Path(__file__).resolve().parents[1]/'dashboard.py'
    with patch('src.config.load',return_value=cfg), patch('src.quality_view.service_state',return_value='No comprobado'):
        app=AppTest.from_file(str(script),default_timeout=15).run()
        assert not app.exception
        app.radio(key='selected_view').set_value('🔴 Red en vivo').run()
        assert not app.exception
        assert app.selectbox(key='dashboard_nucleus').value=='Madrid'
        assert any('desactualizada' in w.value for w in app.warning)
        assert any(m.label=='Observaciones evaluadas' and m.value=='2' for m in app.metric)
        app.selectbox(key='dashboard_nucleus').set_value('Asturias').run()
        assert not app.exception
        assert any(m.label=='Observaciones evaluadas' and m.value=='1' for m in app.metric)
        for view in ('🗺️ Por núcleo','📈 Tendencia del día'):
            app.radio(key='selected_view').set_value(view).run()
            assert not app.exception
            assert app.selectbox(key='dashboard_nucleus').value=='Asturias'
            app.selectbox(key='period_preset').set_value('Ayer').run()
            assert not app.exception
        for view in ('🔎 Del dato al indicador','⚙️ Sistema y calidad','📊 El piloto en cifras'):
            app.radio(key='selected_view').set_value(view).run()
            assert not app.exception and not app.error
