from pathlib import Path
from streamlit.testing.v1 import AppTest
from tests.test_trace_data import seed_trace
from src.trace_data import freeze_demo,load_demo
from src.pilot_inventory import build_inventory
import pyarrow.parquet as pq


def test_trace_cases_historical_selection_and_missing_delay(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path);freeze_demo(cfg,raw,parquet);build_inventory(cfg)
    script=f'from src.trace_view import render_trace\nrender_trace({cfg!r})'
    app=AppTest.from_string(script).run()
    assert not app.exception and not app.error
    assert any(m.label=='Observaciones en Parquet' and m.value=='5' for m in app.metric)
    app.radio(key='trace_case').set_value('Viaje que requiere normalizar').run()
    assert not app.exception and not app.error
    assert any('1 valores cero' in warning.value for warning in app.warning)
    app.radio(key='trace_mode').set_value('Explorar el histórico').run()
    assert not app.exception and not app.error
    assert app.selectbox(key='trace_day').value=='2026-10-27'


def test_modified_demo_is_blocked(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path);freeze_demo(cfg,raw,parquet)
    _,paths=load_demo(cfg)
    data=pq.ParquetFile(paths['parquet']).read().to_pandas()
    data.loc[0,'arrival_delay']=999
    data.to_parquet(paths['parquet'],index=False)
    app=AppTest.from_string(f'from src.trace_view import render_trace\nrender_trace({cfg!r})').run()
    assert not app.exception
    assert any('huellas' in e.value for e in app.error)
    assert len(app.success)==0


def test_unprepared_demo_keeps_page_available(tmp_path):
    cfg={'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}
    app=AppTest.from_string(f'from src.trace_view import render_trace\nrender_trace({cfg!r})').run()
    assert not app.exception
    assert any('aún no está preparado' in info.value for info in app.info)
