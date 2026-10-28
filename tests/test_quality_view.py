from unittest.mock import patch
from streamlit.testing.v1 import AppTest
from tests.test_quality_data import seed_quality
from src.pilot_inventory import build_inventory
from src.quality_data import analyze_day,build_continuity


def test_quality_view_scopes_and_report(tmp_path):
    cfg,day=seed_quality(tmp_path)
    inv=build_inventory(cfg);continuity=build_continuity(cfg);report=analyze_day(cfg,day)
    script=f'from src.quality_view import render_quality\nrender_quality({cfg!r})'
    with patch('src.quality_view.service_state',return_value='Activo'):
        app=AppTest.from_string(script).run()
        assert not app.exception
        assert any(m.label=='Tras normalizar espacios' for m in app.metric)
        for scope in ('Todo el histórico','Período del EDA','Elegir fechas'):
            app.radio(key='quality_scope').set_value(scope).run()
            assert not app.exception


def test_missing_report_does_not_appear_as_success(tmp_path):
    cfg,day=seed_quality(tmp_path);build_inventory(cfg)
    script=f'from src.quality_view import render_quality\nrender_quality({cfg!r})'
    with patch('src.quality_view.service_state',return_value='No comprobado'):
        app=AppTest.from_string(script).run()
        assert not app.exception
        assert any('no ejecutada' in info.value for info in app.info)
        assert not any(m.label=='Tras normalizar espacios' for m in app.metric)
