import os
from unittest.mock import patch
from streamlit.testing.v1 import AppTest
from tests.test_trace_data import seed_trace
from src.pilot_inventory import build_inventory


def test_page_selection_and_installation_instructions(tmp_path):
    cfg,*_=seed_trace(tmp_path);build_inventory(cfg)
    with patch.dict(os.environ,{'ANTICIPA_DOWNLOADS_ENABLED':'1'}):
        app=AppTest.from_string(f'from src.download_view import render_downloads\nrender_downloads({cfg!r})').run()
        assert not app.exception
        assert any(m.label=='Días con archivos' and m.value=='1' for m in app.metric)
        assert any('Dónde colocar' in title.value for title in app.subheader)
        assert any('data/' in code.value for code in app.code)
        app.selectbox(key='downloads_period').set_value('Elegir fechas').run()
        assert not app.exception
        app.multiselect(key='downloads_formats').set_value([]).run()
        assert not app.exception and any('al menos' in info.value for info in app.info)


def test_plain_streamlit_install_explains_unavailable_downloads(tmp_path):
    cfg,*_=seed_trace(tmp_path);build_inventory(cfg)
    with patch.dict(os.environ,{'ANTICIPA_DOWNLOADS_ENABLED':'0'}):
        app=AppTest.from_string(f'from src.download_view import render_downloads\nrender_downloads({cfg!r})').run()
        assert not app.exception
        assert any('no están habilitadas' in info.value for info in app.info)
