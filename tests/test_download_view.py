import os
from datetime import date
from urllib.parse import parse_qs,urlsplit
import pytest
from unittest.mock import patch
from streamlit.testing.v1 import AppTest
from tests.test_trace_data import seed_trace
from src.pilot_inventory import build_inventory
from src.download_view import preset_dates


@pytest.fixture(autouse=True)
def fixed_today():
    with patch("src.download_view.today_utc",return_value=date(2026,10,28)):
        yield


def test_page_selection_and_installation_instructions(tmp_path):
    cfg,*_=seed_trace(tmp_path);build_inventory(cfg)
    with patch.dict(os.environ,{'ANTICIPA_DOWNLOADS_ENABLED':'1'}):
        app=AppTest.from_string(f'from src.download_view import render_downloads\nrender_downloads({cfg!r})').run()
        assert not app.exception
        assert len(app.radio)==1 and len(app.date_input)==0
        assert len(app.dataframe)==0 and len(app.selectbox)==1
        assert any('Dónde colocar' in element.label for element in app.expander)
        assert any('data/' in code.value for code in app.code)
        app.radio(key='download_format_simple').set_value('Ambos').run()
        assert not app.exception
        app.multiselect(key='download_sources_simple').set_value([]).run()
        assert not app.exception and any('al menos' in info.value for info in app.info)


def test_plain_streamlit_install_explains_unavailable_downloads(tmp_path):
    cfg,*_=seed_trace(tmp_path);build_inventory(cfg)
    with patch.dict(os.environ,{'ANTICIPA_DOWNLOADS_ENABLED':'0'}):
        app=AppTest.from_string(f'from src.download_view import render_downloads\nrender_downloads({cfg!r})').run()
        assert not app.exception
        assert any('no están habilitadas' in info.value for info in app.info)


@pytest.mark.parametrize('period,today,expected',[
    ('Hoy',date(2027,1,1),(date(2027,1,1),date(2027,1,1))),
    ('Última semana',date(2027,1,1),(date(2026,12,26),date(2027,1,1))),
    ('Último mes',date(2028,3,1),(date(2028,2,1),date(2028,3,1))),
    ('Datos piloto EDA',date(2027,1,1),(date(2026,5,17),date(2026,7,3))),
])
def test_preset_calendar_boundaries(period,today,expected):
    assert preset_dates(period,today)==expected


def download_query(app):
    return parse_qs(urlsplit(app.get('link_button')[0].proto.url).query)


def test_presets_update_download_and_keep_custom_dates(tmp_path):
    cfg,*_=seed_trace(tmp_path);build_inventory(cfg)
    with patch.dict(os.environ,{'ANTICIPA_DOWNLOADS_ENABLED':'1'}):
        app=AppTest.from_string(f'from src.download_view import render_downloads\nrender_downloads({cfg!r})').run()
        assert download_query(app)['start']==['2026-10-22']
        assert download_query(app)['end']==['2026-10-28']
        app.selectbox(key='download_preset').set_value('Último mes').run()
        assert download_query(app)['start']==['2026-09-29']
        app.selectbox(key='download_preset').set_value('Hoy').run()
        assert not app.exception
        assert not app.get('link_button')
        assert any('No hay archivos' in info.value for info in app.info)
        app.selectbox(key='download_preset').set_value('Datos piloto EDA').run()
        assert not app.get('link_button')
        assert any('17/05/2026 → 03/07/2026' in caption.value for caption in app.caption)
        app.selectbox(key='download_preset').set_value('Personalizado').run()
        app.date_input(key='download_period_simple').set_value((date(2026,10,27),)).run()
        assert not app.get('link_button')
        assert any('fecha inicial' in info.value for info in app.info)
        app.date_input(key='download_period_simple').set_value((date(2026,10,27),date(2026,10,27))).run()
        assert download_query(app)['start']==['2026-10-27']
        app.selectbox(key='download_preset').set_value('Última semana').run()
        assert download_query(app)['start']==['2026-10-22']
        app.selectbox(key='download_preset').set_value('Personalizado').run()
        assert download_query(app)['start']==['2026-10-27']
        assert not app.exception
