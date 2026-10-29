"""Descarga directa del período elegido, en un único ZIP."""
from datetime import date,datetime,timedelta,timezone
import os
from pathlib import Path
import streamlit as st
from src.data_downloads import period_url
from src.pilot_inventory import build_inventory,read_inventory
from src.pilot_view import _size

FORMATS={'Parquet':['parquet'],'Bruto (.pb)':['raw'],'Ambos':['raw','parquet']}
SOURCES={'Posiciones':'vehicle_positions','Actualizaciones de viajes':'trip_updates','Avisos':'alerts'}
PERIODS=['Hoy','Última semana','Último mes','Datos piloto EDA','Personalizado']


def today_utc():
    return datetime.now(timezone.utc).date()


def preset_dates(period,today):
    if period=='Hoy':return today,today
    if period=='Última semana':return today-timedelta(days=6),today
    if period=='Último mes':return today-timedelta(days=29),today
    if period=='Datos piloto EDA':return date(2026,5,17),date(2026,7,3)
    raise ValueError('Período no predefinido')


def export_rows(inv,formats,feeds,start,end):
    result={}
    for item in inv.get('partitions',{}).values():
        if item['feed'] not in feeds or not str(start)<=item['date']<=str(end):continue
        row=result.setdefault(item['date'],dict(day=item['date'],files=0,bytes=0,raw_files=0,parquet_files=0))
        for fmt in formats:
            row['files']+=item[fmt+'_files'];row['bytes']+=item[fmt+'_bytes']
            row[fmt+'_files']+=item[fmt+'_files']
    return sorted((row for row in result.values() if row['files']),key=lambda row:row['day'],reverse=True)


def remember_custom_dates():
    st.session_state['download_custom_dates']=st.session_state['download_period_simple']


def render_downloads(cfg):
    st.header('Descargar datos')
    st.write('Elige el formato y las fechas. Se descargará un único ZIP con todo el período.')
    inv=read_inventory(cfg)
    if not st.session_state.get('downloads_catalog_checked'):
        try:
            with st.spinner('Consultando los datos disponibles…'):inv=build_inventory(cfg)
            st.session_state['downloads_catalog_checked']=True
        except RuntimeError:pass
    if not inv or not inv.get('partitions'):
        st.info('Todavía no hay datos disponibles para descargar.');return
    days=[date.fromisoformat(r['date']) for r in inv['partitions'].values() if r['raw_files'] or r['parquet_files']]
    if not days:
        st.info('No hay archivos conservados.');return
    first,last=min(days),max(days)
    format_label=st.radio('Formato',list(FORMATS),horizontal=True,key='download_format_simple')
    period=st.selectbox('Período',PERIODS,index=1,key='download_preset')
    if period=='Personalizado':
        default_end=min(today_utc(),last)
        default_end=max(first,default_end)
        default_start=max(first,default_end-timedelta(days=6))
        selected=st.date_input('Desde / hasta',value=st.session_state.get('download_custom_dates',(default_start,default_end)),min_value=first,max_value=last,key='download_period_simple',format='DD/MM/YYYY',on_change=remember_custom_dates)
        st.session_state['download_custom_dates']=selected
    else:
        selected=preset_dates(period,today_utc())
        start,end=selected
        st.caption(f'{start:%d/%m/%Y} → {end:%d/%m/%Y}')
        if period=='Última semana':st.caption('Últimos 7 días, incluido hoy.')
        elif period=='Último mes':st.caption('Últimos 30 días, incluido hoy.')
    st.caption('Fechas UTC, como las carpetas de los archivos. El día en curso es parcial.')
    gtfs=Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'
    with st.expander('Elegir fuentes y referencia GTFS'):
        source_labels=st.multiselect('Información incluida',list(SOURCES),default=list(SOURCES),key='download_sources_simple')
        include_gtfs=st.checkbox('Incluir la referencia de líneas y estaciones (GTFS)',value=gtfs.exists(),disabled=not gtfs.exists(),key='download_reference_simple')
        st.caption('Se incluyen los archivos de toda la red capturada. La copia GTFS disponible puede ser posterior a las fechas elegidas.')
    if len(selected)!=2:
        st.info('Selecciona la fecha inicial y la final.')
    elif not source_labels:
        st.info('Selecciona al menos una fuente en «Elegir fuentes y referencia GTFS».')
    else:
        start,end=selected
        formats=FORMATS[format_label];feeds=[SOURCES[label] for label in source_labels]
        rows=export_rows(inv,formats,feeds,start,end)
        if not rows:
            st.info('No hay archivos de esa información en las fechas elegidas.')
        elif os.environ.get('ANTICIPA_DOWNLOADS_ENABLED')!='1':
            st.info('Las descargas no están habilitadas en esta instalación. Consulta DESCARGAR_DATOS.md del proyecto.')
        else:
            size=sum(row['bytes'] for row in rows)+(gtfs.stat().st_size if include_gtfs and gtfs.exists() else 0)
            st.write(f'**{start:%d/%m/%Y} → {end:%d/%m/%Y}** · {_size(size)} antes de comprimir')
            base=st.get_option('server.baseUrlPath') or ''
            base='/'+base.strip('/') if base else ''
            st.link_button('Descargar ZIP',period_url(str(start),str(end),formats,feeds,include_gtfs,base),type='primary',use_container_width=True)
            st.caption('La descarga se genera al momento. Si eliges muchos días, puede tardar; mantén la conexión hasta que termine.')
    with st.expander('Dónde colocar los archivos'):
        st.markdown('Descomprime el ZIP **en la carpeta principal del proyecto Cercanías**, junto a `dashboard.py` y `config.yaml`. Conserva la carpeta `data/` y sus nombres internos.')
        st.code('''cercanias/
├── dashboard.py
├── config.yaml
└── data/
    ├── raw/       ← mensajes originales (.pb)
    └── bronze/    ← archivos Parquet''',language='text')
        st.write('El ZIP incluye instrucciones completas y un manifiesto de los archivos. Si has elegido bruto, la guía explica cómo generar los Parquet.')
        st.markdown('[Guía del proyecto y puesta en marcha](https://github.com/mikiligero/cercanias/blob/main/DESCARGAR_DATOS.md)')
