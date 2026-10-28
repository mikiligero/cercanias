"""Página para seleccionar y descargar el histórico conservado de Anticipa."""
from datetime import date,timedelta
import os
from pathlib import Path
import pandas as pd
import streamlit as st
from src.data_downloads import download_url,downloader_script
from src.pilot_inventory import build_inventory,read_inventory
from src.pilot_view import LABELS,_local,_number,_size

FORMAT_LABELS={'Bruto · mensajes originales (.pb)':'raw','Procesado · Parquet (.parquet)':'parquet'}


def export_rows(inv,formats,feeds,start,end):
    """Recuentos del inventario; la selección exacta se fija al iniciar cada ZIP."""
    result={}
    for item in inv.get('partitions',{}).values():
        if item['feed'] not in feeds or not str(start)<=item['date']<=str(end):
            continue
        row=result.setdefault(item['date'],{'day':item['date'],'files':0,'bytes':0,'raw_files':0,'parquet_files':0})
        for fmt in formats:
            row['files']+=item[fmt+'_files']
            row['bytes']+=item[fmt+'_bytes']
            row[fmt+'_files']+=item[fmt+'_files']
    return sorted((row for row in result.values() if row['files']),key=lambda row:row['day'],reverse=True)


def render_downloads(cfg):
    base=st.get_option('server.baseUrlPath') or ''
    base='/'+base.strip('/') if base else ''
    st.header('Descargar datos almacenados')
    st.write('Elige qué información necesitas y las fechas. Cada ZIP conserva las carpetas del proyecto e incluye instrucciones y un manifiesto de los archivos.')
    st.caption('Los archivos cubren toda la red capturada. El filtro Madrid del cuadro de mando no recorta los originales. Los períodos de descarga se expresan en UTC, igual que las carpetas.')
    inv=read_inventory(cfg)
    refresh=st.button('Actualizar listado de archivos',key='downloads_refresh')
    if refresh or not st.session_state.get('downloads_catalog_checked'):
        try:
            with st.spinner('Revisando las capturas disponibles…'):
                inv=build_inventory(cfg)
            st.session_state['downloads_catalog_checked']=True
        except RuntimeError:
            st.info('El inventario se está actualizando. Se muestra el último recuento disponible.')
    if not inv or not inv.get('partitions'):
        st.info('Todavía no hay datos inventariados para descargar.')
        return
    all_dates=[date.fromisoformat(row['date']) for row in inv['partitions'].values() if row['raw_files'] or row['parquet_files']]
    if not all_dates:
        st.info('No hay archivos conservados.');return
    first,last=min(all_dates),max(all_dates)
    left,right=st.columns(2)
    with left:
        chosen_formats=st.multiselect('Tipo de información',list(FORMAT_LABELS),default=['Procesado · Parquet (.parquet)'],key='downloads_formats')
        formats=[FORMAT_LABELS[value] for value in chosen_formats]
    with right:
        labels={value:key for key,value in LABELS.items()}
        chosen_feeds=st.multiselect('Fuentes',list(labels),default=[LABELS['vehicle_positions'],LABELS['trip_updates']],key='downloads_feeds')
        feeds=[labels[value] for value in chosen_feeds]
    preset=st.selectbox('Período de descarga',['Último día completo disponible','Últimos 7 días','Todo el histórico','Elegir fechas'],key='downloads_period')
    from datetime import datetime,timezone
    available=[date.fromisoformat(row['day']) for row in export_rows(inv,formats,feeds,first,last)] if formats and feeds else []
    completed=[day for day in available if day<datetime.now(timezone.utc).date()]
    start=end=max(completed or available or [first])
    if not completed and available and preset=='Último día completo disponible':
        st.info('Solo hay archivos del día en curso para esta selección; su contenido es parcial.')
    if preset=='Últimos 7 días':start,end=max(first,last-timedelta(days=6)),last
    elif preset=='Todo el histórico':start,end=first,last
    elif preset=='Elegir fechas':
        selected=st.date_input('Fechas inicial y final (UTC)',value=(start,end),min_value=first,max_value=last,key='downloads_dates')
        if len(selected)!=2:
            st.info('Selecciona las fechas inicial y final.');return
        start,end=selected
    st.caption(f'Del {start:%d/%m/%Y} al {end:%d/%m/%Y}, ambos incluidos · días UTC completos. Inventario del '+_local(inv['generated_at'])+' (Madrid).')
    enabled=os.environ.get('ANTICIPA_DOWNLOADS_ENABLED')=='1'
    if not enabled:
        st.info('Las descargas no están habilitadas en esta instalación. La guía DESCARGAR_DATOS.md del repositorio explica cómo activarlas.')
    rows=export_rows(inv,formats,feeds,start,end) if formats and feeds else []
    if not formats or not feeds:
        st.info('Elige al menos un tipo de información y una fuente.')
    elif not rows:
        st.info('No hay archivos de esas fuentes y formatos en el período seleccionado. La ausencia de capturas no demuestra que no existieran incidencias.')
    else:
        a,b,c=st.columns(3)
        a.metric('Días con archivos',_number(len(rows)))
        b.metric('Archivos inventariados',_number(sum(row['files'] for row in rows)))
        c.metric('Tamaño antes de comprimir',_size(sum(row['bytes'] for row in rows)))
        st.caption('Se genera un ZIP por día para que puedas descargar períodos largos por partes. Los tamaños son anteriores a la compresión; el día en curso es parcial. Al empezar cada descarga se fijan los archivos estables que existen en ese momento.')
        if enabled:
            st.link_button('Descargar ZIP del '+rows[0]['day'],download_url(rows[0]['day'],formats,feeds,base=base),type='primary')
        table=pd.DataFrame([{'Fecha (UTC)':row['day'],'Archivos':row['files'],
            'Brutos':row['raw_files'],'Parquet':row['parquet_files'],'Tamaño':_size(row['bytes']),
            'Descarga':download_url(row['day'],formats,feeds,base=base) if enabled else None} for row in rows])
        st.dataframe(table,use_container_width=True,hide_index=True,column_config={
            'Descarga':st.column_config.LinkColumn('Paquete del día',display_text='Descargar ZIP')})
        if enabled:
            st.markdown('**Descargar todo el período elegido**')
            st.write('Si son varios días, este pequeño programa descarga sus ZIP de uno en uno. Si lo vuelves a ejecutar, conserva los paquetes que ya se hayan descargado completos.')
            include_gtfs=st.checkbox('Añadir también la referencia GTFS disponible',value=True,key='downloads_script_gtfs')
            script=downloader_script([row['day'] for row in rows],formats,feeds,include_gtfs=include_gtfs)
            st.download_button('Descargar programa para obtener todo el período',script.encode('utf-8'),
                file_name='descargar_anticipa.py',mime='text/x-python',key='downloads_script')
            st.code('python descargar_anticipa.py','bash')
            st.caption('Requiere Python 3.10 o posterior. Guarda los ZIP en la carpeta descargas_anticipa, junto al lugar desde donde lo ejecutes.')
            st.caption('Para actualizar un ZIP del día en curso, borra ese paquete de tu carpeta local antes de ejecutar de nuevo el programa.')
    st.divider()
    st.subheader('Referencia de líneas y estaciones · GTFS estático')
    gtfs=Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'
    if gtfs.exists():
        from datetime import datetime,timezone
        when=datetime.fromtimestamp(gtfs.stat().st_mtime,timezone.utc).isoformat()
        st.write('Copia disponible: '+_local(when)+' (Madrid) · '+_size(gtfs.stat().st_size)+'.')
        st.caption('Se descarga una sola vez. Esta copia puede ser posterior al período que elijas y no sustituye las versiones históricas del horario.')
        if enabled:
            st.link_button('Descargar referencia GTFS',download_url(kind='gtfs',base=base),use_container_width=False)
    else:
        st.info('No hay referencia GTFS conservada en esta instalación.')
    st.divider()
    _instructions()


def _instructions():
    st.subheader('Dónde colocar lo descargado')
    st.markdown('1. Descarga o clona el [proyecto Cercanías en GitHub](https://github.com/mikiligero/cercanias).\n'
                '2. Descomprime cada ZIP **en la carpeta principal del proyecto**, junto a `dashboard.py` y `config.yaml`.\n'
                '3. Combina las carpetas `data/` de los distintos paquetes y conserva los nombres internos. Cada ZIP incluye `LEEME-DATOS.md` y `manifest.json` con las rutas y huellas de los archivos.')
    st.code('''cercanias/
├── dashboard.py
├── config.yaml
└── data/
    ├── raw/
    │   ├── vehicle_positions/AAAA-MM-DD/*.pb
    │   ├── trip_updates/AAAA-MM-DD/*.pb
    │   ├── alerts/AAAA-MM-DD/*.pb
    │   └── gtfs_static/fomento_transit.zip
    └── bronze/
        ├── vehicle_positions/date=AAAA-MM-DD/*.parquet
        ├── trip_updates/date=AAAA-MM-DD/*.parquet
        └── alerts/date=AAAA-MM-DD/*.parquet''',language='text')
    st.write('Si usas Python para descomprimir un paquete, entra primero en la carpeta del proyecto:')
    st.code('python -m zipfile -e /ruta/al/paquete.zip .','bash')
    with st.expander('Cómo consultar los archivos y abrir el cuadro de mando'):
        st.write('Puedes analizar un Parquet directamente con pandas o DuckDB. Para abrir el cuadro completo necesitas Parquet de las tres fuentes y la referencia GTFS. Si no hay avisos en el período, descarga también un día que tenga archivos de avisos; el panel conservará su fecha y avisará si están desactualizados.')
        st.code('python -m venv venv\n# Activa el entorno según tu sistema\npip install -r requirements.txt\npython -m src.pilot_inventory\npython serve_dashboard.py','bash')
        st.caption('En macOS/Linux: source venv/bin/activate. En Windows PowerShell: .\\venv\\Scripts\\Activate.ps1. La guía README.md contiene los pasos de puesta en marcha.')
    with st.expander('Si has descargado solamente el bruto (.pb)'):
        st.write('Necesitas generar los Parquet antes de abrir sus indicadores. El repositorio incluye un reprocesado local que conserva los originales y omite Parquet que ya existan:')
        st.code('python reprocess_downloads.py --feed trip_updates --start 2026-10-01 --end 2026-10-07','bash')
        st.caption('Sustituye la fuente y las fechas por las descargadas. Fuentes: vehicle_positions, trip_updates y alerts. Las fechas son UTC. Consulta DESCARGAR_DATOS.md para más opciones.')
