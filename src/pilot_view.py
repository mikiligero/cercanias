"""Vista de evidencias de captura para la demostración del piloto."""
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import plotly.express as px
import streamlit as st
from src.pilot_inventory import build_inventory, read_inventory

LABELS = {'vehicle_positions': 'Posiciones de trenes',
          'trip_updates': 'Actualizaciones de viajes', 'alerts': 'Alertas del operador'}
COLORS = {'Posiciones de trenes': '#087f8c', 'Actualizaciones de viajes': '#4375b5',
          'Alertas del operador': '#d58b27'}


def _number(value):
    return f'{int(value):,}'.replace(',', '.')


def _size(value):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if value < 1000 or unit == 'TB':
            return f'{value:.1f}'.replace('.', ',') + ' ' + unit
        value /= 1000


def _local(value):
    return datetime.fromisoformat(value).astimezone(ZoneInfo('Europe/Madrid')).strftime('%d/%m/%Y %H:%M')


def render_pilot(cfg):
    st.header('El piloto en cifras')
    st.write('Un histórico propio construido a partir de las fuentes públicas de Cercanías Renfe.')
    inventory = read_inventory(cfg)
    if st.session_state.get('refresh_data', False):
        try:
            with st.spinner('Revisando las capturas nuevas…'):
                inventory = build_inventory(cfg)
        except RuntimeError as exc:
            st.info(str(exc))
    if inventory is None:
        st.info('El primer inventario está en preparación. Las otras vistas siguen disponibles.')
        return
    st.caption('Inventario calculado el ' + _local(inventory['generated_at']) + ' · hora de Madrid. Actualización manual.')
    data = pd.DataFrame(inventory['partitions'].values())
    if data.empty:
        st.info('Todavía no hay archivos de captura inventariados.')
        return
    data['date'] = pd.to_datetime(data['date'])
    first_day, last_day = data['date'].min().date(), data['date'].max().date()
    scope = st.radio('Período del inventario',
                     ['Todo el histórico', 'Período del EDA de la memoria', 'Elegir fechas'],
                     horizontal=True, key='pilot_scope')
    if scope == 'Período del EDA de la memoria':
        data = data[data['date'].between('2026-05-17', '2026-07-03')]
        st.caption('17/05–03/07/2026 · recuento actual de los archivos conservados en ese período; puede diferir del corte original del análisis.')
    elif scope == 'Elegir fechas':
        dates = st.date_input('Fechas de captura (UTC)', value=(first_day, last_day),
                              min_value=first_day, max_value=last_day, key='pilot_dates')
        if len(dates) != 2:
            st.info('Selecciona una fecha de inicio y otra de fin.')
            return
        data = data[data['date'].between(pd.Timestamp(dates[0]), pd.Timestamp(dates[1]))]
    data = data[(data['raw_files'] > 0) | (data['parquet_files'] > 0)]
    if data.empty:
        st.info('No hay capturas conservadas en estas fechas.')
        return
    starts = [x for x in data['first'] if pd.notna(x)]
    ends = [x for x in data['last'] if pd.notna(x)]
    if starts and ends:
        st.info(f'Primera captura: {_local(min(starts))}  ·  Última captura: {_local(max(ends))}  ·  hora de Madrid')
    metrics = st.columns(4)
    metrics[0].metric('Días con capturas', _number(data['date'].nunique()))
    metrics[1].metric('Snapshots conservados', _number(data['raw_files'].sum()))
    metrics[2].metric('Filas procesadas', _number(data['rows'].sum()))
    metrics[3].metric('Histórico almacenado', _size(data['raw_bytes'].sum() + data['parquet_bytes'].sum()))
    st.caption('Cada snapshot es una captura guardada de una fuente. Las filas son observaciones repetidas de trenes, paradas o alertas; no representan viajes ni viajeros distintos.')

    st.subheader('Evolución de la captura')
    dates = pd.date_range(data['date'].min(), data['date'].max(), freq='D')
    grid = pd.MultiIndex.from_product([dates, LABELS], names=['date', 'feed'])
    daily = data.set_index(['date', 'feed'])[['raw_files', 'rows', 'raw_bytes', 'parquet_bytes']].reindex(grid, fill_value=0).reset_index()
    daily['Fuente'] = daily['feed'].map(LABELS)
    measure = st.radio('Qué mostrar', ['Snapshots diarios', 'Filas procesadas', 'Volumen acumulado'],
                       horizontal=True, key='pilot_measure')
    if measure == 'Volumen acumulado':
        daily = daily.sort_values('date')
        daily['bytes'] = daily['raw_bytes'] + daily['parquet_bytes']
        daily['GB'] = daily.groupby('feed')['bytes'].cumsum() / 1e9
        fig = px.area(daily, x='date', y='GB', color='Fuente', color_discrete_map=COLORS,
                      labels={'date': 'Fecha de captura (UTC)', 'GB': 'GB acumulados'})
    else:
        field = 'raw_files' if measure == 'Snapshots diarios' else 'rows'
        fig = px.bar(daily, x='date', y=field, color='Fuente', barmode='stack',
                     color_discrete_map=COLORS,
                     labels={'date': 'Fecha de captura (UTC)', field: measure})
    fig.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=0),
                      legend=dict(orientation='h', y=1.12), hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)
    st.caption('Agrupación por fecha UTC; el día en curso es parcial. Se muestran también los días intermedios sin archivos. Los recuentos reflejan capturas conservadas tras deduplicar; no miden por sí solos la disponibilidad de Renfe ni las peticiones realizadas.')

    st.subheader('Qué aporta cada fuente')
    table = []
    for feed, label in LABELS.items():
        group = data[data['feed'] == feed]
        table.append({'Fuente': label, 'Días con capturas': int(group['date'].nunique()),
                      'Snapshots originales': int(group['raw_files'].sum()),
                      'Parquet legibles': int(group['valid_parquet_files'].sum()),
                      'Filas procesadas': int(group['rows'].sum()),
                      'Originales': _size(group['raw_bytes'].sum()),
                      'Parquet': _size(group['parquet_bytes'].sum())})
    st.dataframe(pd.DataFrame(table), use_container_width=True, hide_index=True)
    errors = int(data['invalid_parquet_files'].sum())
    if errors:
        st.warning(f'{_number(errors)} archivos Parquet no se han podido leer. Sus filas quedan fuera del recuento; el volumen de esos archivos sí se contabiliza.')
    st.caption('El último archivo de una fuente indica su última captura conservada. En alertas, la ausencia de nuevos archivos no demuestra una caída del origen.')

    summary = data[['date', 'feed', 'raw_files', 'parquet_files', 'valid_parquet_files', 'rows',
                    'raw_bytes', 'parquet_bytes', 'invalid_parquet_files']].copy()
    summary = summary.sort_values(['date', 'feed'])
    summary['date'] = summary['date'].dt.strftime('%Y-%m-%d')
    summary['inventory_generated_at'] = inventory['generated_at']
    st.download_button('Descargar resumen diario (CSV)', summary.to_csv(index=False).encode('utf-8-sig'),
                       file_name='anticipa-piloto-resumen-diario.csv', mime='text/csv', key='pilot_csv')
    with st.expander('Cómo se calculan estas cifras'):
        st.write('Los snapshots se cuentan en los archivos originales .pb. Las filas se obtienen de los metadatos de los Parquet legibles, sin consultar todas las observaciones. Los días con capturas son fechas con archivos conservados, no días de funcionamiento continuo.')
        st.write('El volumen suma el tamaño de los originales y de los Parquet de las tres fuentes GTFS-RT. Se expresa en unidades decimales (1 GB = 1.000.000.000 bytes); puede diferir del espacio reservado por el disco. No incluye copias de recuperación, el entorno de Python ni el GTFS estático.')
        st.write('Horario GTFS estático conservado: ' + _size(inventory.get('gtfs_bytes', 0)) + '. Se utiliza como referencia para el cruce de líneas y estaciones.')
        st.write('Al actualizar las cifras se revisan las carpetas modificadas. La fecha del inventario permite conocer hasta cuándo se ha realizado el recuento. Este panel demuestra captura, conservación y análisis; no acredita resultados de predicción ni de avisos propios de Anticipa.')
