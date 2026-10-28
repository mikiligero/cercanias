"""Anticipa · cuadro de mando del piloto. Actualización manual."""
import html
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from src.config import load
from src.pilot_view import render_pilot, _number
from src.quality_view import render_quality
from src.trace_view import render_trace
from src.alerts_view import render_alerts
from src.dashboard_data import build_dashboard_connection
from src.presentation import (MADRID, NUCLEUS_NAMES, PERIOD_OPTIONS, decimal,
                              local_time, madrid_series, period_bounds)
from src.query import (FILTER_DESCRIPTION, snapshot_summary, delay_audit, delays_by_trip,
                       punctuality_over_time, network_heatmap, nucleus_current,
                       nucleus_history, nucleus_top_delays)

VIEWS = ['📊 El piloto en cifras', '🔎 Del dato al indicador', '🔴 Red en vivo',
         '🗺️ Por núcleo', '📈 Tendencia del día', '⚙️ Sistema y calidad']
LOGO = Path(__file__).resolve().parent/'assets/logo-anticipa.png'
st.set_page_config(page_title='Anticipa · Cuadro de mando (prototipo)', page_icon='🚆', layout='wide')


@st.cache_resource
def _load_cfg():
    return load()


@st.cache_resource
def _load_gtfs_static(raw_dir, version):
    path = Path(raw_dir) / 'gtfs_static/fomento_transit.zip'
    result = {}
    with zipfile.ZipFile(path) as z:
        for table, cols in (
            ('trips', ['trip_id', 'route_id', 'service_id']),
            ('routes', ['route_id', 'route_short_name', 'route_long_name']),
            ('stops', ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])):
            with z.open(table+'.txt') as f:
                df = pd.read_csv(f, dtype=str)
            df.columns = df.columns.str.strip()
            result[table] = df[cols].apply(lambda c: c.str.strip()).drop_duplicates()
    result['routes']['nucleus_code'] = result['routes']['route_id'].str.extract(r'^(\d+)T')
    return result


def _semaforo(value):
    if pd.isna(value):
        return '⚪'
    return '🟢' if value <= 2 else '🟡' if value <= 10 else '🔴'


def _name(code):
    return NUCLEUS_NAMES.get(code, 'Núcleo '+str(code))


def _audit(con, start=None, end=None, ncode=None):
    counts = delay_audit(con, start, end, ncode)
    st.caption(FILTER_DESCRIPTION+' '+
        f"Observaciones: {_number(counts['validas'])} incluidas · {_number(counts['excluidas'])} fuera del intervalo · "
        f"{_number(counts['sin_delay'])} sin valor de retraso. Total: {_number(counts['total'])}.")


def _table(df, formats=None):
    st.dataframe(df.style.format(formats or {}, na_rep='Sin datos', decimal=',', thousands='.'),
                 use_container_width=True, hide_index=True)


def _plot(fig):
    fig.update_layout(separators=',.')
    st.plotly_chart(fig, use_container_width=True)


def _live(con, cfg, gtfs, ncode, start, end, preset):
    summary = snapshot_summary(con, ncode).iloc[0]
    st.subheader('Última captura · '+scope_name)
    a,b,c,d,e = st.columns(5)
    a.metric('Observaciones evaluadas', _number(summary['trenes']))
    b.metric('Con retraso positivo', _number(summary['con_retraso']))
    c.metric('Sin retraso (≤0 s)', _number(summary['a_tiempo']))
    d.metric('Media con retraso positivo', decimal(summary['retraso_medio_min'], ' min'))
    e.metric('Máximo incluido', decimal(summary['retraso_max_min'], ' min'))
    stamp = summary['snapshot_ts']
    st.caption('Dato generado el '+local_time(stamp)+' (Madrid). '+
               _number(summary['viajes'])+' identificadores de viaje distintos entre las observaciones evaluadas.')
    if pd.notna(stamp) and datetime.now(MADRID).timestamp()-float(stamp)>120:
        st.warning('El último dato de actualizaciones tiene más de dos minutos. Comprueba la fuente en Sistema y calidad.')
    _audit(con, ncode=ncode)
    if not summary['trenes']:
        st.info('No hay observaciones evaluables en la última captura para este ámbito. Las métricas sin datos no se sustituyen por cero.')
    st.divider()
    st.subheader('Observaciones de retraso · '+preset)
    left,right = st.columns([1,3])
    with left:
        minimum = st.slider('Retraso mínimo (min)', 0, 30, 1, key='t1_min_delay')
        route_df = gtfs['routes'] if gtfs else pd.DataFrame()
        if gtfs and ncode:
            route_df = route_df[route_df['nucleus_code']==ncode]
        lines = sorted(route_df['route_short_name'].dropna().unique()) if gtfs else []
        selected = st.multiselect('Filtrar línea', lines, key='t1_lines')
    with right:
        if gtfs:
            period_con = build_dashboard_connection(cfg, gtfs, start, end, period_latest=True)
            try:
                stamp = period_con.execute('SELECT max(snapshot_ts) FROM trip_updates WHERE snapshot_ts BETWEEN ? AND ?', [start,end]).fetchone()[0]
                if stamp is None:
                    st.info('No hay capturas en el período seleccionado.')
                else:
                    df = delays_by_trip(period_con, stamp, minimum*60, ncode)
                    if selected:
                        df = df[df['linea'].isin(selected)]
                    if df.empty:
                        st.info('No hay observaciones que cumplan estos filtros en la captura seleccionada.')
                    else:
                        df.insert(0, 'Estado', df['retraso_min'].map(_semaforo))
                        _table(df.rename(columns={'linea':'Línea','trip_id':'ID de viaje','parada':'Parada',
                            'retraso_min':'Retraso (min)','retraso_seg':'Retraso (s)'}), {'Retraso (min)':'{:.1f}','Retraso (s)':'{:.0f}'})
                    st.caption(f'{len(df)} observaciones · captura del '+local_time(stamp)+' (Madrid). El filtro de línea afecta a esta tabla.')
            finally:
                period_con.close()
        else:
            st.info('El detalle por línea requiere la referencia GTFS.')
    st.divider()
    render_alerts(con, cfg, gtfs, ncode)


def _nuclei(con, ncode, start, end, preset):
    current = nucleus_current(con)
    current['nucleo'] = current['ncode'].map(_name)
    if ncode:
        current = current[current['ncode']==ncode]
    st.subheader('Estado actual · '+scope_name)
    if current.empty:
        st.info('Sin posiciones ni estimaciones con correspondencia GTFS para este ámbito.')
    records = current.to_dict('records')
    for offset in range(0, len(records), 3):
        for col,row in zip(st.columns(3), records[offset:offset+3]):
            value = row['retraso_medio_min']
            bg = '#eef1f5' if pd.isna(value) else '#d4edda' if value<=2 else '#fff3cd' if value<=10 else '#f8d7da'
            positions = 'Sin datos' if pd.isna(row['circulando']) else _number(row['circulando'])
            with col:
                st.markdown(f'''<div style="background:{bg};color:#1E2742;border-radius:10px;padding:16px;margin-bottom:8px">
                  <h4 style="margin:0;color:#1E2742">{_semaforo(value)} {html.escape(row['nucleo'])}</h4>
                  <hr style="margin:6px 0"><b>Vehículos observados:</b> {positions}<br>
                  <b>Desviación media estimada:</b> {decimal(value,' min')}<br>
                  <b>Máximo incluido:</b> {decimal(row['retraso_max_min'],' min')}<br>
                  <small>{_number(row['validas'])} observaciones incluidas · {_number(row['excluidas'])} fuera del filtro · {_number(row['sin_delay'])} sin retraso informado</small>
                  </div>''', unsafe_allow_html=True)
    st.caption('Vehículos: entidades distintas del último mensaje de posiciones que cruzan con GTFS. Las medias de retraso cuentan observaciones de parada; pueden incluir adelantos. Las capturas de posiciones y retrasos pueden tener fechas distintas.')
    _audit(con, ncode=ncode)
    st.divider()
    st.subheader('Comparación histórica · '+preset)
    history = nucleus_history(con, start, end)
    if history.empty:
        st.info('No hay observaciones dentro del filtro en este período.')
    else:
        history['nucleo'] = history['ncode'].map(_name)
        options = sorted(history['nucleo'].unique())
        default = [scope_name] if ncode and scope_name in options else (['Madrid'] if 'Madrid' in options else options[:1])
        selected = st.multiselect('Núcleos que se comparan en el histórico', options, default=default, key='t2_nuclei')
        shown = history[history['nucleo'].isin(selected)].copy()
        if not shown.empty:
            shown['ts'] = madrid_series(shown['ts'])
            fig = px.line(shown, x='ts', y='media_min', color='nucleo',
                labels={'ts':'Hora (Madrid)','media_min':'Desviación media estimada (min)','nucleo':'Núcleo'}, height=400)
            fig.update_layout(hovermode='x unified')
            _plot(fig)
            grouped = shown.groupby('nucleo').agg(snapshots=('ts','count'),observaciones=('n_obs','sum'),
                                                suma=('suma_min','sum'),maximo=('max_min','max')).reset_index()
            grouped['media'] = grouped['suma']/grouped['observaciones']
            grouped = grouped.rename(columns={'nucleo':'Núcleo','snapshots':'Capturas','observaciones':'Observaciones',
                                              'media':'Desviación media (min)','maximo':'Máximo incluido (min)'})
            _table(grouped[['Núcleo','Capturas','Observaciones','Desviación media (min)','Máximo incluido (min)']],
                   {'Desviación media (min)':'{:.1f}','Máximo incluido (min)':'{:.1f}'})
            st.caption('Media del período ponderada por observaciones. El selector de comparación solo afecta a este gráfico y su tabla.')
        comparison_codes=history.loc[history['nucleo'].isin(selected),'ncode'].unique().tolist()
        st.caption('Los siguientes recuentos corresponden a los núcleos seleccionados para la comparación.')
        _audit(con, start, end, comparison_codes)
    st.divider()
    st.subheader('Mayores retrasos actuales · '+scope_name)
    top = nucleus_top_delays(con, ncode)
    if top.empty:
        st.info('No hay observaciones con retraso mayor de un minuto dentro del filtro en la última captura.')
    else:
        top['ncode'] = top['ncode'].map(_name)
        _table(top.rename(columns={'ncode':'Núcleo','linea':'Línea','trip_id':'ID de viaje','retraso_min':'Retraso (min)'}), {'Retraso (min)':'{:.1f}'})


def _trend(con, gtfs, ncode, start, end, preset):
    st.subheader('Evolución de las estimaciones · '+scope_name+' · '+preset)
    _audit(con, start, end, ncode)
    data = punctuality_over_time(con, start, end, ncode)
    if data.empty:
        st.info('No hay observaciones evaluables en el período seleccionado.')
    else:
        data['ts'] = madrid_series(data['ts'])
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['ts'],y=data['pct_puntual'],name='% sin retraso (≤0 s)',
                            marker_color='steelblue',opacity=0.7,yaxis='y1'))
        fig.add_trace(go.Scatter(x=data['ts'],y=data['retraso_medio_min'],name='Desviación media (min)',
                                line=dict(color='tomato',width=2),mode='lines',yaxis='y2'))
        fig.update_layout(xaxis_title='Hora (Madrid)',yaxis=dict(title='% observaciones sin retraso',range=[0,100]),
            yaxis2=dict(title='Desviación media (min)',overlaying='y',side='right'),
            legend=dict(orientation='h',y=1.1),height=420)
        _plot(fig)
        _table(data.rename(columns={'ts':'Hora (Madrid)','trenes':'Observaciones',
            'pct_puntual':'% sin retraso (≤0 s)','retraso_medio_min':'Desviación media (min)'}),
            {'% sin retraso (≤0 s)':'{:.1f}','Desviación media (min)':'{:.1f}'})
    st.divider()
    if gtfs:
        st.subheader('Desviación media estimada por parada · '+scope_name)
        data = network_heatmap(con, start, end, ncode)
        data = data.dropna(subset=['lat','lon'])
        data = data[data['lat'].between(-90,90) & data['lon'].between(-180,180) & ~((data['lat']==0)&(data['lon']==0))]
        if data.empty:
            st.info('No hay paradas con al menos tres observaciones y coordenadas válidas en este período.')
        else:
            center = {'lat':float(data['lat'].median()),'lon':float(data['lon'].median())}
            fig = px.scatter_map(data,lat='lat',lon='lon',color='retraso_medio_min',size='n_obs',
                hover_name='stop_name',hover_data={'retraso_medio_min':':.1f','n_obs':True},
                labels={'retraso_medio_min':'Desviación (min)','n_obs':'Observaciones'},
                color_continuous_scale='RdYlGn_r',map_style='open-street-map',
                zoom=8 if ncode else 5,center=center,height=500)
            fig.update_layout(margin=dict(r=0,t=10,l=0,b=0))
            _plot(fig)


if st.session_state.get('selected_view') == '⚙️ Sistema':
    st.session_state['selected_view'] = '⚙️ Sistema y calidad'
brand,heading=st.columns([1,7],vertical_alignment='center')
with brand:
    st.image(str(LOGO),width=110)
with heading:
    st.title('Cercanías Renfe')
    st.caption('Cuadro de mando del piloto Anticipa')
selected_view = st.radio('Vista', VIEWS, horizontal=True, label_visibility='collapsed', key='selected_view')
cfg = _load_cfg()
analytical = selected_view in VIEWS[2:5]
gtfs = None
if analytical:
    path = Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'
    if path.exists():
        stat = path.stat()
        gtfs = _load_gtfs_static(cfg['storage']['raw_dir'], (stat.st_mtime_ns,stat.st_size))
with st.sidebar:
    st.button('🔄 Actualizar datos',key='refresh_data',use_container_width=True)
    if analytical:
        st.header('Ámbito del piloto')
        codes = sorted(gtfs['routes']['nucleus_code'].dropna().unique()) if gtfs else []
        options = ['Todos los núcleos']+[_name(code) for code in codes]
        current_scope = st.selectbox('Núcleo',options,index=options.index('Madrid') if 'Madrid' in options else 0,key='dashboard_nucleus')
        ncode = next((code for code in codes if _name(code)==current_scope),None)
        scope_name = current_scope
        st.header('⏱ Período')
        preset = st.selectbox('Rango de tiempo',PERIOD_OPTIONS,index=PERIOD_OPTIONS.index('Hoy'),key='period_preset')
        chosen = None
        if preset=='Personalizado':
            today = datetime.now(MADRID).date()
            chosen = st.date_input('Fechas (Madrid)',value=(today-timedelta(days=7),today),max_value=today,key='custom_range')
            if len(chosen)!=2:
                st.info('Selecciona las fechas inicial y final.');st.stop()
        start,end = period_bounds(preset,chosen)
        st.caption(local_time(start,'%d/%m %H:%M')+' → '+local_time(end,'%d/%m %H:%M')+' · Madrid')
        st.caption('El núcleo se aplica a las métricas, tablas y mapa. El período afecta al histórico; el estado actual usa la última captura.')
    else:
        st.caption('Esta vista conserva su propio ámbito y filtros. El inventario cubre toda la red capturada; el ejemplo preparado sigue una observación de Madrid.')
if selected_view!='📊 El piloto en cifras':
    st.caption('Página consultada el '+datetime.now(MADRID).strftime('%d/%m/%Y %H:%M:%S')+' (Madrid) · Actualización manual')
if selected_view=='📊 El piloto en cifras':
    render_pilot(cfg)
elif selected_view=='⚙️ Sistema y calidad':
    render_quality(cfg)
elif selected_view=='🔎 Del dato al indicador':
    render_trace(cfg)
else:
    st.caption('Ámbito: '+scope_name+'. Las observaciones por núcleo requieren correspondencia con la copia GTFS disponible.')
    with st.expander('Cómo interpretar los indicadores'):
        st.write('El retraso procede de estimaciones publicadas por Renfe. Cada fila corresponde a una actualización de parada; un viaje puede aportar varias. El porcentaje sin retraso utiliza valores ≤0 segundos y no valida llegadas reales.')
        st.write(FILTER_DESCRIPTION)
        st.write('La media positiva usa únicamente valores >0. La desviación media incluye adelantos y retrasos dentro del filtro. Los semáforos orientativos son verde hasta 2 minutos, amarillo hasta 10 y rojo por encima de 10; no cambian la definición del porcentaje sin retraso.')
        st.write('En el histórico, algunos campos delay ausentes se guardaron como cero por el lector. La vista Del dato al indicador permite comprobar su presencia en el mensaje original; estos valores requieren revisión antes de interpretar puntualidad real.')
    con = build_dashboard_connection(cfg,gtfs,start,end,history=selected_view!='🔴 Red en vivo')
    try:
        if selected_view=='🔴 Red en vivo':
            _live(con,cfg,gtfs,ncode,start,end,preset)
        elif not gtfs:
            st.info('Esta vista requiere la referencia GTFS estática.')
        elif selected_view=='🗺️ Por núcleo':
            _nuclei(con,ncode,start,end,preset)
        else:
            _trend(con,gtfs,ncode,start,end,preset)
    finally:
        con.close()
