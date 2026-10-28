"""Avisos: vigencia separada de disponibilidad de la fuente."""
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
from src.capture_status import read_status
from src.presentation import local_time, MADRID
from src.query import active_alerts


def render_alerts(con, cfg, gtfs, ncode=None):
    st.subheader('⚠️ Avisos de Renfe')
    status = read_status(cfg['storage']['raw_dir'], 'alerts')
    source_ts = status.get('source_timestamp')
    if not source_ts:
        source_ts = con.execute('SELECT max(snapshot_ts) FROM alerts').fetchone()[0]
    now = datetime.now(timezone.utc).timestamp()
    last = status.get('last_success')
    checked = (datetime.fromisoformat(last).astimezone(MADRID).strftime('%d/%m/%Y %H:%M:%S %Z')
               if last else 'Pendiente de registro')
    st.caption('Fecha del dato: '+local_time(source_ts)+' (Madrid) · Última descarga correcta: '+checked+'.')
    if source_ts is None or now-float(source_ts)>600 or float(source_ts)>now+60:
        st.warning('La fuente de avisos está desactualizada. No se puede verificar la vigencia de sus avisos; no se presentan como activos.')
        if status.get('error'):
            st.info('La última descarga ha fallado. Consulta Sistema y calidad para revisar el estado de la fuente.')
        return
    if not last or (now-datetime.fromisoformat(last).timestamp()>120) or status.get('error'):
        st.warning('La descarga reciente de avisos no está confirmada. No se afirma que no haya incidencias.')
        return
    alerts = active_alerts(con, now_ts=now)
    names = {}
    route_scope = set()
    if gtfs:
        names = dict(zip(gtfs['routes']['route_id'], gtfs['routes']['route_short_name']))
        if ncode:
            route_scope = set(gtfs['routes'].loc[gtfs['routes']['nucleus_code']==ncode, 'route_id'])
    shown = 0
    for _, row in alerts.iterrows():
        routes = [v.strip() for v in (row['informed_routes'] or '').split(',') if v.strip()]
        if ncode and routes and not route_scope.intersection(routes):
            continue
        lines = sorted({names[r] for r in routes if r in names})
        title = ', '.join(lines[:6]) + (' y más' if len(lines)>6 else '')
        title = title or 'Aviso sin línea identificada'
        start = row['active_start']
        since = local_time(start) if pd.notna(start) else 'Inicio no informado'
        with st.expander(title+' · '+since):
            st.write(row['description_text'] or 'Sin descripción.')
            if not routes:
                st.caption('El aviso no identifica rutas; su ámbito requiere comprobar el texto.')
        shown += 1
    if not shown:
        st.info('No hay avisos que cumplan los criterios de vigencia y ámbito en la última captura reciente.')
    st.caption('Vigencia según el primer intervalo conservado por el parser. La captura reciente no garantiza la cobertura de todas las incidencias.')
