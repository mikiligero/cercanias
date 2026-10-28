"""Panel de sistema, continuidad y calidad del piloto Anticipa."""
import json
import shutil
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from src.dashboard_data import latest_file
from src.pilot_inventory import build_inventory, read_inventory
from src.pilot_view import LABELS, _local, _number, _size
from src.quality_data import analyze_day, build_continuity, continuity_gaps, operational_gaps, read_continuity, read_day_report
from src.capture_status import read_status
from src.presentation import decimal


def service_state(name):
    try:
        p=subprocess.run(['systemctl','is-active',name],capture_output=True,text=True,timeout=3)
        return {'active':'Activo','inactive':'Inactivo','failed':'Con fallo','activating':'Arrancando'}.get(p.stdout.strip(),'No comprobado')
    except (OSError,subprocess.TimeoutExpired):
        return 'No comprobado'


def latest_raw(cfg,feed):
    root=Path(cfg['storage']['raw_dir'])/feed
    if root.exists():
        for day in sorted(root.iterdir(),reverse=True):
            if day.is_dir():
                files=sorted(day.glob(feed+'_*.pb'))
                if files:return files[-1]
    return None


def _stamp(file,feed=None):
    if file is None:return None
    name=file.stem[len(feed)+1:] if feed else file.stem.removeprefix('part-')
    try:return datetime.strptime(name,'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
    except ValueError:return None


def _time(value):
    return _local(value.isoformat()) if value else 'Sin archivos'


def render_quality(cfg):
    st.header('Sistema y calidad')
    st.write('Qué se está capturando, dónde aparecen huecos y qué comprobaciones han pasado los datos.')
    inv=read_inventory(cfg);continuity=read_continuity(cfg)
    if st.session_state.get('refresh_data',False):
        try:
            with st.spinner('Actualizando el inventario y la continuidad…'):
                inv=build_inventory(cfg);continuity=build_continuity(cfg)
        except RuntimeError as exc:st.info(str(exc))
    now=datetime.now(timezone.utc)
    ingest=service_state('cercanias-pipeline');panel=service_state('cercanias-dashboard')
    last_position=_stamp(latest_raw(cfg,'vehicle_positions'),'vehicle_positions')
    age=int((now-last_position).total_seconds()) if last_position else None
    disk_path=Path(cfg['storage']['raw_dir'])
    while not disk_path.exists() and disk_path.parent!=disk_path:
        disk_path=disk_path.parent
    disk=shutil.disk_usage(disk_path)
    cards=st.columns(4)
    cards[0].metric('Proceso de captura',ingest)
    cards[1].metric('Edad del último snapshot',f'{age}s' if age is not None else 'Sin datos')
    cards[2].metric('Espacio libre',_size(disk.free))
    cards[3].metric('Servicio del panel',panel)
    st.caption('Consultado el '+_local(now.isoformat())+' · hora de Madrid. La edad corresponde al dato de posiciones conservado, no a la latencia completa del servicio.')
    if ingest in ('Inactivo','Con fallo'):st.error('El servicio de captura no está activo. Revisa su estado antes de usar datos recientes.')
    if age is not None and age>120:st.warning('El último snapshot de posiciones tiene más de dos minutos. Es necesario comprobar la captura y el origen antes de atribuir la causa.')
    if disk.used/disk.total>=0.85:st.warning(f'El disco está al {disk.used/disk.total:.0%}. Conviene planificar espacio adicional para conservar el histórico.')
    st.progress(disk.used/disk.total,text=f'Disco: {_size(disk.used)} usados de {_size(disk.total)}')
    sources=[]
    for feed,label in LABELS.items():
        raw=_stamp(latest_raw(cfg,feed),feed)
        bronze=_stamp(latest_file(cfg['storage']['bronze_dir'],feed))
        status=read_status(cfg['storage']['raw_dir'],feed)
        success=status.get('last_success')
        sources.append({'Fuente':label,'Último original conservado':_time(raw),'Último Parquet conservado':_time(bronze),
                        'Última descarga correcta':_local(success) if success else 'Pendiente de registro',
                        'Estado de descarga':status.get('state','Pendiente de registro')})
    st.dataframe(pd.DataFrame(sources),use_container_width=True,hide_index=True)
    st.caption('Horas de Madrid. Un proceso activo no confirma cada descarga. En alertas, la ausencia de nuevas capturas puede responder al contenido de la fuente; no se interpreta como caída.')
    if not inv:
        st.info('El inventario está pendiente. Pulsa Actualizar datos para prepararlo.')
        return
    data=pd.DataFrame(inv['partitions'].values())
    if data.empty:
        st.info('Todavía no hay capturas inventariadas.');return
    data['date']=pd.to_datetime(data['date'])
    lo,hi=data['date'].min().date(),data['date'].max().date()
    scope=st.radio('Calendario de captura', ['Últimos 30 días','Todo el histórico','Período del EDA','Elegir fechas'],horizontal=True,key='quality_scope')
    start,end=max(lo,hi-timedelta(days=29)),hi
    if scope=='Todo el histórico':start=lo
    elif scope=='Período del EDA':start,end=date(2026,5,17),date(2026,7,3)
    elif scope=='Elegir fechas':
        chosen=st.date_input('Fechas del calendario (UTC)',value=(start,end),min_value=lo,max_value=hi,key='quality_dates')
        if len(chosen)!=2:
            st.info('Selecciona el inicio y el fin.');return
        start,end=chosen
    selected=data[data['date'].between(pd.Timestamp(start),pd.Timestamp(end))]
    grid=pd.date_range(start,end,freq='D')
    counts=selected.pivot(index='feed',columns='date',values='raw_files').reindex(index=list(LABELS),columns=grid,fill_value=0).fillna(0)
    st.subheader('Continuidad de las capturas')
    fig=go.Figure(go.Heatmap(z=counts.values,x=grid,y=list(LABELS.values()),
        colorscale=[[0,'#f1f3f5'],[0.01,'#d5e9e6'],[1,'#087f8c']],zmin=0,
        colorbar=dict(title='Capturas'),hovertemplate='%{y}<br>%{x|%d/%m/%Y}<br>%{z:,.0f} capturas<extra></extra>'))
    fig.update_layout(height=230,margin=dict(l=0,r=0,t=10,b=0),xaxis_title='Fecha UTC',yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig,use_container_width=True)
    st.caption('Inventario del '+_local(inv['generated_at'])+'. Cada celda cuenta archivos originales tras deduplicar; el día actual es parcial. Cero archivos no permite afirmar por sí solo que Renfe estuviera caído.')
    if continuity:
        gaps=continuity_gaps(continuity,start,end)
        positions=counts.loc[['vehicle_positions','trip_updates']].sum(axis=0)
        gaps=operational_gaps(gaps)
        operative=[g for g in gaps if g['operational_seconds']>120]
        k1,k2,k3,k4=st.columns(4)
        k1.metric('Intervalos mayores de 2 min',_number(len(gaps)))
        k2.metric('En ventana de referencia',_number(len(operative)))
        k3.metric('Mayor intervalo en ventana',decimal(max(g['operational_seconds'] for g in operative)/60,' min') if operative else 'Sin intervalos >2 min')
        k4.metric('Días sin capturas VP ni TU',_number((positions==0).sum()))
        st.caption('Intervalos entre marcas temporales de posiciones y actualizaciones conservadas, incluidos cruces de medianoche y recortados al período seleccionado. No es una medición de disponibilidad ni de peticiones fallidas. Alertas queda fuera de este cálculo.')
        st.caption('Ventana de referencia para la demo: 05:30–00:30, hora de Madrid. Se cuenta la parte de cada intervalo que coincide con ella, aunque empiece de madrugada. No sustituye los horarios oficiales de cada núcleo. El recuento completo se conserva.')
        if gaps:
            with st.expander('Ver los mayores intervalos sin snapshots'):
                table=pd.DataFrame(gaps)
                table['Fuente']=table['feed'].map(LABELS)
                table['Desde (Madrid)']=pd.to_datetime(table['start'],unit='s',utc=True).dt.tz_convert('Europe/Madrid').dt.strftime('%d/%m %H:%M:%S')
                table['Hasta (Madrid)']=pd.to_datetime(table['end'],unit='s',utc=True).dt.tz_convert('Europe/Madrid').dt.strftime('%d/%m %H:%M:%S')
                table['Minutos']=(table['seconds']/60).round(1)
                table['Minutos en ventana']=(table['operational_seconds']/60).round(1)
                st.dataframe(table[['Fuente','Desde (Madrid)','Hasta (Madrid)','Minutos','Minutos en ventana']].head(30),use_container_width=True,hide_index=True)
                st.download_button('Descargar intervalos (CSV)',table.to_csv(index=False).encode('utf-8-sig'),file_name='anticipa-intervalos-captura.csv',mime='text/csv')
        st.caption('Continuidad calculada el '+_local(continuity['generated_at'])+'.')
    else:st.info('El análisis de continuidad está pendiente. Pulsa Actualizar datos para calcularlo.')

    st.divider()
    st.subheader('Comprobación de calidad de un día')
    days=sorted(data.loc[(data['feed']=='vehicle_positions') & (data['valid_parquet_files']>0),'date'].dt.strftime('%Y-%m-%d').unique(),reverse=True)
    if not days:
        st.info('No hay días con posiciones procesadas para comprobar.');return
    yesterday=(datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
    default=days.index(yesterday) if yesterday in days else 0
    day=st.selectbox('Día que se comprueba (UTC)',days,index=default,key='quality_day')
    report=read_day_report(cfg,day)
    if st.button('Comprobar el día seleccionado',key='run_quality_day'):
        try:
            with st.spinner('Comprobando archivos, campos y correspondencias…'):
                report=analyze_day(cfg,day)
        except (RuntimeError,OSError) as exc:
            st.error('No se ha podido completar la comprobación. Conservamos el resultado anterior.')
    if report:
        _render_report(report)
    else:st.info('Comprobación no ejecutada para este día. Usa el botón para obtener resultados verificables.')
    st.divider()
    st.subheader('Conservación y alcance de las comprobaciones')
    errors=int(selected['invalid_parquet_files'].sum())
    names=int(selected['unexpected_names'].sum())
    q1,q2=st.columns(2)
    q1.metric('Parquet con metadatos no legibles',_number(errors))
    q2.metric('Archivos con nombre inesperado',_number(names))
    recovery=[]
    for folder in Path(cfg['storage']['bronze_dir']).parent.glob('recovery-backup-*'):
        recovery.extend(p for p in folder.iterdir() if p.is_file())
    with st.expander('Recuperación y comprobaciones pendientes'):
        st.write(f'La carpeta de recuperación conserva {_number(len(recovery))} archivos ({_size(sum(p.stat().st_size for p in recovery))}). Incluye copias de archivos reconstruidos; ese recuento no equivale a pérdidas definitivas.')
        st.write('Este panel comprueba conservación de archivos, intervalos entre snapshots, reglas de campos y coincidencia de IDs con el GTFS disponible. La integridad completa, la reconciliación histórica con versiones GTFS, la disponibilidad K1, la recuperación ante fallos y la latencia hasta el navegador requieren pruebas específicas.')
        st.write('Los avisos y el modelo predictivo de Anticipa permanecen fuera de estas comprobaciones. Los registros con incidencias no se eliminan desde este panel.')


def _render_report(report):
    st.caption('Comprobación del '+_local(report['generated_at'])+' · día de datos '+report['date']+' (UTC).')
    matching=report.get('matching')
    if matching and matching['total']:
        total=matching['total'];exact=100*matching['exact']/total;normalized=100*matching['normalized']/total
        st.markdown('**Correspondencia de identificadores con GTFS**')
        a,b,c=st.columns(3)
        a.metric('IDs observados distintos',_number(total))
        b.metric('Coincidencia exacta',decimal(exact,'%'))
        c.metric('Tras normalizar espacios',decimal(normalized,'%'),delta=decimal(normalized-exact,' puntos'))
        st.progress(matching['normalized']/total,text=f'{_number(matching["normalized"])} IDs con correspondencia · {_number(matching["unmatched"])} sin correspondencia')
        st.caption('Se comparan los mismos IDs distintos de posiciones, antes y después de quitar espacios a ambos lados. Referencia: copia GTFS guardada el '+_local(matching['gtfs_saved_at'])+'. Este porcentaje no mide cobertura de circulaciones planificadas ni demuestra reconciliación histórica.')
        if datetime.fromisoformat(matching['gtfs_saved_at']).date()>date.fromisoformat(report['date']):
            st.info('La copia GTFS es posterior al día analizado. Las diferencias pueden deberse a versiones del horario; este cruce no reproduce la reconciliación histórica de la memoria.')
        if matching['examples']:
            with st.expander('Ejemplos de IDs sin correspondencia'):
                st.dataframe(pd.DataFrame({'ID de origen':matching['examples']}),use_container_width=True,hide_index=True)
    else:st.info('Cruce con GTFS no ejecutado o sin IDs comparables en este día.')
    rules=pd.DataFrame(report['rules'])
    if not rules.empty:
        st.markdown('**Reglas aplicadas a los registros**')
        rules['Fuente']=rules['feed'].map(LABELS)
        rules['% afectado']=(100*rules['affected']/rules['total'].replace(0,float('nan'))).round(2)
        rules=rules.rename(columns={'rule':'Comprobación','affected':'Filas afectadas','total':'Filas evaluadas','state':'Resultado'})
        st.dataframe(rules[['Fuente','Comprobación','Filas afectadas','Filas evaluadas','% afectado','Resultado']],use_container_width=True,hide_index=True)
        st.caption('Las reglas pueden afectar a las mismas filas y no deben sumarse. El intervalo de −5 a 120 minutos procede del filtro exploratorio: los valores fuera de él se señalan para revisión. Una incidencia de campo no implica que deba borrarse el registro.')
    else:st.info('Las reglas de registros no se han podido ejecutar para este día.')
    with st.expander('Del original al Parquet'):
        rows=[{'Fuente':LABELS.get(feed,feed),'Originales':r['original'],'Parquet':r['parquet'],
               'Metadatos no legibles':r['invalid'],'Originales sin Parquet':r['without_parquet']} for feed,r in report['files'].items()]
        st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)
        st.caption('Claves revisadas: posiciones = snapshot + entidad; actualizaciones = snapshot + entidad + parada + secuencia. Sus repeticiones se señalan para revisión, sin afirmar que todos los campos de las filas sean idénticos.')
        st.caption('Una captura original sin Parquet puede corresponder a un mensaje sin entidades o a un fallo de procesamiento; este recuento no atribuye una causa. No equivale a filas perdidas.')
        audited=[]
        for feed,counts in report['files'].items():
            audit=counts.get('unpaired_audit')
            if audit:
                audited.append({'Fuente':LABELS.get(feed,feed),'Sin registros de esta fuente':audit['empty'],
                                'Con registros pendientes de revisar':audit['with_records'],'Originales no legibles':audit['invalid']})
        if audited:
            st.dataframe(pd.DataFrame(audited),use_container_width=True,hide_index=True)
            st.caption('Se ha abierto cada original sin Parquet del día y comprobado su contenido. No se atribuye el motivo al horario ni se reprocesa o borra ningún archivo.')
        else:
            st.info('Este informe es anterior a la comprobación del contenido de los originales sin Parquet. Vuelve a comprobar el día para incluirla.')
    for problem in report['query_errors']:
        st.warning('Comprobación incompleta para '+LABELS.get(problem['feed'],problem['feed'])+'. Se conserva el fallo en el informe; no se presenta como prueba superada.')
    st.download_button('Descargar informe de calidad (JSON)',json.dumps(report,ensure_ascii=False,indent=2).encode('utf-8'),
                       file_name=f'anticipa-calidad-{report["date"]}.json',mime='application/json')
