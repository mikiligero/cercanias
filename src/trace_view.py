"""Recorrido guiado de una captura conservada hasta el indicador del panel."""
import json
import re
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
import streamlit as st
from src.pilot_inventory import build_inventory,read_inventory
from src.pilot_view import _local,_number,_size
from src.presentation import decimal
from src.trace_data import (load_demo,load_reference,load_capture,enrich,source_observation,
                            indicator,make_evidence,evidence_zip,scalar,file_hash)


def _show(value):
    value=scalar(value)
    return 'Nulo' if value is None else repr(value) if isinstance(value,str) else str(value)


def _explore_paths(cfg):
    inv=read_inventory(cfg)
    if st.session_state.get('refresh_data',False):
        try:inv=build_inventory(cfg)
        except RuntimeError as exc:st.info(str(exc))
    if not inv:
        st.info('El inventario de capturas aún no está disponible.');return None
    days=sorted({row['date'] for row in inv['partitions'].values() if row['feed']=='trip_updates' and row['valid_parquet_files']>0},reverse=True)
    if not days:
        st.info('No hay capturas de actualizaciones de viajes para explorar.');return None
    day=st.selectbox('Día de la captura (UTC)',days,key='trace_day')
    folder=Path(cfg['storage']['bronze_dir'])/'trip_updates'/('date='+day)
    files=sorted(p for p in folder.glob('part-*.parquet') if re.fullmatch(r'part-\d{8}T\d{6}Z\.parquet',p.name))
    if not files:
        st.info('No hay archivos de captura en este día.');return None
    default=next((i for i,p in enumerate(files) if p.stem[14:16]>='12'),0)
    selected=st.selectbox('Snapshot conservado',files,index=default,
        format_func=lambda p:p.stem[14:16]+':'+p.stem[16:18]+':'+p.stem[18:20]+' UTC',key='trace_snapshot_'+day)
    stamp=selected.stem[5:]
    raw=Path(cfg['storage']['raw_dir'])/'trip_updates'/day/('trip_updates_'+stamp+'.pb')
    return {'raw':raw,'parquet':selected,'gtfs':Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'}


def render_trace(cfg):
    st.header('Del dato al indicador')
    st.write('Sigue una observación real: mensaje de Renfe, fila procesada, correspondencias y cálculo del indicador.')
    st.caption('Captura → Mensaje original → Parquet → GTFS → Indicador')
    mode=st.radio('Captura que se muestra',['Ejemplo preparado para la demo','Explorar el histórico'],horizontal=True,key='trace_mode')
    manifest=None
    if mode=='Ejemplo preparado para la demo':
        demo=load_demo(cfg)
        if not demo:
            st.info('El ejemplo de la demo aún no está preparado. Puedes explorar el histórico.');return
        manifest,paths=demo
        st.caption('Ejemplo fijado con su copia de GTFS. La captura y el horario permanecen estables al actualizar la página.')
    else:
        paths=_explore_paths(cfg)
        if paths is None:return
    try:
        capture=load_capture(paths['raw'],paths['parquet'])
        reference=load_reference(paths['gtfs'])
        summary=indicator(paths['parquet'])
    except Exception:
        st.error('No se ha podido leer el original, el Parquet o la referencia GTFS de esta captura. Elige otra para continuar.');return
    if manifest and any(actual!=manifest['files'][key]['sha256'] for key,actual in
                        [('raw',capture['raw_hash']),('parquet',capture['parquet_hash']),('gtfs',file_hash(paths['gtfs']))]):
        st.error('Los archivos del ejemplo no coinciden con sus huellas conservadas. Es necesario revisar el ejemplo antes de utilizarlo.');return
    if not capture['comparison']['verified']:
        st.error('El mensaje original no reproduce las filas conservadas. No se puede mostrar esta captura como trazabilidad verificada.')
        st.json(capture['comparison']);return
    data=capture['saved']
    if data.empty:
        st.info('La captura no contiene observaciones de paradas.');return
    rows=data.to_dict('records');details=[enrich(row,reference) for row in rows]
    cases={}
    for i,(row,detail) in enumerate(zip(rows,details)):
        if pd.notna(row['arrival_delay']) and row['arrival_delay']>0:
            cases.setdefault(detail['category'],i)
    default=next(iter(cases.values()),0)
    if manifest:
        preferred=['Viaje con coincidencia exacta','Viaje que requiere normalizar','Correspondencia incompleta']
        available=[case for case in preferred if case in manifest['cases']]
        if available:
            case=st.radio('Caso del ejemplo',available,horizontal=True,key='trace_case')
            default=manifest['cases'][case]
    def label(i):
        row,detail=rows[i],details[i]
        delay='sin retraso informado' if pd.isna(row['arrival_delay']) else decimal(row['arrival_delay']/60,' min')
        return f'Fila {i+1} · {detail["line"] or row["trip_id"]} · {detail["station"] or row["stop_id"]} · {delay}'
    key='trace_row_'+capture['raw_hash'][:12]+'_'+str(default)
    index=st.selectbox('Observación que seguimos',list(range(len(data))),index=default,format_func=label,key=key)
    row=rows[index];detail=details[index];source=source_observation(capture,index)
    _render_steps(capture,row,detail,source,summary,reference,paths,index)


def _render_steps(capture,row,detail,source,summary,reference,paths,index):
    feed=capture['feed'];stamp=int(feed.header.timestamp)
    st.subheader('1 · Captura conservada')
    a,b,c,d=st.columns(4)
    a.metric('Fuente','trip_updates')
    b.metric('Entidades del mensaje',_number(len(feed.entity)))
    c.metric('Observaciones en Parquet',_number(len(capture['saved'])))
    d.metric('Tamaño del original',_size(paths['raw'].stat().st_size))
    when=datetime.fromtimestamp(stamp,timezone.utc)
    st.caption('Mensaje generado el '+_local(when.isoformat())+' (Madrid) · '+when.strftime('%H:%M:%S UTC')+'. Archivo '+paths['raw'].name+'.')
    left,right=st.columns(2)
    with left:
        st.subheader('2 · Mensaje original')
        st.caption(f'Entidad {source["entity_index"]+1} del mensaje · actualización de parada {source["stop_update_index"]+1}. Protobuf representado como JSON para poder leerlo.')
        followed=next(field for field in source['fields'] if field['column']=='arrival_delay')
        st.caption('Campo seguido: '+followed['source_path']+' = '+(_show(followed['source_value'])+' segundos' if followed['present'] else 'no informado')+'.')
        st.json(source['entity'],expanded=5)
    with right:
        st.subheader('3 · Fila procesada')
        st.caption('Valores conservados en Parquet para esta observación.')
        display=pd.DataFrame([{'Campo':key,'Tipo':capture['types'].get(key,'—'),'Valor':_show(value)} for key,value in row.items()])
        st.dataframe(display,use_container_width=True,hide_index=True,height=390)
        st.success(f'El parseo del original reproduce las {_number(len(capture["saved"]))} filas y todos sus campos de origen.')
        st.caption('ingested_at conserva la hora del procesamiento original. Al repetir el parseo se excluye ese campo de la comparación, porque se generaría una hora nueva.')
    with st.expander('Ver la correspondencia campo a campo'):
        mapping=[]
        for field in source['fields']:
            col=field['column'];value=row.get(col)
            if field['present']:rule='Valor de origen conservado'
            elif value is None or pd.isna(value):rule='Campo ausente → nulo'
            else:rule='Campo no informado; se conserva el valor predeterminado del lector'
            mapping.append({'Campo en Parquet':col,'Ruta en el mensaje':field['source_path'],
                            'Original':_show(field['source_value']) if field['present'] else 'No informado',
                            'Valor en Parquet':_show(value),'Tratamiento':rule})
        st.dataframe(pd.DataFrame(mapping),use_container_width=True,hide_index=True)
        st.caption('Los identificadores se conservan tal como llegan. La normalización mostrada en el siguiente paso se aplica al cruce de consulta. La conversión del retraso a minutos se realiza al calcular el indicador.')
    st.subheader('4 · Correspondencia con el horario GTFS')
    match_rows=[]
    for label,key,input_value,id_key,name_key in (
        ('Viaje','trip',row.get('trip_id'),'trip_id','route_id'),
        ('Línea','route',detail['trip']['row']['route_id'] if detail['trip']['row'] else None,'route_id','route_short_name'),
        ('Parada','stop',row.get('stop_id'),'stop_id','stop_name')):
        match=detail[key];found=match['row']
        match_rows.append({'Cruce':label,'ID de entrada':_show(input_value),
                           'ID en GTFS':_show(found[id_key]) if found else 'Sin correspondencia única',
                           'Resultado':match['mode'],'Referencia':found.get(name_key,'').strip() if found else '—'})
    st.dataframe(pd.DataFrame(match_rows),use_container_width=True,hide_index=True)
    st.write(f'**Línea:** {detail["line"] or "Sin correspondencia"} · **Parada:** {detail["station"] or "Sin correspondencia"}')
    st.caption('Las comillas permiten ver espacios en los identificadores. Referencia GTFS conservada el '+_local(reference['saved_at'])+' (Madrid). Los identificadores sin correspondencia se mantienen visibles.')
    if datetime.fromisoformat(reference['saved_at']).date()>when.date():
        st.info('Esta copia GTFS es posterior a la captura. Las diferencias de versión pueden limitar el cruce histórico; el resultado no reproduce por sí solo la reconciliación de aquella fecha.')
    st.subheader('5 · Del retraso en segundos al indicador')
    delay=scalar(row['arrival_delay'])
    present=next(f['present'] for f in source['fields'] if f['column']=='arrival_delay')
    if delay is not None and present:
        st.write(f'**Observación elegida:** {delay:g} segundos ÷ 60 = **{decimal(delay/60)} minutos de retraso estimado**.')
    elif delay is None:
        st.info('Esta observación no tiene valor de retraso en Parquet y queda fuera de los cálculos de retraso.')
    else:
        st.info('El mensaje no informa arrival.delay para esta observación. El archivo conserva un valor predeterminado; no debe interpretarse como una llegada puntual observada.')
    st.caption('arrival_delay es una estimación de llegada publicada por Renfe. No es una llegada consumada ni una predicción generada por Anticipa.')
    st.markdown('**Resumen de esta captura**')
    a,b,c,d=st.columns(4)
    a.metric('Observaciones evaluadas',_number(summary['trenes']))
    b.metric('Con retraso positivo',_number(summary['con_retraso'] or 0))
    c.metric('Retraso medio positivo',decimal(summary['retraso_medio_min'],' min'))
    d.metric('Retraso máximo',decimal(summary['retraso_max_min'],' min'))
    positives=int(summary['con_retraso'] or 0)
    if positives:
        st.write(f'**Cálculo del retraso medio positivo:** {summary["positive_delay_sum_seconds"]:g} segundos ÷ {positives} observaciones ÷ 60 = **{decimal(summary["retraso_medio_min"])} minutos** (redondeado a una décima).')
    else:st.info('No hay observaciones con retraso positivo en esta captura; la media positiva no tiene un valor calculable.')
    st.caption('Este resumen reproduce los valores del archivo sin aplicar el filtro del EDA. Se cuentan filas con arrival_delay no nulo; una entidad puede aportar varias paradas. Los recuentos no representan viajes distintos y los identificadores sin correspondencia GTFS se conservan.')
    analytical=summary.get('analytical_filter')
    if analytical:
        st.markdown('**Efecto del filtro usado en los indicadores del panel**')
        st.write('Intervalo incluido: −5 a 120 minutos. '+
            f"{_number(analytical['trenes'])} observaciones incluidas · {_number(analytical['fuera_rango'])} fuera del intervalo · {_number(analytical['sin_delay'])} sin valor de retraso.")
        st.caption('Media positiva con filtro: '+decimal(analytical['retraso_medio_min'],' min')+
                   ' · máximo incluido: '+decimal(analytical['retraso_max_min'],' min')+'. Los archivos originales permanecen iguales.')
    defaults=0
    for j,(_,si) in enumerate(capture['links']):
        ei=capture['links'][j][0];stu=feed.entity[ei].trip_update.stop_time_update[si]
        if not (stu.HasField('arrival') and stu.arrival.HasField('delay')) and pd.notna(capture['saved'].iloc[j]['arrival_delay']) and capture['saved'].iloc[j]['arrival_delay']==0:
            defaults+=1
    if defaults:
        st.warning(f'{defaults} valores cero del archivo proceden de campos delay no informados en el mensaje. Se reproduce el cálculo actual; esos ceros requieren revisión antes de interpretarlos como puntualidad.')
    evidence=make_evidence(capture,index,reference,detail,summary,paths['raw'],paths['parquet'])
    packet=evidence_zip(evidence,capture,paths['raw'],paths['parquet'])
    st.download_button('Descargar evidencia de la captura (ZIP)',packet,file_name=f'anticipa-trazabilidad-{stamp}-fila-{index+1}.zip',mime='application/zip',key='trace_evidence')
    st.download_button('Descargar observación y cálculo (JSON)',json.dumps(evidence,ensure_ascii=False,indent=2).encode('utf-8'),file_name=f'anticipa-observacion-{stamp}-{index+1}.json',mime='application/json',key='trace_json')
    with st.expander('Huellas y reproducibilidad'):
        st.write('La comprobación vuelve a interpretar el Protobuf y compara sus filas con el Parquet conservado, sin cambiar los archivos. Se verifica el orden, el número de filas y los valores de todos los campos de origen.')
        st.json({'Original SHA-256':capture['raw_hash'],'Parquet SHA-256':capture['parquet_hash'],
                 'GTFS SHA-256':reference['sha256'],'Comparación':capture['comparison']})
        st.caption('El ZIP incluye original, Parquet, tabla CSV, observación y filas GTFS utilizadas. El horario completo se conserva en el servidor y se identifica por su huella. La reproducción comprueba el procesamiento existente; no sustituye las reglas de calidad del otro panel.')
