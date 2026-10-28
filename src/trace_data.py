"""Trazabilidad de una captura real de trip_updates hasta sus indicadores."""
import hashlib
import json
import shutil
import zipfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from src.parser import parse_trip_updates
from src.pilot_inventory import _atomic_json


def file_hash(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def scalar(value):
    if value is None or pd.isna(value):return None
    if isinstance(value,(datetime,pd.Timestamp)):return value.isoformat()
    return value.item() if hasattr(value,'item') else value


def demo_root(cfg):
    return Path(cfg['storage']['bronze_dir']).parent/'demo/del-dato-al-indicador'


def load_demo(cfg):
    root=demo_root(cfg)
    try:
        manifest=json.loads((root/'demo.json').read_text())
    except (FileNotFoundError,json.JSONDecodeError):return None
    return manifest,{key:root/value['relative'] for key,value in manifest['files'].items()}


@lru_cache(maxsize=4)
def _load_reference(path,mtime,size):
    reference={'saved_at':datetime.fromtimestamp(mtime/1e9,timezone.utc).isoformat(),'sha256':file_hash(path)}
    with zipfile.ZipFile(path) as z:
        for table,key,columns in (
            ('trips','trip_id',['trip_id','route_id','service_id']),
            ('routes','route_id',['route_id','route_short_name','route_long_name']),
            ('stops','stop_id',['stop_id','stop_name'])):
            with z.open(table+'.txt') as f:data=pd.read_csv(f,dtype=str,keep_default_na=False)
            data.columns=data.columns.str.strip();data=data[[c for c in columns if c in data.columns]].drop_duplicates()
            exact={};normalized={}
            for row in data.to_dict('records'):
                value=row[key]
                exact.setdefault(value,[]).append(row)
                normalized.setdefault(value.strip(),[]).append(row)
            reference[table]={'exact':exact,'normalized':normalized,'key':key}
    return reference


def load_reference(path):
    path=Path(path);stat=path.stat()
    return _load_reference(str(path),stat.st_mtime_ns,stat.st_size)


def lookup(reference,table,value):
    if value is None or pd.isna(value) or not str(value).strip():
        return {'mode':'Sin identificador','row':None,'candidates':0}
    value=str(value);index=reference[table]
    rows=index['exact'].get(value,[]);mode='Exacta'
    if not rows:rows=index['normalized'].get(value.strip(),[]);mode='Tras normalizar espacios'
    if not rows:return {'mode':'Sin correspondencia','row':None,'candidates':0}
    if len(rows)>1:return {'mode':'Ambigua','row':None,'candidates':len(rows)}
    return {'mode':mode,'row':rows[0],'candidates':1}


def enrich(row,reference):
    trip=lookup(reference,'trips',row.get('trip_id'))
    stop=lookup(reference,'stops',row.get('stop_id'))
    route=lookup(reference,'routes',trip['row']['route_id'] if trip['row'] else None)
    if not trip['row'] or not stop['row'] or not route['row']:category='Correspondencia incompleta'
    elif trip['mode']=='Tras normalizar espacios':category='Viaje que requiere normalizar'
    else:category='Viaje con coincidencia exacta'
    return {'trip':trip,'stop':stop,'route':route,'category':category,
            'line':route['row']['route_short_name'].strip() if route['row'] else None,
            'station':stop['row']['stop_name'].strip() if stop['row'] else None}


def load_capture(raw_path,parquet_path):
    feed=gtfs_realtime_pb2.FeedMessage();feed.ParseFromString(Path(raw_path).read_bytes())
    if not feed.IsInitialized():raise ValueError('Mensaje Protobuf incompleto')
    parquet=pq.ParquetFile(parquet_path)
    types={field.name:str(field.type) for field in parquet.schema_arrow}
    saved=parquet.read().to_pandas().reset_index(drop=True)
    replayed=parse_trip_updates(Path(raw_path)).reset_index(drop=True)
    cols=set(saved.columns)-{'ingested_at'};other=set(replayed.columns)-{'ingested_at'}
    comparison={'same_row_count':len(saved)==len(replayed),'same_columns':cols==other,'different_cells':0}
    if len(saved)==len(replayed):
        for col in cols&other:
            a,b=saved[col],replayed[col]
            equal=(a.isna()&b.isna()) | a.eq(b).fillna(False)
            comparison['different_cells']+=int((~equal).sum())
    comparison['verified']=comparison['same_row_count'] and comparison['same_columns'] and not comparison['different_cells']
    links=[]
    for ei,entity in enumerate(feed.entity):
        if entity.HasField('trip_update'):
            links.extend((ei,si) for si,_ in enumerate(entity.trip_update.stop_time_update))
    return {'feed':feed,'saved':saved,'types':types,'links':links,'comparison':comparison,
            'raw_hash':file_hash(raw_path),'parquet_hash':file_hash(parquet_path)}


def source_observation(capture,index):
    ei,si=capture['links'][index]
    entity=capture['feed'].entity[ei];tu=entity.trip_update;stu=tu.stop_time_update[si]
    fields=[('trip_id','trip_update.trip.trip_id',tu.HasField('trip') and tu.trip.HasField('trip_id'),tu.trip.trip_id),
            ('stop_id',f'trip_update.stop_time_update[{si}].stop_id',stu.HasField('stop_id'),stu.stop_id),
            ('stop_sequence',f'trip_update.stop_time_update[{si}].stop_sequence',stu.HasField('stop_sequence'),stu.stop_sequence),
            ('arrival_delay',f'trip_update.stop_time_update[{si}].arrival.delay',stu.HasField('arrival') and stu.arrival.HasField('delay'),stu.arrival.delay)]
    return {'entity':MessageToDict(entity,preserving_proto_field_name=True),'entity_index':ei,
            'stop_update_index':si,'fields':[{'column':col,'source_path':path,'present':bool(present),
                                           'source_value':value if present else None} for col,path,present,value in fields]}


def indicator(parquet_path):
    import duckdb
    from src.query import snapshot_summary
    con=duckdb.connect()
    try:
        path=str(parquet_path).replace("'","''")
        con.execute(f"CREATE VIEW trip_updates AS SELECT * FROM read_parquet('{path}')")
        summary={k:scalar(v) for k,v in snapshot_summary(con,filtered=False).iloc[0].items()}
        summary['analytical_filter']={k:scalar(v) for k,v in snapshot_summary(con).iloc[0].items()}
        summary['positive_delay_sum_seconds']=scalar(con.execute('SELECT sum(arrival_delay) FROM trip_updates WHERE snapshot_ts=(SELECT max(snapshot_ts) FROM trip_updates) AND arrival_delay>0').fetchone()[0])
        return summary
    finally:con.close()


def freeze_demo(cfg,raw_path,parquet_path):
    raw_path,parquet_path=Path(raw_path),Path(parquet_path)
    gtfs=Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'
    capture=load_capture(raw_path,parquet_path)
    if not capture['comparison']['verified']:raise ValueError('La captura no reproduce las filas conservadas.')
    ref=load_reference(gtfs);cases={};scores={}
    for i,row in enumerate(capture['saved'].to_dict('records')):
        detail=source_observation(capture,i)
        present=next(f['present'] for f in detail['fields'] if f['column']=='arrival_delay')
        if pd.notna(row['arrival_delay']) and row['arrival_delay']>0 and present:
            detail=enrich(row,ref);category=detail['category']
            route_id=detail['route']['row']['route_id'].strip() if detail['route']['row'] else ''
            score=(0 if route_id.startswith('10T') else 1,abs(float(row['arrival_delay'])-180),i)
            if category not in scores or score<scores[category]:
                scores[category]=score;cases[category]=i
    stamp=raw_path.stem[len('trip_updates_'):]
    version=stamp+'-'+capture['raw_hash'][:8]+'-'+ref['sha256'][:8]
    root=demo_root(cfg);folder=root/version;folder.mkdir(parents=True,exist_ok=True)
    files={}
    for key,source in [('raw',raw_path),('parquet',parquet_path),('gtfs',gtfs)]:
        dest=folder/source.name
        if not dest.exists():shutil.copy2(source,dest)
        if file_hash(dest)!=file_hash(source):raise ValueError('La copia preparada difiere de su archivo de origen.')
        files[key]={'relative':str(dest.relative_to(root)),'name':source.name,'sha256':file_hash(dest)}
    manifest={'version':1,'prepared_at':datetime.now(timezone.utc).isoformat(),'source':'trip_updates',
              'snapshot_ts':int(capture['feed'].header.timestamp),'row_count':len(capture['saved']),
              'files':files,'cases':cases,'parser_sha256':file_hash(Path(__file__).with_name('parser.py'))}
    _atomic_json(folder/'manifest.json',manifest);_atomic_json(root/'demo.json',manifest)
    return manifest


def make_evidence(capture,index,reference,enriched,summary,raw_path,parquet_path):
    return {'source':'trip_updates','snapshot_ts':int(capture['feed'].header.timestamp),
        'raw_file':Path(raw_path).name,'raw_sha256':capture['raw_hash'],
        'parquet_file':Path(parquet_path).name,'parquet_sha256':capture['parquet_hash'],
        'gtfs_sha256':reference['sha256'],'gtfs_saved_at':reference['saved_at'],
        'parser_sha256':file_hash(Path(__file__).with_name('parser.py')),
        'row_index':int(index),'source_observation':source_observation(capture,index),
        'processed_row':{k:scalar(v) for k,v in capture['saved'].iloc[index].items()},
        'parquet_schema':capture['types'],'replay_comparison':capture['comparison'],'enrichment':enriched,'capture_indicator':summary,
        'method':'Retraso medio positivo = suma de arrival_delay > 0 / número de filas con arrival_delay > 0 / 60. Los valores son estimaciones de Renfe; las filas no son viajes únicos.'}


def evidence_zip(evidence,capture,raw_path,parquet_path):
    from io import BytesIO
    output=BytesIO()
    with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as z:
        z.write(raw_path,'original/'+Path(raw_path).name)
        z.write(parquet_path,'procesado/'+Path(parquet_path).name)
        z.writestr('observacion.json',json.dumps(evidence,ensure_ascii=False,indent=2))
        z.writestr('captura_procesada.csv',capture['saved'].to_csv(index=False))
        z.writestr('referencias_gtfs_utilizadas.json',json.dumps({'gtfs_sha256':evidence['gtfs_sha256'],
            'saved_at':evidence['gtfs_saved_at'],'matches':evidence['enrichment']},ensure_ascii=False,indent=2))
        z.writestr('LEEME.txt','Evidencia de una captura real de trip_updates de Anticipa.\n'
            'observacion.json identifica la fila y sus campos, la comparación del parseo y el cálculo del indicador.\n'
            'Se incluye el mensaje Protobuf completo y su Parquet. Las referencias GTFS contienen las filas usadas,\n'
            'no el horario completo; el SHA-256 identifica la copia conservada en el servidor.\n'
            'ingested_at es la fecha del parseo y se excluye de la comparación al reproducirlo.\n'
            'arrival_delay es la estimación publicada por Renfe, no una llegada observada ni una predicción de Anticipa.\n')
    return output.getvalue()
