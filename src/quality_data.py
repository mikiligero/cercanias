"""Continuidad de capturas y comprobaciones reproducibles de un día."""
import json
import fcntl
import os
from datetime import datetime, timezone
from datetime import timedelta, time
from zoneinfo import ZoneInfo
from pathlib import Path
import duckdb
import pandas as pd
import pyarrow.parquet as pq
from src.pilot_inventory import _atomic_json, _signature, inventory_path

POSITION_FEEDS = ('vehicle_positions', 'trip_updates')


def operational_gaps(gaps, start_hour=time(5,30), end_hour=time(0,30)):
    """Intersección completa con una ventana civil; incluye cruces y cambios DST."""
    zone=ZoneInfo('Europe/Madrid')
    result=[]
    for gap in gaps:
        first=datetime.fromtimestamp(gap['start'],zone).date()-timedelta(days=1)
        last=datetime.fromtimestamp(gap['end'],zone).date()
        day=first;seconds=0
        while day<=last:
            a=datetime.combine(day,start_hour,zone)
            next_day=day+timedelta(days=1) if end_hour<=start_hour else day
            b=datetime.combine(next_day,end_hour,zone)
            seconds+=max(0,min(gap['end'],int(b.timestamp()))-max(gap['start'],int(a.timestamp())))
            day+=timedelta(days=1)
        result.append(dict(gap, operational_seconds=seconds))
    return result


def audit_unpaired(files, feed):
    """Abre cada original sin Parquet; nunca deduce el motivo a partir de su hora."""
    from google.transit import gtfs_realtime_pb2
    result=dict(empty=0,with_records=0,invalid=0,examples=[])
    for path in files:
        try:
            message=gtfs_realtime_pb2.FeedMessage()
            message.ParseFromString(path.read_bytes())
            if not message.IsInitialized():
                raise ValueError('Protobuf incompleto')
            if feed=='vehicle_positions':
                count=sum(e.HasField('vehicle') for e in message.entity)
            else:
                count=sum(len(e.trip_update.stop_time_update) for e in message.entity if e.HasField('trip_update'))
            category='with_records' if count else 'empty'
        except Exception:
            category='invalid'
        result[category]+=1
        if category!='empty' and len(result['examples'])<10:
            result['examples'].append(dict(file=path.name,state=category))
    return result


def _cache_path(cfg, name):
    return inventory_path(cfg).parent / name


def read_continuity(cfg):
    try:
        return json.loads(_cache_path(cfg, 'continuity.json').read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def capture_times(folder, feed):
    if not folder.exists():
        return []
    prefix = feed + '_'
    result = []
    for entry in os.scandir(folder):
        if entry.is_file() and entry.name.startswith(prefix) and entry.name.endswith('.pb'):
            try:
                result.append(int(datetime.strptime(entry.name[len(prefix):-3], '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc).timestamp()))
            except ValueError:
                continue
    return sorted(set(result))


def _build_continuity(cfg):
    previous = (read_continuity(cfg) or {}).get('partitions', {})
    partitions = {}
    for feed in POSITION_FEEDS:
        root = Path(cfg['storage']['raw_dir']) / feed
        if not root.exists():
            continue
        for day in sorted(root.iterdir()):
            if not day.is_dir():
                continue
            try:
                datetime.strptime(day.name, '%Y-%m-%d')
            except ValueError:
                continue
            key = feed + '/' + day.name
            sig = _signature(day)
            if key in previous and previous[key]['signature'] == sig:
                partitions[key] = previous[key]
                continue
            stamps = capture_times(day, feed)
            gaps = [[a, b, b-a] for a, b in zip(stamps, stamps[1:]) if b-a > 120]
            partitions[key] = dict(feed=feed, date=day.name, signature=sig, count=len(stamps),
                                   first=stamps[0] if stamps else None, last=stamps[-1] if stamps else None,
                                   intervals=max(len(stamps)-1, 0), gaps=gaps)
    result = dict(generated_at=datetime.now(timezone.utc).isoformat(), partitions=partitions)
    target = _cache_path(cfg, 'continuity.json');target.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(target, result)
    return result


def continuity_gaps(cache, start, end):
    """Incluye huecos entre días; recorta a los extremos del período seleccionado."""
    gaps = []
    first_ts = int(datetime.fromisoformat(str(start)).replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.fromisoformat(str(end)).replace(tzinfo=timezone.utc).timestamp()) + 86400
    for feed in POSITION_FEEDS:
        days = sorted((r for r in cache['partitions'].values() if r['feed'] == feed and r['first'] is not None), key=lambda r:r['date'])
        previous = None
        for day in days:
            intervals = list(day['gaps'])
            if previous is not None and day['first']-previous > 120:
                intervals.append([previous, day['first'], day['first']-previous])
            for a,b,seconds in intervals:
                if b > first_ts and a < end_ts:
                    lo,hi=max(a,first_ts),min(b,end_ts)
                    if hi-lo > 120:
                        gaps.append(dict(feed=feed, start=lo, end=hi, seconds=hi-lo))
            previous = day['last']
    return sorted(gaps,key=lambda r:r['seconds'],reverse=True)


def read_day_report(cfg, day):
    path=_cache_path(cfg, f'quality-{day}.json')
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError,json.JSONDecodeError):
        return None


def _sql_files(files):
    return '[' + ','.join("'"+str(p).replace("'","''")+"'" for p in files) + ']'


def _analyze_day(cfg, day):
    """Valida metadatos, reglas de campos y correspondencia con el GTFS conservado."""
    datetime.strptime(day,'%Y-%m-%d')
    root=Path(cfg['storage']['bronze_dir'])
    report=dict(version=2,date=day, generated_at=datetime.now(timezone.utc).isoformat(), rules=[],
                files={}, matching=None, query_errors=[])
    con=duckdb.connect()
    def rule(feed,label,count,total):
        report['rules'].append(dict(feed=feed,rule=label,affected=int(count or 0),total=int(total),
                                    state='Sin registros' if not total else ('Revisar' if count else 'Sin incidencias')))
    try:
        for feed in POSITION_FEEDS:
            folder=root/feed/('date='+day)
            files=sorted(folder.glob('*.parquet'));valid=[];invalid=0
            for file in files:
                try:
                    pq.read_metadata(file);valid.append(file)
                except Exception:
                    invalid+=1
            rawfolder=Path(cfg['storage']['raw_dir'])/feed/day
            original={p.stem[len(feed)+1:] for p in rawfolder.glob(feed+'_*.pb')}
            parsed={p.stem[5:] for p in files}
            report['files'][feed]=dict(original=len(original),parquet=len(files),valid=len(valid),invalid=invalid,
                                       without_parquet=len(original-parsed))
            report['files'][feed]['unpaired_audit']=audit_unpaired(
                [rawfolder/(feed+'_'+stamp+'.pb') for stamp in sorted(original-parsed)],feed)
            if not valid:
                continue
            try:
                con.execute(f'CREATE VIEW {feed} AS SELECT * FROM read_parquet({_sql_files(valid)},union_by_name=true)')
                if feed=='vehicle_positions':
                    n,missing,coords,limits,zero=con.execute('''SELECT count(*),
                      count(*) FILTER(WHERE trip_id IS NULL OR trim(trip_id)=''),
                      count(*) FILTER(WHERE latitude IS NULL OR longitude IS NULL),
                      count(*) FILTER(WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180),
                      count(*) FILTER(WHERE latitude=0 AND longitude=0) FROM vehicle_positions''').fetchone()
                    rule(feed,'Identificador de viaje vacío',missing,n)
                    rule(feed,'Coordenadas ausentes',coords,n)
                    rule(feed,'Coordenadas fuera de rango',limits,n)
                    rule(feed,'Posición 0°, 0° (revisar)',zero,n)
                    key='snapshot_ts,entity_id'
                else:
                    n,missing,stops,delays,outliers=con.execute('''SELECT count(*),
                      count(*) FILTER(WHERE trip_id IS NULL OR trim(trip_id)=''),
                      count(*) FILTER(WHERE stop_id IS NULL OR trim(stop_id)=''),
                      count(*) FILTER(WHERE arrival_delay IS NULL),
                      count(*) FILTER(WHERE arrival_delay < -300 OR arrival_delay > 7200) FROM trip_updates''').fetchone()
                    rule(feed,'Identificador de viaje vacío',missing,n)
                    rule(feed,'Identificador de parada vacío',stops,n)
                    rule(feed,'Retraso estimado ausente',delays,n)
                    rule(feed,'Retraso fuera de −5 a 120 min',outliers,n)
                    key='snapshot_ts,entity_id,stop_id,stop_sequence'
                duplicates=con.execute(f'SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM {feed} GROUP BY {key} HAVING count(*)>1)').fetchone()[0]
                rule(feed,'Repeticiones de la clave de observación',duplicates,n)
            except Exception as exc:
                report['query_errors'].append(dict(feed=feed,error=str(exc)[:300]))
        gtfs=Path(cfg['storage']['raw_dir'])/'gtfs_static/fomento_transit.zip'
        if gtfs.exists() and report['files'].get('vehicle_positions',{}).get('valid'):
            try:
                import zipfile
                with zipfile.ZipFile(gtfs) as z:
                    with z.open('trips.txt') as f:trips=pd.read_csv(f,dtype=str,keep_default_na=False)
                trips.columns=trips.columns.str.strip()
                ids=con.execute("SELECT DISTINCT trip_id FROM vehicle_positions WHERE trip_id IS NOT NULL AND trim(trip_id)<>''").df()['trip_id'].tolist()
                exact=set(trips['trip_id']);normalized={s.strip() for s in exact}
                matched=sum(value in exact for value in ids)
                after=sum(value.strip() in normalized for value in ids)
                report['matching']=dict(total=len(ids),exact=matched,normalized=after,unmatched=len(ids)-after,
                    examples=sorted(value for value in ids if value.strip() not in normalized)[:20],
                    gtfs_saved_at=datetime.fromtimestamp(gtfs.stat().st_mtime,timezone.utc).isoformat())
            except Exception as exc:
                report['query_errors'].append(dict(feed='Cruce GTFS',error=str(exc)[:300]))
    finally:
        con.close()
    target=_cache_path(cfg,f'quality-{day}.json');target.parent.mkdir(parents=True,exist_ok=True)
    _atomic_json(target,report)
    return report


def _locked(cfg, name, operation):
    folder=inventory_path(cfg).parent
    folder.mkdir(parents=True,exist_ok=True)
    with (folder/name).open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Ya hay una comprobación en curso. Puedes consultar el último resultado.') from None
        return operation()


def build_continuity(cfg):
    return _locked(cfg,'continuity.lock',lambda:_build_continuity(cfg))


def analyze_day(cfg,day):
    return _locked(cfg,'quality.lock',lambda:_analyze_day(cfg,day))
