from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
import json
import zipfile
import pandas as pd
from google.transit import gtfs_realtime_pb2 as pb
from src.parser import parse_trip_updates,write_parquet
from src.trace_data import (load_capture,load_reference,source_observation,enrich,lookup,
    freeze_demo,load_demo,indicator,make_evidence,evidence_zip,file_hash)


def seed_trace(tmp_path):
    cfg={'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}
    feed=pb.FeedMessage();feed.header.gtfs_realtime_version='2.0'
    feed.header.timestamp=int(datetime(2026,10,27,7,30,10,tzinfo=timezone.utc).timestamp())
    for i,trip in enumerate(('A',' B ','MISSING','A')):
        e=feed.entity.add();e.id=f'e{i}';e.trip_update.trip.trip_id=trip
        stu=e.trip_update.stop_time_update.add();stu.stop_id='S';stu.stop_sequence=1
        if i==0:
            stu.arrival.delay=180
            second=e.trip_update.stop_time_update.add();second.stop_id='S2';second.stop_sequence=2;second.arrival.delay=0
        elif i==1:stu.arrival.delay=120
        elif i==2:stu.arrival.time=feed.header.timestamp+120
    folder=tmp_path/'raw/trip_updates/2026-10-27';folder.mkdir(parents=True)
    raw=folder/'trip_updates_20261027T073010Z.pb';raw.write_bytes(feed.SerializeToString())
    parquet=write_parquet(parse_trip_updates(raw),tmp_path/'bronze','trip_updates')
    static=tmp_path/'raw/gtfs_static';static.mkdir(parents=True)
    gtfs=static/'fomento_transit.zip'
    with zipfile.ZipFile(gtfs,'w') as z:
        z.writestr('trips.txt','trip_id,route_id,service_id\nA,R1,WK\nB,R1,WK\n')
        z.writestr('routes.txt','route_id,route_short_name,route_long_name\nR1,C1,Ejemplo\n')
        z.writestr('stops.txt','stop_id,stop_name\nS,Estacion ejemplo\nS2,Otra estacion\n')
    return cfg,raw,parquet,gtfs


def test_raw_replays_parquet_and_tracks_each_stop_update(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path)
    before=(file_hash(raw),file_hash(parquet))
    capture=load_capture(raw,parquet)
    assert capture['comparison']['verified']
    assert len(capture['saved'])==5 and len(capture['feed'].entity)==4
    source=source_observation(capture,1)
    assert source['entity_index']==0 and source['stop_update_index']==1
    assert source['entity']['trip_update']['stop_time_update'][1]['stop_id']=='S2'
    absent=source_observation(capture,3)
    assert next(f for f in absent['fields'] if f['column']=='arrival_delay')['present'] is False
    assert capture['saved'].iloc[3]['arrival_delay']==0
    assert before==(file_hash(raw),file_hash(parquet))


def test_changed_parquet_is_not_verified(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path)
    data=pd.read_parquet(parquet);data.loc[0,'arrival_delay']=999
    data.to_parquet(parquet,index=False)
    capture=load_capture(raw,parquet)
    assert not capture['comparison']['verified']
    assert capture['comparison']['different_cells']>=1


def test_normalization_and_ambiguity_are_explicit(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path)
    capture=load_capture(raw,parquet);ref=load_reference(gtfs)
    assert enrich(capture['saved'].iloc[0].to_dict(),ref)['category']=='Viaje con coincidencia exacta'
    normalized=enrich(capture['saved'].iloc[2].to_dict(),ref)
    assert normalized['category']=='Viaje que requiere normalizar' and normalized['line']=='C1'
    assert enrich(capture['saved'].iloc[3].to_dict(),ref)['trip']['mode']=='Sin correspondencia'
    ref['trips']['normalized']['X']=[{'trip_id':' X'},{'trip_id':'X '}]
    assert lookup(ref,'trips','X')['mode']=='Ambigua'
    assert lookup(ref,'trips','X')['row'] is None


def test_demo_pins_reference_and_export_is_reproducible(tmp_path):
    cfg,raw,parquet,gtfs=seed_trace(tmp_path)
    manifest=freeze_demo(cfg,raw,parquet)
    assert 'Viaje con coincidencia exacta' in manifest['cases']
    assert 'Viaje que requiere normalizar' in manifest['cases']
    loaded,paths=load_demo(cfg)
    original_hash=file_hash(paths['gtfs'])
    gtfs.write_bytes(b'updated reference')
    assert file_hash(paths['gtfs'])==original_hash
    assert file_hash(paths['gtfs'])!=file_hash(gtfs)
    capture=load_capture(paths['raw'],paths['parquet']);reference=load_reference(paths['gtfs'])
    summary=indicator(paths['parquet'])
    assert summary['trenes']==4 and summary['con_retraso']==2
    assert summary['positive_delay_sum_seconds']==300 and summary['retraso_medio_min']==2.5
    index=manifest['cases']['Viaje que requiere normalizar']
    detail=enrich(capture['saved'].iloc[index].to_dict(),reference)
    evidence=make_evidence(capture,index,reference,detail,summary,paths['raw'],paths['parquet'])
    packet=evidence_zip(evidence,capture,paths['raw'],paths['parquet'])
    with zipfile.ZipFile(BytesIO(packet)) as z:
        payload=json.loads(z.read('observacion.json'))
        assert payload['replay_comparison']['verified']
        assert payload['processed_row']['arrival_delay']==120
        assert payload['gtfs_sha256']==original_hash
        assert z.read('original/'+raw.name)==raw.read_bytes()
        assert all(not name.startswith('/') and '..' not in name for name in z.namelist())
