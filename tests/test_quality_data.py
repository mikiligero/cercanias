from pathlib import Path
from datetime import datetime,timezone
import zipfile
import pandas as pd
from src.quality_data import build_continuity,continuity_gaps,analyze_day,read_day_report


def cfg(tmp_path):
    return {'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}


def test_continuity_tracks_missing_day_and_cross_midnight(tmp_path):
    config=cfg(tmp_path)
    for stamp in ('20260602T235000Z','20260604T001000Z','20260604T001030Z'):
        folder=tmp_path/'raw/vehicle_positions'/f'{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}'
        folder.mkdir(parents=True,exist_ok=True)
        (folder/f'vehicle_positions_{stamp}.pb').write_bytes(b'original')
    cache=build_continuity(config)
    all_gaps=continuity_gaps(cache,'2026-06-02','2026-06-04')
    assert len(all_gaps)==1 and all_gaps[0]['seconds']==87600
    missing_day=continuity_gaps(cache,'2026-06-03','2026-06-03')
    assert len(missing_day)==1 and missing_day[0]['seconds']==86400
    assert continuity_gaps(cache,'2026-06-05','2026-06-05')==[]
    assert build_continuity(config)['partitions']==cache['partitions']


def seed_quality(tmp_path):
    config=cfg(tmp_path);day='2026-10-27'
    vp=tmp_path/'bronze/vehicle_positions'/('date='+day)
    tu=tmp_path/'bronze/trip_updates'/('date='+day)
    vp.mkdir(parents=True);tu.mkdir(parents=True)
    pd.DataFrame({'snapshot_ts':[1]*4,'entity_id':['e1','e2','e2','e3'],
      'trip_id':['A',' A ','B',None],'latitude':[40,None,100,0],'longitude':[-3,-3,0,0]}).to_parquet(vp/'part-20261027T120000Z.parquet',index=False)
    (vp/'part-20261027T120030Z.parquet').write_bytes(b'broken')
    raw=tmp_path/'raw/vehicle_positions'/day;raw.mkdir(parents=True)
    for stamp in ('120000','120030','120100'):
        (raw/f'vehicle_positions_20261027T{stamp}Z.pb').write_bytes(b'original')
    pd.DataFrame({'snapshot_ts':[1]*4,'entity_id':['u1','u2','u3','u4'],
      'trip_id':['A','A','A',None],'stop_id':['s',None,'s','s'],'stop_sequence':[1]*4,
      'arrival_delay':[0,None,-301,7201]}).to_parquet(tu/'part-20261027T120000Z.parquet',index=False)
    static=tmp_path/'raw/gtfs_static';static.mkdir(parents=True)
    with zipfile.ZipFile(static/'fomento_transit.zip','w') as z:
        z.writestr('trips.txt','trip_id,route_id\n A ,10TC1\n')
    return config,day


def test_day_quality_has_fixed_denominator_and_explicit_failures(tmp_path):
    config,day=seed_quality(tmp_path);report=analyze_day(config,day)
    assert not report['query_errors']
    assert report['matching']['total']==3
    assert report['matching']['exact']==1
    assert report['matching']['normalized']==2
    assert report['matching']['unmatched']==1
    files=report['files']['vehicle_positions']
    assert (files['original'],files['valid'],files['invalid'],files['without_parquet'])==(3,1,1,1)
    rules={(r['feed'],r['rule']):r for r in report['rules']}
    assert rules['vehicle_positions','Coordenadas ausentes']['affected']==1
    assert rules['vehicle_positions','Coordenadas fuera de rango']['affected']==1
    assert rules['vehicle_positions','Repeticiones de la clave de observación']['affected']==1
    assert rules['trip_updates','Retraso estimado ausente']['affected']==1
    assert rules['trip_updates','Retraso fuera de −5 a 120 min']['affected']==2
    assert read_day_report(config,day)==report


def test_empty_day_is_not_reported_as_passed(tmp_path):
    report=analyze_day(cfg(tmp_path),'2026-10-01')
    assert report['matching'] is None and report['rules']==[]
    assert all(v['parquet']==0 for v in report['files'].values())


def test_schema_error_is_retained_in_report(tmp_path):
    config=cfg(tmp_path)
    folder=tmp_path/'bronze/vehicle_positions/date=2026-10-01';folder.mkdir(parents=True)
    pd.DataFrame({'unexpected':[1]}).to_parquet(folder/'part-20261001T120000Z.parquet',index=False)
    report=analyze_day(config,'2026-10-01')
    assert report['query_errors']
    assert report['rules']==[]
    assert report['matching'] is None


def test_empty_parquet_has_no_passed_rules(tmp_path):
    config,day=seed_quality(tmp_path)
    for feed in ('vehicle_positions','trip_updates'):
        folder=tmp_path/'bronze'/feed/('date='+day)
        for path in folder.glob('*.parquet'):
            if path.read_bytes()[:4]==b'PAR1':
                import pyarrow.parquet as pq
                data=pq.ParquetFile(path).read()
                pq.write_table(data.slice(0,0),path)
    report=analyze_day(config,day)
    assert report['rules']
    assert all(rule['state']=='Sin registros' for rule in report['rules'])
