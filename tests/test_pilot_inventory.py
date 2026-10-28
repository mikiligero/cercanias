import json
from pathlib import Path
import pandas as pd
import pytest
from src import pilot_inventory as inv


def config(tmp_path):
    return {'storage': {'raw_dir': str(tmp_path/'raw'), 'bronze_dir': str(tmp_path/'bronze')}}


def add_capture(cfg, stamp='20260517T120000Z', rows=3):
    raw = Path(cfg['storage']['raw_dir'])/'vehicle_positions/2026-05-17'
    bronze = Path(cfg['storage']['bronze_dir'])/'vehicle_positions/date=2026-05-17'
    raw.mkdir(parents=True, exist_ok=True); bronze.mkdir(parents=True, exist_ok=True)
    (raw/f'vehicle_positions_{stamp}.pb').write_bytes(b'original')
    p=bronze/f'part-{stamp}.parquet'
    pd.DataFrame({'snapshot_ts': [1]*rows}).to_parquet(p, index=False)
    return raw, bronze, p


def test_inventory_counts_valid_rows_and_reports_unreadable_files(tmp_path):
    cfg=config(tmp_path);raw,bronze,p=add_capture(cfg)
    (bronze/'part-20260517T120030Z.parquet').write_bytes(b'broken')
    data=inv.build_inventory(cfg)
    r=data['partitions']['vehicle_positions/2026-05-17']
    assert (r['raw_files'],r['parquet_files'],r['valid_parquet_files'],r['rows'],r['invalid_parquet_files']) == (1,2,1,3,1)
    assert r['raw_bytes']==8
    assert r['parquet_bytes']==p.stat().st_size+6
    assert r['first']==r['last']=='2026-05-17T12:00:00+00:00'
    assert inv.read_inventory(cfg)==data


def test_unchanged_partitions_do_not_reopen_parquet(tmp_path,monkeypatch):
    cfg=config(tmp_path);add_capture(cfg);inv.build_inventory(cfg)
    def forbidden(*args,**kwargs):
        raise AssertionError('El histórico sin cambios no debe releerse')
    monkeypatch.setattr(inv,'_scan_partition',forbidden)
    assert inv.build_inventory(cfg)['partitions']['vehicle_positions/2026-05-17']['rows']==3


def test_new_files_refresh_partition_without_double_counting(tmp_path):
    cfg=config(tmp_path);add_capture(cfg);inv.build_inventory(cfg)
    add_capture(cfg,'20260517T120030Z',5)
    r=inv.build_inventory(cfg)['partitions']['vehicle_positions/2026-05-17']
    assert r['rows']==8 and r['raw_files']==2
    assert r['last']=='2026-05-17T12:00:30+00:00'
    assert inv.build_inventory(cfg)['partitions']['vehicle_positions/2026-05-17']['rows']==8


def test_failed_update_preserves_previous_inventory(tmp_path,monkeypatch):
    cfg=config(tmp_path);add_capture(cfg);before=inv.build_inventory(cfg)
    def fail(*args,**kwargs):
        raise OSError('Lectura fallida')
    monkeypatch.setattr(inv,'_scan_partition',fail)
    with pytest.raises(OSError): inv.build_inventory(cfg,force=True)
    assert inv.read_inventory(cfg)==before
