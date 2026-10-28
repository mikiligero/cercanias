"""Inventario incremental de los archivos conservados del piloto."""
import argparse
import fcntl
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from src.config import load

FEEDS = ('vehicle_positions', 'trip_updates', 'alerts')
SCHEMA_VERSION = 1
logger = logging.getLogger(__name__)


def inventory_path(cfg):
    return Path(cfg['storage']['bronze_dir']).parent / 'inventory' / 'pilot.json'


def read_inventory(cfg):
    try:
        data = json.loads(inventory_path(cfg).read_text())
        return data if data.get('version') == SCHEMA_VERSION else None
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _signature(path):
    try:
        st = path.stat()
        return [st.st_ino, st.st_mtime_ns]
    except FileNotFoundError:
        return None


def _scan_partition(raw, bronze, feed, day):
    result = dict(feed=feed, date=day, raw_files=0, parquet_files=0,
                  valid_parquet_files=0, rows=0, raw_bytes=0, parquet_bytes=0,
                  invalid_parquet_files=0, unexpected_names=0, errors=[], first=None, last=None)
    stamps = []
    for folder, suffix, key in ((raw, '.pb', 'raw'), (bronze, '.parquet', 'parquet')):
        if not folder.exists():
            continue
        for entry in os.scandir(folder):
            if not entry.name.endswith(suffix) or not entry.is_file():
                continue
            try:
                st = entry.stat()
            except FileNotFoundError:
                continue
            result[key + '_files'] += 1
            result[key + '_bytes'] += st.st_size
            if key == 'parquet':
                try:
                    metadata = pq.read_metadata(entry.path)
                    result['rows'] += metadata.num_rows
                    result['valid_parquet_files'] += 1
                except Exception as exc:
                    result['invalid_parquet_files'] += 1
                    if len(result['errors']) < 5:
                        result['errors'].append({'file': entry.name, 'error': str(exc)[:160]})
                    continue
            prefix = feed + '_' if key == 'raw' else 'part-'
            try:
                if not entry.name.startswith(prefix):
                    raise ValueError('Nombre inesperado')
                stamp = entry.name[len(prefix):-len(suffix)]
                dt = datetime.strptime(stamp, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                stamps.append(dt.isoformat())
            except ValueError:
                result['unexpected_names'] += 1
    if stamps:
        result['first'], result['last'] = min(stamps), max(stamps)
    return result


def _atomic_json(path, data):
    fd, name = tempfile.mkstemp(prefix='.pilot-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(data, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def build_inventory(cfg, force=False, progress=None):
    target = inventory_path(cfg)
    target.parent.mkdir(parents=True, exist_ok=True)
    with (target.parent / 'pilot.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Ya hay una actualización del inventario en curso.') from None
        old = read_inventory(cfg) or {}
        previous = old.get('partitions', {})
        partitions = {}
        for feed in FEEDS:
            raw_root = Path(cfg['storage']['raw_dir']) / feed
            bronze_root = Path(cfg['storage']['bronze_dir']) / feed
            dates = set()
            for root, prefix in ((raw_root, ''), (bronze_root, 'date=')):
                if root.exists():
                    for p in root.iterdir():
                        value = p.name[len(prefix):]
                        try:
                            datetime.strptime(value, '%Y-%m-%d')
                        except ValueError:
                            continue
                        if p.is_dir() and (not prefix or p.name.startswith(prefix)):
                            dates.add(value)
            for day in sorted(dates):
                key = feed + '/' + day
                raw, bronze = raw_root / day, bronze_root / ('date=' + day)
                signature = [_signature(raw), _signature(bronze)]
                cached = previous.get(key)
                if not force and cached and cached.get('signature') == signature:
                    partitions[key] = cached
                    continue
                row = _scan_partition(raw, bronze, feed, day)
                # Si el capturador añade archivos durante el recuento, se revisará en la próxima ejecución.
                row['signature'] = signature
                partitions[key] = row
                if progress:
                    progress(row)
        gtfs_path = Path(cfg['storage']['raw_dir']) / 'gtfs_static' / 'fomento_transit.zip'
        gtfs_bytes = gtfs_path.stat().st_size if gtfs_path.exists() else 0
        result = dict(version=SCHEMA_VERSION,
                      generated_at=datetime.now(timezone.utc).isoformat(),
                      partitions=partitions, gtfs_bytes=gtfs_bytes)
        _atomic_json(target, result)
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    def report(row):
        print(row['feed'], row['date'], 'snapshots', row['raw_files'],
              'rows', row['rows'], 'invalid', row['invalid_parquet_files'], flush=True)
    result = build_inventory(load(), force=args.force, progress=report)
    print('DONE', result['generated_at'], len(result['partitions']), flush=True)
