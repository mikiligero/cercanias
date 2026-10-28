"""Selección acotada de archivos para las consultas del panel."""
from datetime import datetime, timezone
from pathlib import Path
import duckdb

FEEDS = ('vehicle_positions', 'trip_updates', 'alerts')


def latest_file(bronze, feed, start_ts=None, end_ts=None):
    root = Path(bronze) / feed
    low = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts is not None else None
    high = datetime.fromtimestamp(end_ts, timezone.utc).strftime('%Y-%m-%d') if end_ts is not None else None
    for day in sorted(root.glob('date=*'), reverse=True):
        date = day.name[5:]
        if (low and date < low) or (high and date > high):
            continue
        for p in sorted(day.glob('part-*.parquet'), reverse=True):
            if start_ts is None and end_ts is None:
                return p
            ts = int(datetime.strptime(p.stem[5:], '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc).timestamp())
            if (start_ts is None or ts >= start_ts) and (end_ts is None or ts <= end_ts):
                return p
    return None


def build_dashboard_connection(cfg, gtfs, start_ts=None, end_ts=None, history=False, period_latest=False):
    bronze = Path(cfg['storage']['bronze_dir'])
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='Europe/Madrid'")
        for feed in FEEDS:
            latest = latest_file(bronze, feed)
            if latest is None:
                raise FileNotFoundError(f'Sin datos disponibles para {feed}')
            files = {latest}
            if history:
                low = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d')
                high = datetime.fromtimestamp(end_ts, timezone.utc).strftime('%Y-%m-%d')
                for day in (bronze / feed).glob('date=*'):
                    if low <= day.name[5:] <= high:
                        files.update(day.glob('part-*.parquet'))
            if period_latest and feed == 'trip_updates':
                selected = latest_file(bronze, feed, start_ts, end_ts)
                files = {selected or latest}
            paths = ','.join("'" + str(p).replace("'", "''") + "'" for p in sorted(files))
            con.execute(f'CREATE VIEW {feed} AS SELECT * FROM read_parquet([{paths}], hive_partitioning=true)')
        if gtfs:
            for table in ('trips', 'routes', 'stops'):
                con.register(table, gtfs[table])
        return con
    except Exception:
        con.close()
        raise
