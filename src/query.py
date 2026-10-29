"""Consultas de indicadores; los filtros analíticos nunca modifican el bruto."""
import re
import zipfile
from pathlib import Path
import duckdb
import pandas as pd
from src.dashboard_data import CURRENT_WINDOW_SECONDS

DELAY_MIN_S, DELAY_MAX_S = -300, 7200
FILTER_DESCRIPTION = ('Filtro del EDA: −5 a 120 min, incluidos. Los valores fuera del '
                      'intervalo se conservan y se cuentan como excluidos del indicador.')


def delay_condition(alias='tu'):
    return f'{alias}.arrival_delay BETWEEN {DELAY_MIN_S} AND {DELAY_MAX_S}'


def nucleus_condition(ncode=None, alias='tu'):
    if ncode is None:
        return 'TRUE'
    codes=list(ncode) if isinstance(ncode,(list,tuple)) else [ncode]
    if not codes:
        return 'FALSE'
    if any(not re.fullmatch(r'\d+', str(code)) for code in codes):
        raise ValueError('Código de núcleo no válido')
    selected=','.join("'"+str(code)+"'" for code in codes)
    return f"""EXISTS (SELECT 1 FROM trips t JOIN routes r ON t.route_id=r.route_id
       WHERE trim({alias}.trip_id)=t.trip_id AND regexp_extract(r.route_id,'^(\\d+)T',1) IN ({selected}))"""


def _read_gtfs_csv(zip_path, name):
    with zipfile.ZipFile(zip_path) as z:
        with z.open(name) as f:
            df = pd.read_csv(f, dtype=str)
    df.columns = df.columns.str.strip()
    return df.apply(lambda c: c.str.strip() if c.dtype == 'object' else c)


def current_snapshot_sql(ncode=None):
    """Última presencia del ámbito antes de aplicar el filtro de retrasos."""
    if ncode is None:
        return '(SELECT max(snapshot_ts) FROM trip_updates)'
    return f'''(SELECT max(tu.snapshot_ts) FROM trip_updates tu
        WHERE {nucleus_condition(ncode)} AND tu.snapshot_ts >=
        (SELECT max(snapshot_ts)-{CURRENT_WINDOW_SECONDS} FROM trip_updates))'''


def build_connection(cfg):
    """Conexión general para análisis explícitos del histórico completo."""
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='Europe/Madrid'")
        for feed in ('vehicle_positions', 'trip_updates', 'alerts'):
            pattern = str(Path(cfg['storage']['bronze_dir']) / feed / '**/*.parquet').replace("'", "''")
            con.execute(f"CREATE VIEW {feed} AS SELECT * FROM read_parquet('{pattern}',hive_partitioning=true)")
        path = Path(cfg['storage']['raw_dir']) / 'gtfs_static/fomento_transit.zip'
        if path.exists():
            for table, columns in (
                ('trips', ['trip_id', 'route_id', 'service_id']),
                ('routes', ['route_id', 'route_short_name', 'route_long_name']),
                ('stops', ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])):
                con.register(table, _read_gtfs_csv(path, table + '.txt')[columns])
        return con
    except Exception:
        con.close()
        raise


def snapshot_summary(con, ncode=None, filtered=True):
    """Última captura del ámbito, sin retroceder si no quedan valores válidos.

    `trenes` se mantiene por compatibilidad: cuenta filas, no trenes.
    Trazabilidad usa filtered=False para reproducir el archivo conservado.
    """
    valid = delay_condition() if filtered else 'tu.arrival_delay IS NOT NULL'
    return con.execute(f"""
      WITH latest AS (SELECT {current_snapshot_sql(ncode)} ts),
      scoped AS (SELECT tu.* FROM trip_updates tu JOIN latest ON tu.snapshot_ts=ts
                 WHERE {nucleus_condition(ncode)})
      SELECT (SELECT ts FROM latest) snapshot_ts,
        (SELECT strftime(to_timestamp(ts), '%Y-%m-%d %H:%M:%S %Z') FROM latest) AS "snapshot",
        count(*) total_observaciones,
        count(*) FILTER(WHERE tu.arrival_delay IS NULL) sin_delay,
        count(*) FILTER(WHERE tu.arrival_delay < {DELAY_MIN_S} OR tu.arrival_delay > {DELAY_MAX_S}) fuera_rango,
        count(*) FILTER(WHERE {valid}) trenes,
        count(DISTINCT tu.trip_id) FILTER(WHERE {valid}) viajes,
        count(*) FILTER(WHERE {valid} AND tu.arrival_delay > 0) con_retraso,
        count(*) FILTER(WHERE {valid} AND tu.arrival_delay <= 0) a_tiempo,
        round(avg(tu.arrival_delay) FILTER(WHERE {valid} AND tu.arrival_delay > 0)/60.0,1) retraso_medio_min,
        round(max(tu.arrival_delay) FILTER(WHERE {valid})/60.0,1) retraso_max_min
      FROM scoped tu
    """).df()


def delay_audit(con, start_ts=None, end_ts=None, ncode=None):
    period = (f'tu.snapshot_ts BETWEEN {int(start_ts)} AND {int(end_ts)}'
              if start_ts is not None and end_ts is not None else
              f'tu.snapshot_ts={current_snapshot_sql(ncode)}')
    return con.execute(f"""SELECT count(*) total,
        count(*) FILTER(WHERE tu.arrival_delay IS NULL) sin_delay,
        count(*) FILTER(WHERE {delay_condition()}) validas,
        count(*) FILTER(WHERE tu.arrival_delay < {DELAY_MIN_S} OR tu.arrival_delay > {DELAY_MAX_S}) excluidas
        FROM trip_updates tu WHERE {period} AND {nucleus_condition(ncode)}""").df().iloc[0].to_dict()


def delays_by_trip(con, snapshot_ts=None, min_delay_sec=60, ncode=None):
    stamp = str(int(snapshot_ts)) if snapshot_ts is not None else current_snapshot_sql(ncode)
    return con.execute(f"""SELECT r.route_short_name linea,tu.trip_id,s.stop_name parada,
        round(tu.arrival_delay/60.0,1) retraso_min,tu.arrival_delay retraso_seg
        FROM trip_updates tu JOIN trips t ON trim(tu.trip_id)=t.trip_id
        JOIN routes r ON t.route_id=r.route_id LEFT JOIN stops s ON trim(tu.stop_id)=s.stop_id
        WHERE tu.snapshot_ts={stamp} AND {delay_condition()}
        AND tu.arrival_delay >= {int(min_delay_sec)} AND {nucleus_condition(ncode)}
        ORDER BY tu.arrival_delay DESC""").df()


def punctuality_over_time(con, start_ts=None, end_ts=None, ncode=None):
    period = f'AND tu.snapshot_ts BETWEEN {int(start_ts)} AND {int(end_ts)}' if start_ts is not None and end_ts is not None else ''
    return con.execute(f"""SELECT to_timestamp(tu.snapshot_ts) ts,count(*) trenes,
        round(100.0*count(*) FILTER(WHERE tu.arrival_delay<=0)/count(*),1) pct_puntual,
        round(avg(tu.arrival_delay)/60.0,1) retraso_medio_min
        FROM trip_updates tu WHERE {delay_condition()} AND {nucleus_condition(ncode)} {period}
        GROUP BY tu.snapshot_ts ORDER BY tu.snapshot_ts""").df()


def active_alerts(con, now_ts=None):
    """Snapshots recientes y primer intervalo de vigencia conservado en Bronze."""
    import time
    now_ts = int(time.time() if now_ts is None else now_ts)
    return con.execute("""WITH latest AS (SELECT max(snapshot_ts) ts FROM alerts)
        SELECT entity_id,informed_routes,active_start,active_end,description_text
        FROM alerts JOIN latest ON snapshot_ts=ts
        WHERE ts BETWEEN ? AND ? AND (active_start IS NULL OR active_start<=?)
          AND (active_end IS NULL OR active_end>?) ORDER BY active_start DESC""",
        [now_ts-600, now_ts+60, now_ts, now_ts]).df()


def network_heatmap(con, start_ts=None, end_ts=None, ncode=None):
    period = f'AND tu.snapshot_ts BETWEEN {int(start_ts)} AND {int(end_ts)}' if start_ts is not None and end_ts is not None else ''
    return con.execute(f"""SELECT s.stop_name,try_cast(s.stop_lat AS DOUBLE) lat,
        try_cast(s.stop_lon AS DOUBLE) lon,count(*) n_obs,
        round(avg(tu.arrival_delay)/60.0,1) retraso_medio_min
        FROM trip_updates tu JOIN stops s ON trim(tu.stop_id)=s.stop_id
        WHERE {delay_condition()} AND {nucleus_condition(ncode)} {period}
        GROUP BY s.stop_name,s.stop_lat,s.stop_lon HAVING count(*)>=3
        ORDER BY retraso_medio_min DESC""").df()


def nucleus_current(con):
    return con.execute(f"""WITH
      mapped_vp AS (
        SELECT vp.*,r.nucleus_code ncode FROM vehicle_positions vp
        JOIN trips t ON trim(vp.trip_id)=t.trip_id JOIN routes r ON t.route_id=r.route_id
        WHERE r.nucleus_code IS NOT NULL AND vp.snapshot_ts >=
          (SELECT max(snapshot_ts)-{CURRENT_WINDOW_SECONDS} FROM vehicle_positions)),
      mapped_tu AS (
        SELECT tu.*,r.nucleus_code ncode FROM trip_updates tu
        JOIN trips t ON trim(tu.trip_id)=t.trip_id JOIN routes r ON t.route_id=r.route_id
        WHERE r.nucleus_code IS NOT NULL AND tu.snapshot_ts >=
          (SELECT max(snapshot_ts)-{CURRENT_WINDOW_SECONDS} FROM trip_updates)),
      latest_vp AS (SELECT ncode,max(snapshot_ts) ts FROM mapped_vp GROUP BY ncode),
      latest_tu AS (SELECT ncode,max(snapshot_ts) ts FROM mapped_tu GROUP BY ncode),
      positions AS (
        SELECT vp.ncode,max(vp.snapshot_ts) positions_ts,count(DISTINCT vp.entity_id) circulando
        FROM mapped_vp vp JOIN latest_vp ON vp.snapshot_ts=latest_vp.ts AND vp.ncode=latest_vp.ncode
        GROUP BY vp.ncode),
      delays AS (
        SELECT tu.ncode,max(tu.snapshot_ts) delays_ts,
          round(max(tu.arrival_delay) FILTER(WHERE {delay_condition()})/60.0,1) max_min,
          round(avg(tu.arrival_delay) FILTER(WHERE {delay_condition()})/60.0,1) media_min,
          count(*) FILTER(WHERE {delay_condition()}) validas,
          count(*) FILTER(WHERE tu.arrival_delay < {DELAY_MIN_S} OR tu.arrival_delay > {DELAY_MAX_S}) excluidas,
          count(*) FILTER(WHERE tu.arrival_delay IS NULL) sin_delay
        FROM mapped_tu tu JOIN latest_tu ON tu.snapshot_ts=latest_tu.ts AND tu.ncode=latest_tu.ncode
        GROUP BY tu.ncode)
      SELECT coalesce(p.ncode,d.ncode) ncode,p.circulando,
        p.positions_ts,d.delays_ts,
        d.max_min retraso_max_min,d.media_min retraso_medio_min,
        coalesce(d.validas,0) validas,coalesce(d.excluidas,0) excluidas,coalesce(d.sin_delay,0) sin_delay
      FROM positions p FULL OUTER JOIN delays d ON p.ncode=d.ncode
      ORDER BY p.circulando DESC NULLS LAST""").df()


def nucleus_history(con, start_ts, end_ts):
    return con.execute(f"""SELECT to_timestamp(tu.snapshot_ts) ts,r.nucleus_code ncode,
        count(*) n_obs,sum(tu.arrival_delay)/60.0 suma_min,
        round(avg(tu.arrival_delay)/60.0,1) media_min,round(max(tu.arrival_delay)/60.0,1) max_min
        FROM trip_updates tu JOIN trips t ON trim(tu.trip_id)=t.trip_id
        JOIN routes r ON t.route_id=r.route_id WHERE {delay_condition()}
        AND r.nucleus_code IS NOT NULL AND tu.snapshot_ts BETWEEN {int(start_ts)} AND {int(end_ts)}
        GROUP BY tu.snapshot_ts,r.nucleus_code ORDER BY tu.snapshot_ts""").df()


def nucleus_top_delays(con, ncode=None):
    return con.execute(f"""SELECT r.nucleus_code ncode,r.route_short_name linea,tu.trip_id,
        round(tu.arrival_delay/60.0,1) retraso_min FROM trip_updates tu
        JOIN trips t ON trim(tu.trip_id)=t.trip_id JOIN routes r ON t.route_id=r.route_id
        WHERE tu.snapshot_ts={current_snapshot_sql(ncode)}
        AND {delay_condition()} AND tu.arrival_delay>60 AND {nucleus_condition(ncode)}
        ORDER BY tu.arrival_delay DESC LIMIT 20""").df()
