"""
Capa de consulta DuckDB (in-memory).

Uso típico:
    from src.config import load
    from src.query import build_connection, snapshot_summary, delays_by_trip
    from src.query import punctuality_over_time, active_alerts

    con = build_connection(load())
    delays_by_trip(con).head(10)
"""
import logging
import zipfile
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _read_gtfs_csv(zip_path: Path, name: str) -> pd.DataFrame:
    """Lee un fichero del ZIP estático limpiando los espacios de Renfe."""
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(name) as f:
            df = pd.read_csv(f, dtype=str)
    df.columns = df.columns.str.strip()
    return df.apply(lambda c: c.str.strip() if c.dtype == "object" else c)


# ---------------------------------------------------------------------------
# Conexión principal
# ---------------------------------------------------------------------------

def build_connection(cfg: dict) -> duckdb.DuckDBPyConnection:
    """
    Crea una conexión DuckDB in-memory con:
      - Vistas sobre los Parquet de bronze (vehicle_positions, trip_updates, alerts)
      - Tablas del GTFS estático registradas (trips, routes, stops)

    Llámalo una vez al arrancar y reutiliza la conexión en todas las consultas.
    """
    bronze_dir = Path(cfg["storage"]["bronze_dir"])
    zip_path   = Path(cfg["storage"]["raw_dir"]) / "gtfs_static" / "fomento_transit.zip"

    con = duckdb.connect()

    # --- Vistas sobre bronze (lectura lazy de Parquet) ---
    for feed in ("vehicle_positions", "trip_updates", "alerts"):
        pattern = str(bronze_dir / feed / "**" / "*.parquet")
        con.execute(f"""
            CREATE VIEW {feed} AS
            SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {feed}").fetchone()[0]
        logger.info("Vista '%s': %d filas", feed, count)

    # --- Tablas GTFS estático (en memoria, leídas del ZIP) ---
    if zip_path.exists():
        trips  = _read_gtfs_csv(zip_path, "trips.txt")[["trip_id", "route_id", "service_id"]]
        routes = _read_gtfs_csv(zip_path, "routes.txt")[["route_id", "route_short_name", "route_long_name"]]
        stops  = _read_gtfs_csv(zip_path, "stops.txt")[["stop_id", "stop_name", "stop_lat", "stop_lon"]]

        con.register("trips",  trips)
        con.register("routes", routes)
        con.register("stops",  stops)
        logger.info("GTFS estático cargado: %d trips, %d routes, %d stops",
                    len(trips), len(routes), len(stops))
    else:
        logger.warning("ZIP estático no encontrado en %s — joins enriquecidos no disponibles", zip_path)

    return con


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------

def snapshot_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Estado de la red en el último snapshot de trip_updates.
    Devuelve una fila con totales y métricas de retraso.
    """
    return con.execute("""
        WITH latest AS (
            SELECT max(snapshot_ts) AS ts FROM trip_updates
        )
        SELECT
            strftime(to_timestamp(any_value(ts)), '%Y-%m-%d %H:%M:%S UTC') AS snapshot,
            count(*)                                             AS trenes,
            sum(CASE WHEN arrival_delay >  0 THEN 1 END)        AS con_retraso,
            sum(CASE WHEN arrival_delay <= 0 THEN 1 END)        AS a_tiempo,
            round(avg(CASE WHEN arrival_delay > 0
                          THEN arrival_delay END) / 60.0, 1)    AS retraso_medio_min,
            round(max(arrival_delay) / 60.0, 1)                 AS retraso_max_min
        FROM trip_updates
        JOIN latest ON snapshot_ts = ts
        WHERE arrival_delay IS NOT NULL
    """).df()


def delays_by_trip(
    con: duckdb.DuckDBPyConnection,
    snapshot_ts: int | None = None,
    min_delay_sec: int = 60,
) -> pd.DataFrame:
    """
    Tabla de retrasos por tren, enriquecida con línea y nombre de parada.

    Args:
        snapshot_ts:    Epoch del snapshot a consultar. None = último disponible.
        min_delay_sec:  Solo incluye trenes con retraso >= este valor (default 60s).

    Devuelve columnas: linea, trip_id, parada, retraso_min, retraso_seg.
    """
    ts_filter = (
        f"snapshot_ts = {snapshot_ts}"
        if snapshot_ts
        else "snapshot_ts = (SELECT max(snapshot_ts) FROM trip_updates)"
    )
    return con.execute(f"""
        SELECT
            r.route_short_name                  AS linea,
            tu.trip_id,
            s.stop_name                         AS parada,
            round(tu.arrival_delay / 60.0, 1)  AS retraso_min,
            tu.arrival_delay                    AS retraso_seg
        FROM trip_updates tu
        JOIN trips  t ON tu.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        JOIN stops  s ON tu.stop_id = s.stop_id
        WHERE {ts_filter}
          AND tu.arrival_delay >= {min_delay_sec}
        ORDER BY tu.arrival_delay DESC
    """).df()


def punctuality_over_time(
    con: duckdb.DuckDBPyConnection,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> pd.DataFrame:
    """
    Serie temporal de puntualidad: una fila por snapshot.
    Columnas: ts (timestamp), trenes, pct_puntual, retraso_medio_min.
    """
    ts_filter = ""
    if start_ts is not None and end_ts is not None:
        ts_filter = f"AND snapshot_ts BETWEEN {start_ts} AND {end_ts}"
    return con.execute(f"""
        SELECT
            to_timestamp(snapshot_ts)                        AS ts,
            count(*)                                         AS trenes,
            round(100.0 * sum(CASE WHEN arrival_delay <= 0
                                   THEN 1 ELSE 0 END)
                        / count(*), 1)                       AS pct_puntual,
            round(avg(arrival_delay) / 60.0, 1)             AS retraso_medio_min
        FROM trip_updates
        WHERE arrival_delay IS NOT NULL
          {ts_filter}
        GROUP BY snapshot_ts
        ORDER BY snapshot_ts
    """).df()


def active_alerts(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Alertas activas en el último snapshot, con texto y rutas afectadas.
    """
    return con.execute("""
        WITH latest AS (
            SELECT max(snapshot_ts) AS ts FROM alerts
        )
        SELECT
            entity_id,
            informed_routes,
            strftime(to_timestamp(active_start), '%Y-%m-%d %H:%M') AS desde,
            description_text
        FROM alerts
        JOIN latest ON snapshot_ts = ts
        ORDER BY active_start DESC
    """).df()


def network_heatmap(
    con: duckdb.DuckDBPyConnection,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> pd.DataFrame:
    """
    Retraso medio por parada a lo largo del período seleccionado.
    Útil para identificar los puntos negros de la red.
    Devuelve: stop_name, stop_lat, stop_lon, n_observaciones, retraso_medio_min.
    """
    ts_filter = ""
    if start_ts is not None and end_ts is not None:
        ts_filter = f"AND tu.snapshot_ts BETWEEN {start_ts} AND {end_ts}"
    return con.execute(f"""
        SELECT
            s.stop_name,
            CAST(s.stop_lat AS DOUBLE) AS lat,
            CAST(s.stop_lon AS DOUBLE) AS lon,
            count(*)                                        AS n_obs,
            round(avg(tu.arrival_delay) / 60.0, 1)         AS retraso_medio_min
        FROM trip_updates tu
        JOIN stops s ON tu.stop_id = s.stop_id
        WHERE tu.arrival_delay IS NOT NULL
          {ts_filter}
        GROUP BY s.stop_name, s.stop_lat, s.stop_lon
        HAVING count(*) >= 3
        ORDER BY retraso_medio_min DESC
    """).df()
