import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


def _read_feed(pb_path: Path) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_path.read_bytes())
    return feed


def _translated_text(ts) -> str | None:
    """Extrae texto de un TranslatedString, preferiblemente en español."""
    if not ts.translation:
        return None
    for t in ts.translation:
        if t.language in ("es", "es-ES"):
            return t.text
    return ts.translation[0].text


def parse_vehicle_positions(pb_path: Path) -> pd.DataFrame:
    """
    Parsea vehicle_positions.pb. Una fila por entidad.

    Campos notables de Renfe: route_id, start_date y start_time vienen vacíos;
    el join con GTFS estático se hará por trip_id. La línea se puede derivar
    del vehicle_label (ej. 'C1-23530-PLATF.(2)').
    """
    feed = _read_feed(pb_path)
    snapshot_ts = feed.header.timestamp
    now = datetime.now(timezone.utc)

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        has_trip = vp.HasField("trip")
        has_vdesc = vp.HasField("vehicle")
        has_pos = vp.HasField("position")

        rows.append({
            "snapshot_ts":           snapshot_ts,
            "ingested_at":           now,
            "entity_id":             entity.id,
            "trip_id":               vp.trip.trip_id      if has_trip  else None,
            "vehicle_id":            vp.vehicle.id        if has_vdesc else None,
            "vehicle_label":         vp.vehicle.label     if has_vdesc else None,
            "latitude":              vp.position.latitude  if has_pos   else None,
            "longitude":             vp.position.longitude if has_pos   else None,
            "bearing":               vp.position.bearing   if has_pos   else None,
            "speed_ms":              vp.position.speed     if has_pos   else None,
            "stop_id":               vp.stop_id            or None,
            "current_stop_sequence": vp.current_stop_sequence,
            # 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO
            "current_status":        vp.current_status,
            "vehicle_ts":            vp.timestamp          or None,
        })

    if not rows:
        logger.warning("[vehicle_positions] %s: sin entidades", pb_path.name)
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    logger.info("[vehicle_positions] %s: %d filas", pb_path.name, len(df))
    return df


def parse_trip_updates(pb_path: Path) -> pd.DataFrame:
    """
    Parsea trip_updates.pb. Una fila por (entidad, stop_time_update).

    Renfe publica una sola STU por entidad (próxima parada). El campo clave
    es arrival_delay (segundos de retraso) — es la fuente de verdad oficial
    para validar el retraso calculado propio.
    """
    feed = _read_feed(pb_path)
    snapshot_ts = feed.header.timestamp
    now = datetime.now(timezone.utc)

    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        has_trip = tu.HasField("trip")
        has_veh = tu.HasField("vehicle")

        base = {
            "snapshot_ts": snapshot_ts,
            "ingested_at": now,
            "entity_id":   entity.id,
            "trip_id":     tu.trip.trip_id  if has_trip else None,
            "vehicle_id":  tu.vehicle.id    if has_veh  else None,
        }

        for stu in tu.stop_time_update:
            has_arr = stu.HasField("arrival")
            has_dep = stu.HasField("departure")
            rows.append({
                **base,
                "stop_sequence":         stu.stop_sequence,
                "stop_id":               stu.stop_id or None,
                "arrival_delay":         stu.arrival.delay    if has_arr else None,
                "arrival_time":          stu.arrival.time     if has_arr else None,
                "departure_delay":       stu.departure.delay  if has_dep else None,
                "departure_time":        stu.departure.time   if has_dep else None,
                # 0=SCHEDULED, 1=SKIPPED, 2=NO_DATA
                "schedule_relationship": stu.schedule_relationship,
            })

    if not rows:
        logger.warning("[trip_updates] %s: sin filas", pb_path.name)
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    logger.info("[trip_updates] %s: %d filas (%d entidades)",
                pb_path.name, len(df), len(feed.entity))
    return df


def parse_alerts(pb_path: Path) -> pd.DataFrame:
    """
    Parsea alerts.pb. Una fila por alerta.

    Renfe: header_text vacío, texto real en description_text (español).
    active_period.end=0 significa sin fecha de fin → se guarda como None.
    Los route_ids informados se concatenan separados por coma.
    """
    feed = _read_feed(pb_path)
    snapshot_ts = feed.header.timestamp
    now = datetime.now(timezone.utc)

    rows = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        al = entity.alert

        active_start = al.active_period[0].start or None if al.active_period else None
        active_end   = al.active_period[0].end   or None if al.active_period else None

        informed_routes = ",".join(
            ie.route_id for ie in al.informed_entity if ie.route_id
        ) or None
        informed_stops = ",".join(
            ie.stop_id for ie in al.informed_entity if ie.stop_id
        ) or None

        rows.append({
            "snapshot_ts":      snapshot_ts,
            "ingested_at":      now,
            "entity_id":        entity.id,
            # 1=UNKNOWN_CAUSE … 9=MAINTENANCE; 1=UNKNOWN_EFFECT … 9=MODIFIED_SERVICE
            "cause":            al.cause,
            "effect":           al.effect,
            "active_start":     active_start,
            "active_end":       active_end,
            "informed_routes":  informed_routes,
            "informed_stops":   informed_stops,
            "header_text":      _translated_text(al.header_text),
            "description_text": _translated_text(al.description_text),
        })

    if not rows:
        logger.warning("[alerts] %s: sin alertas", pb_path.name)
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    logger.info("[alerts] %s: %d alertas", pb_path.name, len(df))
    return df


def write_parquet(df: pd.DataFrame, bronze_dir: Path, feed_name: str) -> Path:
    """
    Escribe el DataFrame en Parquet particionado por fecha (esquema Hive).
    Ruta: bronze_dir/{feed_name}/date=YYYY-MM-DD/part-{timestamp}.parquet
    El nombre del fichero coincide con el .pb del que procede.
    """
    feed_ts = int(df["snapshot_ts"].iloc[0])
    dt = datetime.fromtimestamp(feed_ts, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    ts_str = dt.strftime("%Y%m%dT%H%M%SZ")

    out_dir = bronze_dir / feed_name / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"part-{ts_str}.parquet"
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

    logger.info("[%s] parquet → %s (%d filas)", feed_name, out_path.name, len(df))
    return out_path


_PARSERS = {
    "vehicle_positions": parse_vehicle_positions,
    "trip_updates":      parse_trip_updates,
    "alerts":            parse_alerts,
}


def process(pb_path: Path, feed_name: str, bronze_dir: Path) -> Path | None:
    """Parsea un .pb y escribe su Parquet. Interfaz principal para el scheduler."""
    parse_fn = _PARSERS.get(feed_name)
    if parse_fn is None:
        logger.error("Parser desconocido: %s", feed_name)
        return None

    df = parse_fn(pb_path)
    if df.empty:
        return None

    return write_parquet(df, bronze_dir, feed_name)
