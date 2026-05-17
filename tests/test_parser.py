"""Tests del parser GTFS-RT.

Usan ficheros .pb reales de data/raw/ como fixtures — sin mocks,
porque queremos verificar que el parser funciona con los datos reales
de Renfe, incluyendo los campos vacíos y las peculiaridades del feed.
"""
import pandas as pd
import pytest
from pathlib import Path
from datetime import timezone

from src.parser import (
    parse_vehicle_positions,
    parse_trip_updates,
    parse_alerts,
    write_parquet,
    process,
)

# ---------------------------------------------------------------------------
# vehicle_positions
# ---------------------------------------------------------------------------

VP_COLUMNS = {
    "snapshot_ts", "ingested_at", "entity_id", "trip_id",
    "vehicle_id", "vehicle_label", "latitude", "longitude",
    "bearing", "speed_ms", "stop_id", "current_stop_sequence",
    "current_status", "vehicle_ts",
}


class TestParseVehiclePositions:

    def test_returns_dataframe(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        assert isinstance(df, pd.DataFrame)

    def test_not_empty(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        assert len(df) > 0

    def test_has_all_columns(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        assert VP_COLUMNS.issubset(df.columns)

    def test_snapshot_ts_is_integer(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        assert pd.api.types.is_integer_dtype(df["snapshot_ts"])

    def test_snapshot_ts_is_recent(self, vp_pb):
        """El feed debe tener un timestamp de los últimos 30 días."""
        from datetime import datetime
        df = parse_vehicle_positions(vp_pb)
        ts = df["snapshot_ts"].iloc[0]
        now = datetime.now(timezone.utc).timestamp()
        assert now - ts < 30 * 24 * 3600, f"snapshot_ts demasiado antiguo: {ts}"

    def test_ingested_at_is_tz_aware(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        assert df["ingested_at"].dt.tz is not None

    def test_coordinates_in_range(self, vp_pb):
        df = parse_vehicle_positions(vp_pb)
        coords = df[["latitude", "longitude"]].dropna()
        assert (coords["latitude"].between(-90, 90)).all()
        assert (coords["longitude"].between(-180, 180)).all()

    def test_coordinates_are_spain(self, vp_pb):
        """Las coordenadas deben estar dentro de la Península + Canarias."""
        df = parse_vehicle_positions(vp_pb)
        coords = df[["latitude", "longitude"]].dropna()
        assert (coords["latitude"].between(27, 44)).all(), "Latitudes fuera de España"
        assert (coords["longitude"].between(-18, 5)).all(), "Longitudes fuera de España"

    def test_current_status_valid_values(self, vp_pb):
        """0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO."""
        df = parse_vehicle_positions(vp_pb)
        assert df["current_status"].isin([0, 1, 2]).all()

    def test_trip_id_format(self, vp_pb):
        """Los trip_id de Renfe siguen el patrón [0-9]+D[0-9]+[A-Z0-9]+."""
        import re
        df = parse_vehicle_positions(vp_pb)
        ids = df["trip_id"].dropna()
        assert len(ids) > 0, "No hay trip_ids"
        pattern = re.compile(r"^\d+D\d+[A-Za-z0-9]+$")
        assert ids.apply(lambda x: bool(pattern.match(x))).all(), \
            f"trip_ids con formato inesperado: {ids[~ids.apply(lambda x: bool(pattern.match(x)))].tolist()}"

    def test_one_row_per_entity(self, vp_pb):
        """vehicle_positions tiene exactamente una fila por entidad."""
        df = parse_vehicle_positions(vp_pb)
        assert df["entity_id"].nunique() == len(df)


# ---------------------------------------------------------------------------
# trip_updates
# ---------------------------------------------------------------------------

TU_COLUMNS = {
    "snapshot_ts", "ingested_at", "entity_id", "trip_id", "vehicle_id",
    "stop_sequence", "stop_id", "arrival_delay", "arrival_time",
    "departure_delay", "departure_time", "schedule_relationship",
}


class TestParseTripUpdates:

    def test_returns_dataframe(self, tu_pb):
        df = parse_trip_updates(tu_pb)
        assert isinstance(df, pd.DataFrame)

    def test_not_empty(self, tu_pb):
        df = parse_trip_updates(tu_pb)
        assert len(df) > 0

    def test_has_all_columns(self, tu_pb):
        df = parse_trip_updates(tu_pb)
        assert TU_COLUMNS.issubset(df.columns)

    def test_arrival_delay_is_numeric(self, tu_pb):
        df = parse_trip_updates(tu_pb)
        delays = df["arrival_delay"].dropna()
        assert pd.api.types.is_numeric_dtype(delays)

    def test_some_delays_are_positive(self, tu_pb):
        """En condiciones normales siempre hay algún tren retrasado."""
        df = parse_trip_updates(tu_pb)
        delays = df["arrival_delay"].dropna()
        assert (delays > 0).any(), "No se encontraron retrasos positivos"

    def test_rows_gte_entities(self, tu_pb):
        """Filas >= entidades porque algunos viajes tienen varias stop_time_updates."""
        from google.transit import gtfs_realtime_pb2
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(tu_pb.read_bytes())
        n_entities = sum(1 for e in feed.entity if e.HasField("trip_update"))

        df = parse_trip_updates(tu_pb)
        assert len(df) >= n_entities

    def test_schedule_relationship_valid(self, tu_pb):
        """0=SCHEDULED, 1=SKIPPED, 2=NO_DATA."""
        df = parse_trip_updates(tu_pb)
        assert df["schedule_relationship"].isin([0, 1, 2]).all()


# ---------------------------------------------------------------------------
# alerts
# ---------------------------------------------------------------------------

AL_COLUMNS = {
    "snapshot_ts", "ingested_at", "entity_id", "cause", "effect",
    "active_start", "active_end", "informed_routes", "informed_stops",
    "header_text", "description_text",
}


class TestParseAlerts:

    def test_returns_dataframe(self, al_pb):
        df = parse_alerts(al_pb)
        assert isinstance(df, pd.DataFrame)

    def test_not_empty(self, al_pb):
        df = parse_alerts(al_pb)
        assert len(df) > 0

    def test_has_all_columns(self, al_pb):
        df = parse_alerts(al_pb)
        assert AL_COLUMNS.issubset(df.columns)

    def test_description_text_has_content(self, al_pb):
        """Renfe usa description_text (no header_text) para el texto de la alerta."""
        df = parse_alerts(al_pb)
        assert df["description_text"].notna().any()

    def test_active_start_is_numeric_or_none(self, al_pb):
        df = parse_alerts(al_pb)
        non_null = df["active_start"].dropna()
        assert pd.api.types.is_numeric_dtype(non_null)

    def test_informed_routes_format(self, al_pb):
        """Las rutas informadas tienen el formato de Renfe: dígitos + T + dígitos + línea."""
        import re
        df = parse_alerts(al_pb)
        routes = df["informed_routes"].dropna()
        if routes.empty:
            pytest.skip("No hay alertas con rutas informadas")
        # Al menos una ruta debe seguir el patrón Renfe
        pattern = re.compile(r"\d+T\d+[A-Za-z0-9]+")
        has_match = routes.apply(lambda x: bool(pattern.search(x))).any()
        assert has_match, "Ninguna ruta sigue el formato esperado de Renfe"


# ---------------------------------------------------------------------------
# write_parquet y process
# ---------------------------------------------------------------------------

class TestWriteParquet:

    def test_creates_file(self, vp_pb, tmp_path):
        df = parse_vehicle_positions(vp_pb)
        out = write_parquet(df, tmp_path, "vehicle_positions")
        assert out.exists()

    def test_hive_partition_in_path(self, vp_pb, tmp_path):
        """La ruta debe contener date=YYYY-MM-DD (esquema Hive)."""
        import re
        df = parse_vehicle_positions(vp_pb)
        out = write_parquet(df, tmp_path, "vehicle_positions")
        assert re.search(r"date=\d{4}-\d{2}-\d{2}", str(out))

    def test_filename_matches_pb(self, vp_pb, tmp_path):
        """El nombre del Parquet debe contener el mismo timestamp que el .pb."""
        df = parse_vehicle_positions(vp_pb)
        out = write_parquet(df, tmp_path, "vehicle_positions")
        ts_from_pb = vp_pb.stem.split("_")[-1]     # 20260517T100446Z
        assert ts_from_pb in out.name

    def test_parquet_readable(self, vp_pb, tmp_path):
        """El fichero Parquet escrito debe poder leerse de nuevo."""
        df = parse_vehicle_positions(vp_pb)
        out = write_parquet(df, tmp_path, "vehicle_positions")
        df2 = pd.read_parquet(out)
        assert len(df2) == len(df)
        assert set(df2.columns) == set(df.columns)

    def test_parquet_roundtrip_values(self, vp_pb, tmp_path):
        """Los valores clave se preservan en el ciclo escritura/lectura."""
        df = parse_vehicle_positions(vp_pb)
        out = write_parquet(df, tmp_path, "vehicle_positions")
        df2 = pd.read_parquet(out)
        assert df["snapshot_ts"].iloc[0] == df2["snapshot_ts"].iloc[0]
        assert df["trip_id"].iloc[0] == df2["trip_id"].iloc[0]


class TestProcess:

    def test_process_vehicle_positions_returns_path(self, vp_pb, tmp_path):
        result = process(vp_pb, "vehicle_positions", tmp_path)
        assert result is not None
        assert result.exists()

    def test_process_trip_updates_returns_path(self, tu_pb, tmp_path):
        result = process(tu_pb, "trip_updates", tmp_path)
        assert result is not None
        assert result.exists()

    def test_process_alerts_returns_path(self, al_pb, tmp_path):
        result = process(al_pb, "alerts", tmp_path)
        assert result is not None
        assert result.exists()

    def test_process_unknown_feed_returns_none(self, vp_pb, tmp_path):
        result = process(vp_pb, "feed_inexistente", tmp_path)
        assert result is None
