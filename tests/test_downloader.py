"""Tests del FeedDownloader.

Mockean la llamada HTTP para no depender de la red,
pero usan .pb reales como payload de respuesta.
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.downloader import FeedDownloader

HTTP_CFG = {"user_agent": "test/0.1", "timeout_seconds": 5}


def _make_downloader(tmp_path, url="https://fake.renfe.test/feed.pb"):
    return FeedDownloader("vehicle_positions", url,
                          tmp_path / "raw", HTTP_CFG)


def _mock_response(pb_path: Path):
    """Crea un mock de requests.Response con el contenido de un .pb real."""
    resp = MagicMock()
    resp.content = pb_path.read_bytes()
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Arranque: inicialización desde disco
# ---------------------------------------------------------------------------

class TestScanLastTimestamp:

    def test_empty_dir_returns_none(self, tmp_path):
        dl = _make_downloader(tmp_path)
        assert dl._last_ts is None

    def test_reads_timestamp_from_existing_file(self, tmp_path, vp_pb):
        """Si ya hay .pb en disco, _last_ts se inicializa desde el nombre del fichero."""
        feed_dir = tmp_path / "raw" / "vehicle_positions" / "2026-05-17"
        feed_dir.mkdir(parents=True)
        # Copiar un .pb real con nombre que contenga timestamp conocido
        dest = feed_dir / "vehicle_positions_20260517T100446Z.pb"
        dest.write_bytes(vp_pb.read_bytes())

        dl = _make_downloader(tmp_path)
        assert dl._last_ts is not None
        # 20260517T100446Z → epoch esperado
        from datetime import datetime, timezone
        expected = int(datetime(2026, 5, 17, 10, 4, 46, tzinfo=timezone.utc).timestamp())
        assert dl._last_ts == expected

    def test_ignores_malformed_filename(self, tmp_path):
        """Ficheros con nombre sin timestamp válido no deben romper la inicialización."""
        feed_dir = tmp_path / "raw" / "vehicle_positions"
        feed_dir.mkdir(parents=True)
        (feed_dir / "vehicle_positions_NOTATIMESTAMP.pb").write_bytes(b"")

        dl = _make_downloader(tmp_path)
        assert dl._last_ts is None


# ---------------------------------------------------------------------------
# Descarga y deduplicación
# ---------------------------------------------------------------------------

class TestFetch:

    def test_saves_pb_on_first_fetch(self, tmp_path, vp_pb):
        dl = _make_downloader(tmp_path)
        with patch.object(dl._session, "get", return_value=_mock_response(vp_pb)):
            result = dl.fetch()

        assert result is not None
        assert result.exists()
        assert result.suffix == ".pb"

    def test_pb_filename_contains_feed_timestamp(self, tmp_path, vp_pb):
        dl = _make_downloader(tmp_path)
        with patch.object(dl._session, "get", return_value=_mock_response(vp_pb)):
            result = dl.fetch()

        # El nombre debe tener el formato feedname_YYYYMMDDTHHMMSSz.pb
        import re
        assert re.search(r"\d{8}T\d{6}Z", result.name)

    def test_dedup_same_timestamp_returns_none(self, tmp_path, vp_pb):
        """Segunda llamada con el mismo feed (mismo header.timestamp) → None."""
        dl = _make_downloader(tmp_path)
        mock_resp = _mock_response(vp_pb)
        with patch.object(dl._session, "get", return_value=mock_resp):
            dl.fetch()          # primera: guarda
            result = dl.fetch() # segunda: mismo .pb, mismo timestamp

        assert result is None

    def test_dedup_different_timestamp_saves(self, tmp_path, vp_pb, tu_pb):
        """Si el timestamp del feed avanza, sí debe guardar."""
        from google.transit import gtfs_realtime_pb2

        dl = _make_downloader(tmp_path)

        # Primer fetch con vp_pb
        with patch.object(dl._session, "get", return_value=_mock_response(vp_pb)):
            dl.fetch()

        # Modificar el timestamp del protobuf para simular un feed actualizado
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(vp_pb.read_bytes())
        original_ts = feed.header.timestamp
        feed.header.timestamp = original_ts + 20  # 20 segundos después

        resp2 = MagicMock()
        resp2.content = feed.SerializeToString()
        resp2.raise_for_status = MagicMock()

        with patch.object(dl._session, "get", return_value=resp2):
            result = dl.fetch()

        assert result is not None
        assert result.exists()

    def test_pb_saved_in_date_subdirectory(self, tmp_path, vp_pb):
        """Los .pb deben guardarse en raw/{feed}/{YYYY-MM-DD}/."""
        dl = _make_downloader(tmp_path)
        with patch.object(dl._session, "get", return_value=_mock_response(vp_pb)):
            result = dl.fetch()

        # El path debe tener el formato .../YYYY-MM-DD/fichero.pb
        import re
        assert re.search(r"\d{4}-\d{2}-\d{2}", str(result.parent))

    def test_network_error_returns_none(self, tmp_path):
        """Un error de red no debe lanzar excepción — devuelve None."""
        import requests
        dl = _make_downloader(tmp_path)
        with patch.object(dl._session, "get",
                          side_effect=requests.ConnectionError("timeout")):
            result = dl.fetch()

        assert result is None

    def test_updates_last_ts_after_save(self, tmp_path, vp_pb):
        """Tras guardar, _last_ts debe reflejar el timestamp del feed descargado."""
        from google.transit import gtfs_realtime_pb2
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(vp_pb.read_bytes())
        expected_ts = feed.header.timestamp

        dl = _make_downloader(tmp_path)
        with patch.object(dl._session, "get", return_value=_mock_response(vp_pb)):
            dl.fetch()

        assert dl._last_ts == expected_ts
