import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class FeedDownloader:
    """
    Descarga un feed GTFS-RT, deduplica por header.timestamp y persiste el .pb en bruto.

    El timestamp del header del feed se usa tanto para deduplicar (si no avanza, no se escribe)
    como para nombrar el fichero, de modo que el nombre es la verdad sobre cuándo fue generado
    el snapshot — no cuándo lo descargamos nosotros.
    """

    def __init__(self, feed_name: str, url: str, raw_dir: Path, http_cfg: dict):
        self.feed_name = feed_name
        self.url = url
        self.feed_dir = raw_dir / feed_name
        self.feed_dir.mkdir(parents=True, exist_ok=True)
        self._last_ts: int | None = self._scan_last_timestamp()
        self._session = requests.Session()
        self._session.headers["User-Agent"] = http_cfg.get("user_agent", "renfe-gtfs-pilot/0.1")
        self._timeout = http_cfg.get("timeout_seconds", 15)
        logger.debug("[%s] inicializado, último ts en disco: %s", feed_name, self._last_ts)

    def _scan_last_timestamp(self) -> int | None:
        """Inicializa el último timestamp conocido escaneando los ficheros ya guardados en disco."""
        files = sorted(self.feed_dir.rglob(f"{self.feed_name}_*.pb"))
        if not files:
            return None
        stem = files[-1].stem  # ej: vehicle_positions_20260630T165547Z
        ts_str = stem[len(self.feed_name) + 1:]  # ej: 20260630T165547Z
        try:
            dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            logger.warning("[%s] no se pudo parsear timestamp de %s, arrancando desde cero",
                           self.feed_name, files[-1].name)
            return None

    def fetch(self) -> Path | None:
        """
        Descarga el feed. Devuelve el path al .pb guardado, o None si el feed no ha
        cambiado (mismo timestamp) o si la descarga/parseo ha fallado.
        """
        try:
            resp = self._session.get(self.url, timeout=self._timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("[%s] error en descarga: %s", self.feed_name, exc)
            return None

        raw = resp.content

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(raw)
        except Exception as exc:
            logger.error("[%s] error parseando protobuf: %s", self.feed_name, exc)
            return None

        # Si header.timestamp es 0 (campo no informado), usamos la hora actual como fallback
        feed_ts = feed.header.timestamp or int(datetime.now(timezone.utc).timestamp())

        if feed_ts == self._last_ts:
            logger.debug("[%s] sin cambios (ts=%d), omitiendo escritura", self.feed_name, feed_ts)
            return None

        dt = datetime.fromtimestamp(feed_ts, tz=timezone.utc)
        date_dir = self.feed_dir / dt.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{self.feed_name}_{dt.strftime('%Y%m%dT%H%M%SZ')}.pb"
        out_path = date_dir / filename
        out_path.write_bytes(raw)

        self._last_ts = feed_ts
        logger.info("[%s] guardado %s (%d bytes, %d entidades)",
                    self.feed_name, filename, len(raw), len(feed.entity))
        return out_path

    def close(self):
        self._session.close()
