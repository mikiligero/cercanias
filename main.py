import logging
import signal
import sys
import threading
from pathlib import Path

import requests

from src.config import load
from src.scheduler import IngestionScheduler


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,
    )
    # APScheduler es muy verboso a nivel INFO — solo queremos sus warnings
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


def ensure_gtfs_static(cfg: dict) -> None:
    """Descarga el ZIP de GTFS estático si no existe o tiene más de 7 días."""
    logger = logging.getLogger(__name__)
    url      = cfg.get("gtfs_static", {}).get("url")
    dest     = Path(cfg["storage"]["raw_dir"]) / "gtfs_static" / "fomento_transit.zip"
    max_age  = 7 * 24 * 3600  # renovar semanalmente

    if not url:
        logger.warning("gtfs_static.url no definida en config.yaml — saltando descarga")
        return

    if dest.exists():
        age = dest.stat().st_mtime
        import time
        if time.time() - age < max_age:
            logger.info("GTFS estático OK (%.0f h) — %s",
                        (time.time() - age) / 3600, dest)
            return
        logger.info("GTFS estático tiene más de 7 días, renovando...")

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Descargando GTFS estático desde %s ...", url)
    try:
        resp = requests.get(
            url,
            timeout=cfg.get("http", {}).get("timeout_seconds", 30),
            headers={"User-Agent": cfg.get("http", {}).get("user_agent", "cercanias/1.0")},
            stream=True,
        )
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                f.write(chunk)
        size_mb = dest.stat().st_size / 1024 / 1024
        logger.info("GTFS estático guardado en %s (%.1f MB)", dest, size_mb)
    except Exception as exc:
        logger.error("Error descargando GTFS estático: %s", exc)
        if dest.exists():
            dest.unlink()


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Arrancando pipeline de ingesta GTFS-RT de Renfe")

    cfg = load()
    ensure_gtfs_static(cfg)
    scheduler = IngestionScheduler(cfg)

    stop_event = threading.Event()

    def handle_signal(signum, _frame):
        logger.info("Señal %d recibida, iniciando cierre ordenado...", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    scheduler.start()

    # Bloquea el hilo principal hasta que llegue SIGTERM o Ctrl+C
    stop_event.wait()

    scheduler.shutdown()
    logger.info("Pipeline detenido")


if __name__ == "__main__":
    main()
