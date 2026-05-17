import logging
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.downloader import FeedDownloader
from src.parser import process

logger = logging.getLogger(__name__)


class IngestionScheduler:
    """
    Orquesta la descarga y parseo periódico de los feeds GTFS-RT.

    Cada feed tiene su propio FeedDownloader (estado en memoria) y su propio
    job en APScheduler. max_instances=1 garantiza que si una descarga tarda
    más de 30s no se solapa con la siguiente.
    """

    def __init__(self, cfg: dict):
        self._bronze_dir = Path(cfg["storage"]["bronze_dir"])
        self._downloaders: dict[str, FeedDownloader] = {}
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._stats: dict[str, int] = {}
        self._setup(cfg)

    def _setup(self, cfg: dict):
        raw_dir  = Path(cfg["storage"]["raw_dir"])
        http_cfg = cfg["http"]

        for feed_name, feed_cfg in cfg["feeds"].items():
            dl = FeedDownloader(feed_name, feed_cfg["url"], raw_dir, http_cfg)
            self._downloaders[feed_name] = dl
            self._stats[feed_name] = 0

            interval = feed_cfg.get("interval_seconds", 30)
            self._scheduler.add_job(
                func=self._run_feed,
                trigger=IntervalTrigger(seconds=interval),
                args=[feed_name],
                id=feed_name,
                name=f"ingest_{feed_name}",
                max_instances=1,
                misfire_grace_time=15,
                # Primera ejecución inmediata al arrancar
                next_run_time=datetime.now(timezone.utc),
            )
            logger.info("Job registrado: %s cada %ds", feed_name, interval)

    def _run_feed(self, feed_name: str):
        """Descarga un feed y, si hay snapshot nuevo, lo parsea a Parquet."""
        pb_path = self._downloaders[feed_name].fetch()
        if pb_path is None:
            return
        parquet_path = process(pb_path, feed_name, self._bronze_dir)
        if parquet_path:
            self._stats[feed_name] += 1

    def start(self):
        self._scheduler.start()
        feed_list = ", ".join(self._downloaders)
        logger.info("Scheduler iniciado — feeds: [%s]", feed_list)

    def shutdown(self):
        logger.info("Deteniendo scheduler...")
        self._scheduler.shutdown(wait=True)
        for dl in self._downloaders.values():
            dl.close()
        logger.info("Scheduler detenido — snapshots guardados: %s", self._stats)

    @property
    def is_running(self) -> bool:
        return self._scheduler.running
