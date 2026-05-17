import logging
import signal
import sys
import threading

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


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Arrancando pipeline de ingesta GTFS-RT de Renfe")

    cfg = load()
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
