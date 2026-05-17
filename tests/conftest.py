"""Fixtures compartidos para toda la suite de tests."""
import pytest
from pathlib import Path
from src.config import load


@pytest.fixture(scope="session")
def cfg():
    return load()


@pytest.fixture(scope="session")
def vp_pb():
    files = sorted(Path("data/raw/vehicle_positions").rglob("*.pb"))
    if not files:
        pytest.skip("Sin ficheros vehicle_positions .pb — ejecuta main.py primero")
    return files[0]


@pytest.fixture(scope="session")
def tu_pb():
    files = sorted(Path("data/raw/trip_updates").rglob("*.pb"))
    if not files:
        pytest.skip("Sin ficheros trip_updates .pb — ejecuta main.py primero")
    return files[0]


@pytest.fixture(scope="session")
def al_pb():
    files = sorted(Path("data/raw/alerts").rglob("*.pb"))
    if not files:
        pytest.skip("Sin ficheros alerts .pb — ejecuta main.py primero")
    return files[0]
