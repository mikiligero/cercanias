"""Mensajes sintéticos reproducibles: las pruebas no necesitan el histórico."""
import pytest
from pathlib import Path
from src.config import load


@pytest.fixture(scope="session")
def cfg():
    return load()


def _message(tmp_path_factory, feed):
    from google.transit import gtfs_realtime_pb2
    from datetime import datetime, timezone
    message=gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version='2.0'
    stamp=int(datetime(2026,5,17,10,4,46,tzinfo=timezone.utc).timestamp())
    message.header.timestamp=stamp
    for i in range(2):
        e=message.entity.add();e.id='entity-'+str(i)
        if feed=='vehicle_positions':
            e.vehicle.trip.trip_id='10D000'+str(i)+'C3'
            e.vehicle.position.latitude=40.4+i/100
            e.vehicle.position.longitude=-3.7
            e.vehicle.vehicle.id='v'+str(i)
            e.vehicle.current_status=2
            e.vehicle.timestamp=stamp
        elif feed=='trip_updates':
            e.trip_update.trip.trip_id='10D000'+str(i)+'C3'
            stop=e.trip_update.stop_time_update.add()
            stop.stop_id='s'+str(i);stop.stop_sequence=i+1
            stop.arrival.delay=180*i;stop.arrival.time=stamp+600
        else:
            e.alert.active_period.add().start=stamp
            e.alert.informed_entity.add().route_id='10T0001C3'
            text=e.alert.description_text.translation.add();text.language='es';text.text='Aviso sintético para pruebas'
    path=tmp_path_factory.mktemp(feed)/(feed+'_20260517T100446Z.pb')
    path.write_bytes(message.SerializeToString())
    return path


@pytest.fixture(scope='session')
def vp_pb(tmp_path_factory):
    return _message(tmp_path_factory,'vehicle_positions')


@pytest.fixture(scope="session")
def tu_pb(tmp_path_factory):
    return _message(tmp_path_factory,'trip_updates')


@pytest.fixture(scope="session")
def al_pb(tmp_path_factory):
    return _message(tmp_path_factory,'alerts')
