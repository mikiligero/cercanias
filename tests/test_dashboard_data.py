from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from src.dashboard_data import FEEDS, build_dashboard_connection, latest_file


def epoch(day, hour=12):
    return int(datetime(2026, 10, day, hour, tzinfo=timezone.utc).timestamp())


def fixture_data(tmp_path):
    for feed in FEEDS:
        for day in (15, 27, 28):
            folder = tmp_path / feed / f'date=2026-10-{day}'
            folder.mkdir(parents=True)
            pd.DataFrame({'snapshot_ts': [epoch(day)], 'arrival_delay': [day]}).to_parquet(
                folder / f'part-202610{day}T120000Z.parquet', index=False)
    return {'storage': {'bronze_dir': str(tmp_path)}}


def test_live_uses_only_latest_even_with_corrupt_old_file(tmp_path):
    cfg = fixture_data(tmp_path)
    (tmp_path/'trip_updates/date=2026-10-15/part-20261015T110000Z.parquet').write_bytes(b'broken')
    con = build_dashboard_connection(cfg, None)
    try:
        for feed in FEEDS:
            assert con.execute(f'SELECT snapshot_ts FROM {feed}').fetchall() == [(epoch(28),)]
    finally:
        con.close()


def test_period_latest_respects_exact_bounds(tmp_path):
    cfg = fixture_data(tmp_path)
    con = build_dashboard_connection(cfg, None, epoch(27, 0), epoch(28, 0), period_latest=True)
    try:
        assert con.execute('SELECT snapshot_ts FROM trip_updates').fetchall() == [(epoch(27),)]
    finally:
        con.close()
    assert latest_file(tmp_path, 'trip_updates', epoch(26, 0), epoch(26, 23)) is None


def test_history_excludes_older_days_and_preserves_live_snapshot(tmp_path):
    cfg = fixture_data(tmp_path)
    con = build_dashboard_connection(cfg, None, epoch(27, 0), epoch(27, 23), history=True)
    try:
        assert con.execute('SELECT snapshot_ts FROM trip_updates ORDER BY snapshot_ts').fetchall() == [(epoch(27),), (epoch(28),)]
        assert con.execute(f'SELECT snapshot_ts FROM trip_updates WHERE snapshot_ts BETWEEN {epoch(27,0)} AND {epoch(27,23)}').fetchall() == [(epoch(27),)]
    finally:
        con.close()
