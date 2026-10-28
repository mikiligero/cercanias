"""Estado de descarga independiente de la fecha de generación del proveedor."""
import json
import os
import tempfile
from pathlib import Path


def status_path(raw_dir, feed):
    return Path(raw_dir).parent / 'status' / (feed + '.json')


def read_status(raw_dir, feed):
    try:
        return json.loads(status_path(raw_dir, feed).read_text())
    except (OSError, ValueError):
        return {}


def write_status(raw_dir, feed, state):
    path = status_path(raw_dir, feed)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=feed + '-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
