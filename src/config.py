from pathlib import Path
import yaml

_ROOT = Path(__file__).parent.parent


def load(path: Path | None = None) -> dict:
    cfg_path = path or _ROOT / "config.yaml"
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)

    # Convertir rutas relativas a absolutas respecto a la raíz del proyecto
    storage = cfg.setdefault("storage", {})
    for key in ("raw_dir", "bronze_dir"):
        if key in storage:
            storage[key] = str(_ROOT / storage[key])

    return cfg
