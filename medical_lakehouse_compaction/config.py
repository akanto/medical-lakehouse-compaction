import yaml
from pathlib import Path


def load_profile(path: str) -> dict:
    with open(Path(path)) as f:
        return yaml.safe_load(f)
