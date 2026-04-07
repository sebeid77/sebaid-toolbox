from pathlib import Path
from typing import Any, Dict, List

import yaml

# Inside the container, config is mounted at /app/config
CONFIG_PATH = Path("/app/config/databases.yml")


def load_databases() -> List[Dict[str, Any]]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("databases", [])
