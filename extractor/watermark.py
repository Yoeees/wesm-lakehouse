import json
import os
from pathlib import Path


def load_watermark(path: str):
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data.get("last_date")


def save_watermark(path: str, date_str: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"last_date": date_str}), encoding="utf-8")
    os.replace(tmp, p)
