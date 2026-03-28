"""Write Spider2-compatible result.json files."""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_BASE_DIR = "Spider2/methods/spider-agent-snow/output"


def write_spider2_result(
    experiment: str,
    instance_id: str,
    sql: str,
    success: bool,
    base_dir: str | Path = DEFAULT_BASE_DIR,
) -> Path:
    """Write result.json for one instance in Spider2-compatible layout.

    Path: <base_dir>/<experiment>/<instance_id>/spider/result.json
    """
    out_dir = Path(base_dir) / experiment / instance_id / "spider"
    out_dir.mkdir(parents=True, exist_ok=True)

    result = {"sql": sql, "success": success}
    result_path = out_dir / "result.json"
    result_path.write_text(json.dumps(result, indent=2) + "\n")

    log.info("Wrote result: %s", result_path)
    return result_path
