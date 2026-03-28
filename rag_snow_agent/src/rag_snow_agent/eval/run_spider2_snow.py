"""Standardized Spider2-Snow runner entry point.

Thin wrapper that loads instances from JSONL, calls solve_instance for each,
and writes Spider2-compatible result.json files.

CLI usage::

    uv run python -m rag_snow_agent.eval.run_spider2_snow \
      --split_jsonl Spider2/spider2-snow/spider2-snow.jsonl \
      --experiment rag_v1 \
      --limit 25
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .write_results import write_spider2_result

log = logging.getLogger(__name__)


def load_instances(split_jsonl: Path, limit: int | None = None) -> list[dict]:
    """Load instances from a JSONL file."""
    if not split_jsonl.exists():
        print(f"ERROR: split_jsonl not found: {split_jsonl}", file=sys.stderr)
        sys.exit(1)

    instances = []
    with open(split_jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            instances.append(json.loads(line))
            if limit and len(instances) >= limit:
                break
    return instances


def run_instances(
    instances: list[dict],
    experiment: str,
    model: str = "gpt-4o-mini",
    credentials: str = "rag_snow_agent/snowflake_credentials.json",
    best_of_n: int = 1,
    max_repairs: int = 2,
    memory_enabled: bool = True,
    base_dir: str = "Spider2/methods/spider-agent-snow/output",
) -> list[dict]:
    """Run solve_instance for each instance and write results.

    Returns list of per-instance summary dicts.
    """
    results = []
    for i, instance in enumerate(instances, 1):
        instance_id = instance.get("instance_id", f"unknown_{i}")
        instruction = instance.get("instruction", "")
        db_id = instance.get("db_id", "")

        log.info("[%d/%d] Processing %s", i, len(instances), instance_id)

        try:
            from ..agent.agent import solve_instance
            from ..retrieval.schema_slice import SchemaSlice
            from ..snowflake.executor import SnowflakeExecutor

            executor = SnowflakeExecutor(credentials_path=credentials)
            schema_slice = SchemaSlice(db_id=db_id)

            result = solve_instance(
                instance_id=instance_id,
                instruction=instruction,
                db_id=db_id,
                schema_slice=schema_slice,
                model=model,
                executor=executor,
                best_of_n=best_of_n,
                max_repairs=max_repairs,
                memory_enabled=memory_enabled,
            )

            write_spider2_result(
                experiment=experiment,
                instance_id=instance_id,
                sql=result.final_sql,
                success=result.success,
                base_dir=base_dir,
            )

            summary = {
                "instance_id": instance_id,
                "success": result.success,
                "llm_calls": result.llm_calls,
                "repair_count": len(result.repair_trace),
            }

        except Exception as exc:
            log.error("Instance %s failed: %s", instance_id, exc)
            write_spider2_result(
                experiment=experiment,
                instance_id=instance_id,
                sql="SELECT 1 /* error */;",
                success=False,
                base_dir=base_dir,
            )
            summary = {
                "instance_id": instance_id,
                "success": False,
                "llm_calls": 0,
                "repair_count": 0,
                "error": str(exc),
            }

        results.append(summary)

    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Run Spider2-Snow instances")
    parser.add_argument("--split_jsonl", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--credentials", default="rag_snow_agent/snowflake_credentials.json")
    parser.add_argument("--best_of_n", type=int, default=1)
    parser.add_argument("--max_repairs", type=int, default=2)
    args = parser.parse_args()

    instances = load_instances(Path(args.split_jsonl), args.limit)
    results = run_instances(
        instances=instances,
        experiment=args.experiment,
        model=args.model,
        credentials=args.credentials,
        best_of_n=args.best_of_n,
        max_repairs=args.max_repairs,
    )

    total = len(results)
    successes = sum(1 for r in results if r["success"])
    print(f"\nDone: {successes}/{total} succeeded ({100*successes/total:.1f}%)" if total else "No instances")


if __name__ == "__main__":
    main()
