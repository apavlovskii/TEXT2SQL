"""Backfill trace memory from prior run outputs.

TODO: Implement backfilling from completed experiment folders.

Usage (planned):
    uv run python -m rag_snow_agent.eval.backfill_trace_memory \
        --run_dir output/experiment_v1 \
        --db_id GA360

This utility would:
1. Scan a completed experiment output directory for successful instances.
2. Reconstruct TraceRecord objects from logged results.
3. Upsert them into the TraceMemoryStore.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ..chroma.trace_memory import TraceMemoryStore

log = logging.getLogger(__name__)


def backfill_from_run_dir(run_dir: Path, chroma_dir: str | None = None) -> int:
    """Backfill trace memory from a prior run directory.

    TODO: Parse run output files and reconstruct trace records.

    Returns the number of traces upserted.
    """
    store = TraceMemoryStore(persist_dir=chroma_dir)  # noqa: F841
    count = 0
    # TODO: iterate over instance results in run_dir
    # TODO: for each successful instance, build a TraceRecord and upsert
    log.warning("backfill_from_run_dir is not yet implemented")
    return count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill trace memory from run outputs")
    parser.add_argument("--run_dir", required=True, help="Path to completed experiment output")
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        log.error("Run directory not found: %s", run_dir)
        return

    count = backfill_from_run_dir(run_dir, chroma_dir=args.chroma_dir)
    print(f"Backfilled {count} traces from {run_dir}")


if __name__ == "__main__":
    main()
