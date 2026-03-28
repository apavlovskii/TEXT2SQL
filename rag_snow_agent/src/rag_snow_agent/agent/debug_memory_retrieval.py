"""Debug CLI to query trace memory and print results.

Usage:
    uv run python -m rag_snow_agent.agent.debug_memory_retrieval \
        --db_id GA360 --query "total sessions by month" --top_k 5 -v
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml

from ..chroma.trace_memory import TraceMemoryStore

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "defaults.yaml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return yaml.safe_load(_CONFIG_PATH.read_text()) or {}
    return {}


def main(argv: list[str] | None = None) -> None:
    cfg = _load_config()
    mem_cfg = cfg.get("memory", {})

    parser = argparse.ArgumentParser(description="Debug trace memory retrieval")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--top_k", type=int, default=mem_cfg.get("top_k", 3))
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = TraceMemoryStore(persist_dir=args.chroma_dir)

    print(f"\n{'='*60}")
    print(f"Query:  {args.query}")
    print(f"DB:     {args.db_id}")
    print(f"Top-K:  {args.top_k}")
    print(f"{'='*60}")

    traces = store.query_traces(
        db_id=args.db_id,
        instruction=args.query,
        top_k=args.top_k,
    )

    if not traces:
        print("\nNo traces found.")
    else:
        print(f"\n--- {len(traces)} trace(s) found ---")
        for t in traces:
            print(f"\n  trace_id:  {t['trace_id']}")
            print(f"  distance:  {t['distance']:.4f}")
            meta = t.get("metadata", {})
            print(f"  db_id:     {meta.get('db_id', '')}")
            print(f"  instance:  {meta.get('instance_id', '')}")
            print(f"  tables:    {meta.get('tables_used', '')}")
            doc = t.get("document", "")
            if doc:
                print(f"  document:  {doc[:200]}")

    print()


if __name__ == "__main__":
    main()
