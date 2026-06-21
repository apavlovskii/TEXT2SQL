"""Batch-index all DBs used by the representative subset into ChromaDB.

Cheap (Snowflake schema extract + embeddings; no gpt-5.4). Resumable: skips DBs
already present in the index. Tolerant: a failure on one DB doesn't stop the rest.

  uv run python -m scripts.index_representative_dbs
"""
import json
import logging

from rag_snow_agent.chroma.build_index import run as build_index_run
from rag_snow_agent.chroma.chroma_store import ChromaStore

logging.basicConfig(level=logging.WARNING)
SPLIT = "../Spider2/spider2-snow/spider2-snow_representative.jsonl"
CREDS = "snowflake_credentials.json"
CHROMA = ".chroma"


def indexed_dbs() -> set:
    col = ChromaStore(persist_dir=CHROMA).schema_collection()
    total = col.count(); off = 0; dbs = set()
    while off < total:
        res = col.get(limit=20000, offset=off, include=["metadatas"])
        ms = res.get("metadatas") or []
        if not ms:
            break
        for m in ms:
            if m.get("object_type") == "table":
                dbs.add(m.get("db_id"))
        off += len(ms)
    return dbs


def main():
    need = []
    for l in open(SPLIT):
        d = json.loads(l)["db_id"]
        if d not in need:
            need.append(d)
    have = indexed_dbs()
    todo = [d for d in need if d not in have]
    print(f"representative DBs: {len(need)} | already indexed: {len(need)-len(todo)} | to index: {len(todo)}")
    ok, fail = [], []
    for i, db in enumerate(todo, 1):
        print(f"\n[{i}/{len(todo)}] indexing {db} ...", flush=True)
        try:
            counts = build_index_run(db, CREDS, chroma_dir=CHROMA)
            print(f"   OK {db}: {counts}", flush=True)
            ok.append(db)
        except Exception as e:
            print(f"   FAIL {db}: {str(e)[:140]}", flush=True)
            fail.append(db)
    print(f"\n=== DONE: indexed {len(ok)}, failed {len(fail)} ===")
    if fail:
        print("failed:", fail)


if __name__ == "__main__":
    main()
