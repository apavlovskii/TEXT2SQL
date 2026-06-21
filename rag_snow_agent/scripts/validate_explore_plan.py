"""Validation gate for the explore->plan bundle (run BEFORE any full A/B).

Asserts, on one instance, that:
  1. explore() runs probes and returns non-empty evidence.
  2. The question entity is resolved to a real DB value (sf_bq010 -> 'Henley').
  3. build_information_plan() returns a structured plan.
  4. The candidate prompt actually CONTAINS plan + evidence + dialect(ILIKE) + determinism(NULLS LAST).
Exits non-zero if any check fails. Cheap: a few LLM calls on ONE instance.
"""
import json
import sys

from rag_snow_agent.agent.exploration import build_information_plan, explore
from rag_snow_agent.chroma.chroma_store import ChromaStore
from rag_snow_agent.prompting.prompt_builder import build_plan_prompt_with_strategy
from rag_snow_agent.retrieval.debug_retrieve import build_schema_slice
from rag_snow_agent.retrieval.hybrid_retriever import HybridRetriever
from rag_snow_agent.snowflake.executor import SnowflakeExecutor

MODEL = "gpt-5.4"
INST = "sf_bq010"
ENTITY = "henley"

def main():
    inst = next(json.loads(l) for l in open("../Spider2/spider2-snow/spider2-snow.jsonl")
                if json.loads(l)["instance_id"] == INST)
    q, db = inst["instruction"], inst["db_id"]
    print(f"Instance {INST} [{db}]: {q[:110]}")

    store = ChromaStore(persist_dir=".chroma")
    retr = HybridRetriever(store.schema_collection())
    schema_slice, _, _ = build_schema_slice(retriever=retr, query=q, db_id=db,
                                            top_k_tables=8, top_k_columns=25, max_schema_tokens=2500)
    ex = SnowflakeExecutor(credentials_path="snowflake_credentials.json", db_id=db, sample_rows=20)
    er = explore(q, db, schema_slice, ex, MODEL, max_probes=6)
    ex.close()

    plan = build_information_plan(q, schema_slice, er.evidence_text, evidence=None, model=MODEL)

    msgs = build_plan_prompt_with_strategy(
        q, schema_slice, "default",
        exploration_context=er.evidence_text or None, plan_context=plan or None,
    )
    prompt = msgs[0]["content"] + "\n" + msgs[1]["content"]

    results = []
    def check(name, cond, detail=""):
        results.append(cond)
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")

    print("\n--- probes ---")
    for p in er.probes:
        print(f"  ok={p.get('ok')} {p.get('note','')[:50]} | {p['sql'][:80].replace(chr(10),' ')}")
    print(f"\nevidence chars={len(er.evidence_text)} | plan chars={len(plan)}")

    print("\n--- checks ---")
    check("1. >=1 successful probe", er.n_ok >= 1, f"(n_ok={er.n_ok})")
    check("2. non-empty exploration evidence", bool(er.evidence_text))
    check(f"3. entity '{ENTITY}' resolved in evidence", ENTITY in er.evidence_text.lower())
    check("4. non-empty structured plan", bool(plan))
    check("5. prompt contains plan text", bool(plan) and plan[:40] in prompt)
    check("6. prompt contains exploration evidence", bool(er.evidence_text) and er.evidence_text[:40] in prompt)
    check("7. prompt contains dialect discipline (ILIKE)", "ILIKE" in prompt)
    check("8. prompt contains determinism (NULLS LAST)", "NULLS LAST" in prompt)

    ok = all(results)
    print(f"\n{'='*50}\nVALIDATION GATE: {'PASS' if ok else 'FAIL'}\n{'='*50}")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
