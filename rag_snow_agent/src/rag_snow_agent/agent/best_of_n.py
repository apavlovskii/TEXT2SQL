"""Best-of-N: generate N candidates, execute+repair, verify, select the best."""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

from ..chroma.chroma_store import ChromaStore
from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import SnowflakeExecutor
from .candidate_generator import CandidateItem, generate_candidate_sqls
from .error_classifier import classify_snowflake_error
from .metamorphic import run_metamorphic_checks
from .refiner import refine_sql
from .result_fingerprint import build_result_fingerprint
from .selector import explain_candidate_score, score_candidate
from .shape_inference import ExpectedShape, infer_expected_shape
from .verifier import score_candidate_semantics

log = logging.getLogger(__name__)


def _mark_verification_used() -> None:
    """Best-effort telemetry mark when fingerprint/metamorphic verification runs."""
    try:
        from ..observability.instance_telemetry import telemetry
        telemetry.mark("verification_used")
    except Exception:
        pass


def _normalize_cell(v) -> object:
    """Normalize one result cell for order-insensitive equality across candidates."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        # Round floats so trivially-different representations cluster together.
        return round(float(v), 4)
    s = str(v).strip()
    try:
        return round(float(s), 4)
    except (ValueError, TypeError):
        return s.lower()


def _result_signature(cr: dict, max_rows: int = 200) -> tuple | None:
    """Build an order-insensitive signature of a candidate's executed result.

    Returns None for candidates that did not execute (cannot vote). Two
    candidates with the same signature returned the same answer set. Note this
    relies on ``rows_sample`` (a capped sample); for large unordered results the
    sample may differ between equivalent queries, so voting is most reliable on
    the small/aggregate results that dominate this benchmark.
    """
    if not cr.get("execution_success"):
        return None
    row_count = cr.get("row_count")
    if row_count is None:
        return None
    cols = tuple(sorted((c or "").strip().lower() for c in (cr.get("column_names") or [])))
    rows = cr.get("rows_sample") or []
    norm_rows: list[tuple] = []
    for row in rows[:max_rows]:
        if isinstance(row, dict):
            vals = [row.get(c) for c in (cr.get("column_names") or [])]
        else:
            vals = list(row)
        norm_rows.append(tuple(_normalize_cell(v) for v in vals))
    norm_rows.sort(key=lambda t: tuple(str(x) for x in t))
    return (row_count, cols, tuple(norm_rows))


def _assign_consensus_votes(candidate_results: list[dict]) -> None:
    """Cluster candidates by result signature and tag each with its independent
    vote count (number of distinct strategies that converged on the same result).

    Mutates each candidate dict in place, setting ``consensus_votes``.
    """
    clusters: dict[tuple, set] = {}
    for cr in candidate_results:
        sig = _result_signature(cr)
        cr["_result_sig"] = sig
        if sig is None:
            cr["consensus_votes"] = 0
            continue
        # Count distinct strategies so re-emitting the same strategy doesn't
        # inflate the vote — agreement must come from independent derivations.
        clusters.setdefault(sig, set()).add(cr.get("strategy"))
    for cr in candidate_results:
        sig = cr.get("_result_sig")
        if sig is not None:
            cr["consensus_votes"] = len(clusters.get(sig, ()))
        cr.pop("_result_sig", None)


def _candidate_to_result(
    candidate: CandidateItem,
    final_sql: str,
    trace: list,
    exec_result,
) -> dict:
    """Build a structured result dict for one candidate (before scoring).

    ``execution_success`` is True if the SQL executed on Snowflake and returned
    rows, **even if gold-match verification later failed**.  The refiner marks
    ``exec_result.success = False`` when gold doesn't match, but the SQL did
    execute — we detect this via ``row_count > 0`` combined with an error
    message mentioning "gold" or "results don't match".
    """
    row_count = exec_result.row_count if exec_result else None
    rows_sample = exec_result.rows_sample if exec_result else None
    column_names = exec_result.column_names if exec_result else None

    # Determine if SQL actually executed on Snowflake (even if gold failed)
    if exec_result and exec_result.success:
        execution_success = True
    elif exec_result and row_count is not None and row_count > 0:
        # Gold-match failure: SQL executed and returned rows, but results
        # didn't match gold.  Treat as execution success for scoring purposes.
        execution_success = True
    else:
        execution_success = False

    error_type = None
    if exec_result and not exec_result.success and exec_result.error_message:
        error_type = classify_snowflake_error(exec_result.error_message)

    return {
        "candidate_id": candidate.candidate_id,
        "strategy": candidate.strategy,
        "initial_sql": candidate.sql,
        "final_sql": final_sql,
        "success": exec_result.success if exec_result else False,
        "execution_success": execution_success,
        "repairs_count": len(trace),
        "error_type": error_type,
        "row_count": row_count,
        "rows_sample": rows_sample,
        "column_names": column_names,
        "repair_trace": [
            {
                "attempt": t.attempt,
                "error_type": t.error_type,
                "repair_action": t.repair_action,
                "error_message": t.error_message[:200] if t.error_message else None,
            }
            for t in trace
        ],
        "exec_result": exec_result,
        "score": 0.0,
    }


def run_best_of_n(
    instance_id: str,
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    model: str,
    executor: SnowflakeExecutor,
    n: int = 2,
    temperature: float = 0.2,
    max_tokens: int = 800,
    max_repairs: int = 2,
    explain_first: bool = True,
    stop_on_repeated_error: bool = True,
    strategies: list[str] | None = None,
    scoring: dict | None = None,
    enable_verifier: bool = True,
    enable_fingerprinting: bool = True,
    enable_metamorphic: bool = True,
    max_metamorphic_checks: int = 2,
    chroma_store: ChromaStore | None = None,
    gold_dir: str | Path | None = None,
    eval_standards: dict | None = None,
    max_same_error_type: int = 3,
    semantic_context: str | None = None,
    decompose: bool = False,
    sample_context: str | None = None,
    enable_self_critic: bool = False,
    self_critic_max: int = 1,
    enable_consensus: bool = True,
    exploration_context: str | None = None,
    plan_context: str | None = None,
    date_encodings: dict | None = None,
) -> dict:
    """Generate N candidates, execute+repair, verify, select the best."""
    log.info(
        "Best-of-%d for instance %s: %s", n, instance_id, instruction[:80]
    )

    # ── Step 1: Generate N candidates ────────────────────────────────
    candidates = generate_candidate_sqls(
        db_id=db_id,
        instruction=instruction,
        schema_slice=schema_slice,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        n=n,
        strategies=strategies,
        semantic_context=semantic_context,
        decompose=decompose,
        sample_context=sample_context,
        exploration_context=exploration_context,
        plan_context=plan_context,
    )

    # Infer expected shape once for the instruction
    expected_shape = infer_expected_shape(instruction)

    # ── Step 2: Execute + repair each candidate ──────────────────────
    candidate_results: list[dict] = []
    for candidate in candidates:
        log.info(
            "Executing candidate %d (strategy=%s)",
            candidate.candidate_id,
            candidate.strategy,
        )

        if candidate.sql.startswith("SELECT 1"):
            cr: dict = {
                "candidate_id": candidate.candidate_id,
                "strategy": candidate.strategy,
                "initial_sql": candidate.sql,
                "final_sql": candidate.sql,
                "success": False,
                "execution_success": False,
                "repairs_count": 0,
                "error_type": None,
                "row_count": None,
                "rows_sample": None,
                "column_names": None,
                "repair_trace": [],
                "exec_result": None,
                "score": 0.0,
            }
            candidate_results.append(cr)
            continue

        final_sql, trace, exec_result = refine_sql(
            db_id=db_id,
            instruction=instruction,
            schema_slice=schema_slice,
            sql=candidate.sql,
            executor=executor,
            model=model,
            temperature=0.0,
            max_tokens=max_tokens,
            max_repairs=max_repairs,
            explain_first=explain_first,
            stop_on_repeated_error=stop_on_repeated_error,
            chroma_store=chroma_store,
            gold_dir=gold_dir,
            eval_standards=eval_standards,
            instance_id=instance_id,
            max_same_error_type=max_same_error_type,
            sample_context=sample_context,
            enable_self_critic=enable_self_critic,
            self_critic_max=self_critic_max,
            date_encodings=date_encodings,
        )

        cr = _candidate_to_result(candidate, final_sql, trace, exec_result)
        candidate_results.append(cr)

    # ── Step 2b: Self-consistency voting across candidates ───────────
    if enable_consensus:
        _assign_consensus_votes(candidate_results)

    # ── Step 3: Verification pass ────────────────────────────────────
    for cr in candidate_results:
        # Expected shape (same for all candidates)
        cr["expected_shape"] = asdict(expected_shape)

        # Result fingerprint
        if enable_fingerprinting and cr.get("exec_result"):
            fp = build_result_fingerprint(cr["exec_result"])
            cr["result_fingerprint"] = {
                "row_count": fp.row_count,
                "column_count": fp.column_count,
                "column_names": fp.column_names,
                "null_ratios": fp.null_ratios,
                "numeric_stats": fp.numeric_stats,
            }
            _mark_verification_used()
        else:
            cr["result_fingerprint"] = None

        # Metamorphic checks (only on successful candidates)
        if enable_metamorphic and cr.get("execution_success"):
            meta = run_metamorphic_checks(
                instruction=instruction,
                sql=cr["final_sql"],
                executor=executor,
                expected_shape=expected_shape,
                row_count=cr.get("row_count"),
                max_checks=max_metamorphic_checks,
            )
            cr["metamorphic"] = meta
            _mark_verification_used()
        else:
            cr["metamorphic"] = {"checks_run": [], "score_delta": 0.0}

        # Verifier score — semantic plausibility (learned model or heuristic).
        # Pass the full candidate record so the verifier can extract features;
        # gated by enable_verifier so the ablation toggle actually takes effect.
        if enable_verifier:
            cr["verifier_score"] = score_candidate_semantics(
                instruction=instruction,
                sql=cr.get("final_sql", ""),
                schema_slice=schema_slice,
                candidate_record=cr,
            )
        else:
            cr["verifier_score"] = 0.0

        # Remove exec_result before scoring (not serializable)
        cr.pop("exec_result", None)

        # Score with all signals
        cr["score"] = score_candidate(instruction, cr, scoring)
        cr["score_breakdown"] = explain_candidate_score(instruction, cr, scoring)

    # ── Step 4: Select best ──────────────────────────────────────────
    candidate_results.sort(key=lambda c: c["score"], reverse=True)
    best = candidate_results[0]

    # Build selection reason with semantic details
    reason_parts = [
        f"Candidate {best['candidate_id']} (strategy={best['strategy']}) "
        f"scored {best['score']:.1f}"
    ]
    if best.get("execution_success"):
        reason_parts.append(
            f"executed successfully with {best['repairs_count']} repair(s)"
        )
    else:
        reason_parts.append("best score among failed candidates")

    # Mention shape signals
    shape_notes = []
    bd = best.get("score_breakdown", {})
    if "time_series_bonus" in bd:
        shape_notes.append("time-series plausible")
    if "aggregate_single_row_bonus" in bd:
        shape_notes.append("aggregate shape confirmed")
    if "small_output_bonus" in bd:
        shape_notes.append("small result confirmed")
    if "metamorphic_delta" in bd:
        shape_notes.append(f"metamorphic delta={bd['metamorphic_delta']:+.1f}")
    if shape_notes:
        reason_parts.append("shape: " + ", ".join(shape_notes))
    if best.get("consensus_votes", 0) > 1:
        reason_parts.append(
            f"consensus: {best['consensus_votes']} independent candidates agreed"
        )

    selection_reason = "; ".join(reason_parts)
    log.info("Selected: %s", selection_reason)

    return {
        "best_candidate_id": best["candidate_id"],
        "best_sql": best["final_sql"],
        "best_success": best.get("execution_success", False),
        "selection_reason": selection_reason,
        "expected_shape": asdict(expected_shape),
        "candidates": candidate_results,
    }
