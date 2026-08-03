"""Best-of-N: generate N candidates, execute+repair, verify, select the best."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path

from ..chroma.chroma_store import ChromaStore
from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import SnowflakeExecutor
from .candidate_generator import (
    CandidateItem,
    _detect_unqualified_change_ambiguity,
    generate_candidate_sqls,
)
from .error_classifier import classify_snowflake_error
from .llm_client import call_llm
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
    if not row_count:
        # Empty results carry no real agreement signal: candidates sharing
        # an over-strict WHERE clause (a common shared misreading of the
        # question) all return zero rows and would otherwise cluster into a
        # trivial "consensus" that rewards the exact failure mode voting is
        # meant to catch.
        return None
    orig_col_names = cr.get("column_names") or []
    raw_cols = [(c or "").strip().lower() for c in orig_col_names]
    # Sort once and reuse the permutation for both the column-name tuple and
    # every row's values — sorting `cols` alone while pulling `vals` in the
    # original column order left two candidates with identical data but
    # differently-ordered SELECT lists producing mismatched signatures,
    # silently fragmenting their consensus vote across separate clusters.
    order = sorted(range(len(raw_cols)), key=lambda i: raw_cols[i])
    cols = tuple(raw_cols[i] for i in order)
    rows = cr.get("rows_sample") or []
    norm_rows: list[tuple] = []
    for row in rows[:max_rows]:
        if isinstance(row, dict):
            raw_vals = [row.get(c) for c in orig_col_names]
        else:
            raw_vals = list(row)
        vals = [raw_vals[i] for i in order] if len(raw_vals) == len(order) else raw_vals
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


def _mark_tiebreak_used(flipped: bool) -> None:
    """Best-effort telemetry mark; never raises into the selection path."""
    try:
        from ..observability.instance_telemetry import telemetry
        telemetry.mark("tiebreak_used")
        if flipped:
            telemetry.mark("tiebreak_flipped_winner")
    except Exception:
        pass


def _intrinsic_score(cr: dict) -> float:
    """Candidate score with the consensus/self-consistency bonus subtracted
    back out.

    ``consensus_bonus`` rewards agreement (up to (votes-1)*18 points — see
    DEFAULT_SCORING) on top of every other signal (execution success, shape,
    verifier). That's the right default — agreement is real evidence — but
    it means a majority cluster can out-score a minority by vote count alone
    even when the minority is just as strong on every *other* signal. This
    strips that one component back out so two clusters can be compared on
    everything except how many strategies happened to agree with them.
    """
    consensus_component = cr.get("score_breakdown", {}).get("consensus", 0.0)
    return cr["score"] - consensus_component


# A challenger's *intrinsic* score (score with the consensus bonus removed)
# must reach at least this fraction of the leader's intrinsic score to be
# worth an LLM tie-break call. Calibrated against a real, confirmed case
# (sf_bq059): a lone correct candidate (1 vote, intrinsic score 90.4) lost
# outright to a 6-vote wrong cluster (score 200.6, intrinsic 110.6) purely
# because the wrong cluster's vote-count bonus ((6-1)*18=90) dwarfed the
# correct candidate's total score — ratio 90.4/110.6 ≈ 0.82. Set below that
# observed value (not AT it, to avoid overfitting to one data point) so this
# threshold has real margin, while still being conservative enough not to
# fire on every plausible-but-clearly-weaker minority candidate.
TIEBREAK_INTRINSIC_RATIO = 0.75


def _find_tiebreak_pair(candidate_results: list[dict]) -> tuple[dict, dict] | None:
    """Return (leader, challenger) if a second, differently-answered cluster
    is intrinsically competitive with the plurality leader, else None.

    Plurality voting alone can be fooled two ways: (1) several strategies
    share the same mistake, forming a false majority, or (2) that false
    majority's vote-count bonus alone is enough to bury a single correct
    outlier that would otherwise score competitively. Both are covered by
    comparing INTRINSIC scores (see _intrinsic_score) rather than requiring
    a minimum vote count on the challenger — a challenger with only 1 vote
    can still qualify if it's intrinsically nearly as strong as the leader;
    a weak, genuinely-implausible outlier won't have a competitive intrinsic
    score regardless of vote count, so this doesn't fire on ordinary noise.
    """
    best_per_cluster: dict[tuple, dict] = {}
    for cr in candidate_results:
        sig = cr.get("_result_sig")
        if sig is None or not cr.get("execution_success"):
            continue
        if sig not in best_per_cluster or cr["score"] > best_per_cluster[sig]["score"]:
            best_per_cluster[sig] = cr

    clusters = sorted(best_per_cluster.values(), key=lambda c: c["score"], reverse=True)
    if len(clusters) < 2:
        return None
    leader, challenger = clusters[0], clusters[1]
    leader_intrinsic = _intrinsic_score(leader)
    challenger_intrinsic = _intrinsic_score(challenger)
    if leader_intrinsic <= 0:
        # Degenerate case (e.g. leader itself scored <= 0 before any
        # consensus bonus) — ratio comparison is meaningless; fall back to
        # the plain sign check so an intrinsically-negative leader can still
        # be challenged by any intrinsically-positive candidate.
        if challenger_intrinsic < leader_intrinsic:
            return None
        return leader, challenger
    if challenger_intrinsic < leader_intrinsic * TIEBREAK_INTRINSIC_RATIO:
        return None
    return leader, challenger


def _preview_candidate_result(cr: dict, max_rows: int = 5, max_cell: int = 80) -> str:
    """Compact text preview of a candidate's executed result for the LLM
    tie-break prompt (rows_sample/column_names survive the exec_result pop
    since they're stored as top-level keys on the candidate dict)."""
    cols = cr.get("column_names") or []
    rows = cr.get("rows_sample") or []
    lines = [f"row_count={cr.get('row_count')}, columns={cols}"]
    for row in rows[:max_rows]:
        if isinstance(row, dict):
            vals = [row.get(c) for c in cols] if cols else list(row.values())
        else:
            vals = list(row)
        cells = ["NULL" if v is None else str(v)[:max_cell] for v in vals]
        lines.append(" | ".join(cells))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more sampled rows)")
    return "\n".join(lines)


_CHANGE_AMBIGUITY_TIEBREAK_ADDENDUM = (
    "\nNAMED AMBIGUITY DETECTED — unqualified \"change\"/\"difference\": this "
    "question's wording doesn't specify whether a SIGNED value (can be "
    "negative — e.g. a decrease counts as a negative change) or an ABSOLUTE "
    "MAGNITUDE (via ABS(), always non-negative — a big decrease counts just "
    "as much as a big increase) is intended. Both are grammatically valid "
    "readings in isolation, so don't just default to whichever seems more "
    "\"literal\" — that's not a tiebreaker here. Instead weigh this specific "
    "signal: pairing a magnitude superlative (\"highest\"/\"largest\"/"
    "\"biggest\"/\"most\", or their \"lowest\"/\"smallest\" counterparts) with "
    "a direction-NEUTRAL noun like \"change\" (as opposed to a direction-"
    "committed noun like \"increase\"/\"growth\"/\"decrease\"/\"decline\") is "
    "a common data-reporting convention — think \"biggest movers\" style "
    "reporting, where a sharp drop is just as reportable an event as a sharp "
    "rise. If the question had meant a specifically positive/negative trend, "
    "it would more naturally have said \"increase\"/\"growth\" or "
    "\"decrease\"/\"decline\" instead of the neutral \"change\". This favors "
    "(but does not guarantee) the ABSOLUTE MAGNITUDE reading when superlative "
    "wording is present — weigh it as one input alongside the actual SQL "
    "logic and data, not as a rule that overrides everything else.\n"
)


def _llm_tiebreak(instruction: str, leader: dict, challenger: dict, model: str) -> dict:
    """Ask the LLM to adjudicate between two competing, independently-agreed
    candidate answers. Returns {"winner": "leader"|"challenger", "reason": str}.
    Fails safe to "leader" (keep the consensus-based winner) on any error —
    this is a refinement on top of consensus voting, not a replacement for it.

    When the disagreement plausibly stems from a *named* ambiguity type we
    already detect elsewhere in the pipeline (currently: unqualified "change"
    wording — see _detect_unqualified_change_ambiguity), the prompt is
    sharpened with reasoning specific to that ambiguity rather than a generic
    "which is more correct" question — a live test found the generic framing
    alone doesn't reliably out-guess an arbitrary annotator convention on a
    genuinely ambiguous question.
    """
    ambiguity_addendum = ""
    if _detect_unqualified_change_ambiguity(instruction):
        ambiguity_addendum = _CHANGE_AMBIGUITY_TIEBREAK_ADDENDUM

    system = (
        "You are adjudicating between two SQL query results that both claim "
        "to answer the same question — each independently agreed upon by "
        "multiple different query-generation strategies, so neither is a "
        "fluke. Decide which result more correctly and faithfully answers "
        "the question, based on its SQL logic and its actual returned data. "
        f"{ambiguity_addendum}"
        'Respond with STRICT JSON only: {"winner": "A"|"B", "reason": '
        '"<short concrete reason>"}.'
    )
    user = (
        f"Question:\n{instruction}\n\n"
        f"Result A ({leader.get('consensus_votes')} independent candidates agreed):\n"
        f"SQL:\n{leader.get('final_sql', '')}\n\n"
        f"Result A preview:\n{_preview_candidate_result(leader)}\n\n"
        f"Result B ({challenger.get('consensus_votes')} independent candidates agreed):\n"
        f"SQL:\n{challenger.get('final_sql', '')}\n\n"
        f"Result B preview:\n{_preview_candidate_result(challenger)}\n\n"
        "Which result (A or B) more correctly answers the question? Return strict JSON."
    )
    try:
        # 300 tokens was tried first and consistently produced unparseable
        # (truncated) JSON — some models (e.g. gpt-5.5) spend part of their
        # max_tokens budget on invisible reasoning tokens before any visible
        # output, so a budget sized only for the visible JSON reply is too
        # tight. 1500 gives real headroom; this call is infrequent (only
        # close ties reach it at all — see _find_tiebreak_pair) so the extra
        # cost ceiling per call is immaterial.
        raw = call_llm(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            model=model, temperature=0.0, max_tokens=1500,
        )
    except Exception:
        log.debug("LLM tie-break call failed; keeping consensus leader", exc_info=True)
        return {"winner": "leader", "reason": "tie-break call failed"}

    text = (raw or "").strip()
    if "```" in text:
        text = text.split("```")[1] if len(text.split("```")) > 1 else text
        text = text.removeprefix("json").strip()
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]
    try:
        data = json.loads(text)
        winner_letter = str(data.get("winner", "A")).strip().upper()
        winner = "challenger" if winner_letter == "B" else "leader"
        return {"winner": winner, "reason": str(data.get("reason", "") or "")}
    except (ValueError, TypeError):
        log.warning("LLM tie-break returned unparseable output: %r", raw)
        return {"winner": "leader", "reason": "tie-break output unparseable"}


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
    enable_tiebreak: bool = True,
    exploration_context: str | None = None,
    plan_context: str | None = None,
    date_encodings: dict | None = None,
    pregenerated_candidates: list[CandidateItem] | None = None,
) -> dict:
    """Generate N candidates, execute+repair, verify, select the best.

    *pregenerated_candidates*: skip Step 1 (candidate generation) and use
    this list directly — for the batch-generation path, where candidates for
    every instance in a run were already produced via one shared OpenAI
    Batch API job (see eval/experiment_runner.py's batched orchestration and
    candidate_generator.build_raw_sql_candidate_requests /
    assemble_candidates_from_batch_results). None (default): generate
    candidates synchronously here, exactly as before.
    """
    log.info(
        "Best-of-%d for instance %s: %s", n, instance_id, instruction[:80]
    )

    # ── Step 1: Generate N candidates ────────────────────────────────
    if pregenerated_candidates is not None:
        candidates = pregenerated_candidates
    else:
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
    tiebreak_note = None

    # Plurality voting can be fooled when several independent strategies
    # happen to share the same mistake. When a *different*, itself
    # independently-supported cluster is competing for the top spot, ask an
    # LLM to adjudicate directly between the two rather than automatically
    # deferring to whichever cluster is larger. This only fires when both
    # sides have real (>1-candidate) support — see _find_tiebreak_pair.
    if enable_consensus and enable_tiebreak:
        pair = _find_tiebreak_pair(candidate_results)
        if pair is not None:
            leader, challenger = pair
            verdict = _llm_tiebreak(instruction, leader, challenger, model)
            flipped = verdict["winner"] == "challenger"
            _mark_tiebreak_used(flipped)
            log.info(
                "Tie-break (leader=cand%s votes=%s vs challenger=cand%s votes=%s): "
                "winner=%s reason=%s",
                leader["candidate_id"], leader.get("consensus_votes"),
                challenger["candidate_id"], challenger.get("consensus_votes"),
                verdict["winner"], verdict["reason"][:160],
            )
            if flipped:
                best = challenger
                tiebreak_note = (
                    f"tie-break overrode consensus leader (cand{leader['candidate_id']}, "
                    f"{leader.get('consensus_votes')} votes) in favor of cand"
                    f"{challenger['candidate_id']} ({challenger.get('consensus_votes')} "
                    f"votes): {verdict['reason']}"
                )

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
    if tiebreak_note:
        reason_parts.append(tiebreak_note)

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
