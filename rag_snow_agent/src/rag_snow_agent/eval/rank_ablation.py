"""Rank ablation components into impact tiers via paired per-instance flips.

The goal is *ranking*, not significance testing: bucket each component into
High / Moderate / Negligible (or "Hurts") impact tiers from a leave-one-out
ablation suite. This is far more robust at small N than differencing two noisy
accuracy numbers, because it measures the *causal flips* a component produces
on the same instances (paired design).

For each leave-one-out arm we compare against the full system on the shared
instance set:

    rescued  = # instances the FULL system solves but the ablated arm fails
               (the component's positive contribution)
    broken   = # instances the ablated arm solves but the FULL system fails
               (the component's collateral damage)
    net      = rescued - broken          (signed impact, in instances)
    net_frac = net / n_paired            (impact as a fraction of instances)

A percentile bootstrap over instances gives a CI on net_frac; tiers are then
assigned from the magnitude and whether the CI excludes zero.

CLI::

    uv run python -m rag_snow_agent.eval.rank_ablation \
      --experiments_dir reports/experiments \
      --arms A0_full A1_no_best_of_n A2_no_verification A3_no_repair \
             A4_no_sample_records A5_no_join_graph A6_no_semantic A7_baseline
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from pathlib import Path

# The seven leave-one-out toggles, in canonical order. Each ablation arm sets
# exactly one of these True; the full arm sets none; the baseline sets all.
_TOGGLE_KEYS = [
    "disable_best_of_n",
    "disable_repair",
    "disable_verification",
    "disable_verifier",
    "disable_sample_records",
    "disable_join_graph",
    "disable_semantic",
    "disable_memory",
]

# Human-friendly component label per toggle.
_COMPONENT_LABEL = {
    "disable_best_of_n": "best_of_n",
    "disable_repair": "repair",
    "disable_verification": "verification (fingerprint/metamorphic)",
    "disable_verifier": "verifier",
    "disable_sample_records": "sample_records",
    "disable_join_graph": "join_graph",
    "disable_semantic": "semantic_layer",
    "disable_memory": "memory",
}


def _load_arm(arm_dir: Path) -> tuple[dict[str, bool], dict]:
    """Return ({instance_id: success}, toggles) for one experiment directory."""
    results_path = arm_dir / "instance_results.jsonl"
    manifest_path = arm_dir / "manifest.json"
    if not results_path.exists():
        raise FileNotFoundError(f"missing {results_path}")

    success: dict[str, bool] = {}
    with open(results_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            iid = rec.get("instance_id")
            if iid:
                success[iid] = bool(rec.get("success"))

    toggles: dict = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        toggles = manifest.get("toggles", {}) or {}
    return success, toggles


def _enabled_toggles(toggles: dict) -> set[str]:
    """Return the set of toggle keys that are True (i.e. components disabled)."""
    return {k for k in _TOGGLE_KEYS if toggles.get(k)}


# Convenience labels for name-parsed components (keeps output tidy).
_NAME_COMPONENT_LABEL = {
    "best_of_n": "best_of_n",
    "verification": "verification (fingerprint/metamorphic)",
    "verifier": "verifier",
    "repair": "repair",
    "sample_records": "sample_records",
    "join_graph": "join_graph",
    "semantic": "semantic_layer",
    "memory": "memory",
}


def _parse_arm_name(arm: str) -> tuple[str, str | None]:
    """Classify an arm directory name into (kind, component).

    Recognizes the ``A<n>_full`` / ``A<n>_baseline`` / ``A<n>_no_<component>``
    convention. This is the authoritative source for labeling because older
    runs recorded manifest toggles unreliably. Returns:
      ("full", None) | ("baseline", None) | ("component", <label>) | ("other", None)
    """
    stem = re.sub(r"^a\d+[_-]", "", arm.lower())  # strip leading A0_/A7- etc.
    if "full" in stem:
        return ("full", None)
    if "baseline" in stem:
        return ("baseline", None)
    if stem.startswith("no_") or stem.startswith("no-"):
        comp = stem[3:]
        return ("component", _NAME_COMPONENT_LABEL.get(comp, comp))
    return ("other", None)


def _paired_impact(full: dict[str, bool], arm: dict[str, bool]) -> dict:
    """Compute paired rescued/broken/net over the shared instance set."""
    shared = sorted(set(full) & set(arm))
    rescued = [i for i in shared if full[i] and not arm[i]]
    broken = [i for i in shared if arm[i] and not full[i]]
    n = len(shared)
    net = len(rescued) - len(broken)
    return {
        "n_paired": n,
        "rescued": len(rescued),
        "broken": len(broken),
        "rescued_ids": rescued,
        "broken_ids": broken,
        "net": net,
        "net_frac": (net / n) if n else 0.0,
    }


def _bootstrap_ci(
    full: dict[str, bool],
    arm: dict[str, bool],
    iterations: int = 2000,
    seed: int = 12345,
) -> tuple[float, float]:
    """Percentile bootstrap CI (2.5/97.5) on net_frac, resampling instances."""
    shared = sorted(set(full) & set(arm))
    n = len(shared)
    if n == 0:
        return (0.0, 0.0)
    # Per-instance signed contribution to net: +1 rescued, -1 broken, else 0.
    contrib = []
    for i in shared:
        if full[i] and not arm[i]:
            contrib.append(1)
        elif arm[i] and not full[i]:
            contrib.append(-1)
        else:
            contrib.append(0)

    rng = random.Random(seed)
    fracs = []
    for _ in range(iterations):
        s = 0
        for _ in range(n):
            s += contrib[rng.randrange(n)]
        fracs.append(s / n)
    fracs.sort()
    lo = fracs[int(0.025 * iterations)]
    hi = fracs[min(int(0.975 * iterations), iterations - 1)]
    return (round(lo, 4), round(hi, 4))


def _assign_tier(net_frac: float, ci_lo: float, ci_hi: float) -> str:
    """Bucket a component by impact magnitude and CI sign."""
    if ci_hi < 0:
        return "Hurts (removal improves)"
    if ci_lo > 0:  # CI excludes zero on the positive side
        if net_frac >= 0.20:
            return "Tier 1 — dominant"
        if net_frac >= 0.08:
            return "Tier 2 — moderate"
        return "Tier 2 — small but real"
    return "Tier 3 — negligible (within noise)"


def rank_components(
    experiments_dir: Path,
    arms: list[str],
    full_name: str | None = None,
    bootstrap_iters: int = 2000,
) -> dict:
    """Build the tiered impact ranking across leave-one-out ablation arms."""
    loaded: dict[str, tuple[dict, dict]] = {}
    for arm in arms:
        loaded[arm] = _load_arm(experiments_dir / arm)

    # Identify the full arm: explicit override, else by name, else the arm with
    # no toggles set (least reliable; older manifests mis-record toggles).
    if full_name is None:
        by_name = [a for a in loaded if _parse_arm_name(a)[0] == "full"]
        if by_name:
            full_name = by_name[0]
        else:
            candidates = [a for a, (_, t) in loaded.items() if not _enabled_toggles(t)]
            if not candidates:
                raise ValueError(
                    "Could not auto-detect the full arm. Pass --full explicitly."
                )
            full_name = candidates[0]
    full_success, _ = loaded[full_name]

    rows = []
    for arm, (arm_success, toggles) in loaded.items():
        if arm == full_name:
            continue
        # Label primarily from the arm name (authoritative); fall back to the
        # manifest toggle-diff when the name doesn't encode the component.
        kind, name_component = _parse_arm_name(arm)
        enabled = _enabled_toggles(toggles)
        if kind == "baseline":
            component = f"ALL components (baseline)"
            is_baseline = True
        elif kind == "component" and name_component:
            component = name_component
            is_baseline = False
        elif len(enabled) == 1:
            component = _COMPONENT_LABEL.get(next(iter(enabled)), next(iter(enabled)))
            is_baseline = False
        elif len(enabled) > 1:
            component = f"ALL components (baseline: {len(enabled)} disabled)"
            is_baseline = True
        else:
            component = f"{arm} (unlabeled)"
            is_baseline = False

        impact = _paired_impact(full_success, arm_success)
        ci_lo, ci_hi = _bootstrap_ci(full_success, arm_success, bootstrap_iters)
        tier = _assign_tier(impact["net_frac"], ci_lo, ci_hi)
        rows.append({
            "arm": arm,
            "component": component,
            "is_baseline": is_baseline,
            **impact,
            "net_frac_pct": round(100 * impact["net_frac"], 1),
            "ci_lo_pct": round(100 * ci_lo, 1),
            "ci_hi_pct": round(100 * ci_hi, 1),
            "tier": tier,
        })

    # Sort by net impact descending (most impactful component first).
    rows.sort(key=lambda r: r["net"], reverse=True)
    return {
        "full_arm": full_name,
        "full_accuracy_pct": round(
            100 * sum(full_success.values()) / len(full_success), 1
        ) if full_success else 0.0,
        "bootstrap_iters": bootstrap_iters,
        "components": rows,
    }


def render_markdown(ranking: dict) -> str:
    """Render the ranking as a markdown report."""
    lines = []
    lines.append("# Ablation impact ranking (paired leave-one-out)\n")
    lines.append(
        f"Reference (full system): **{ranking['full_arm']}** "
        f"= {ranking['full_accuracy_pct']}% accuracy. "
        f"Bootstrap: {ranking['bootstrap_iters']} resamples.\n"
    )
    lines.append(
        "`net` = instances the component rescues (full solves, ablated fails) "
        "minus instances it breaks. CI is a 95% bootstrap interval on net impact "
        "as a fraction of paired instances.\n"
    )
    lines.append("| Rank | Component | n | rescued | broken | net | net % | 95% CI | Tier |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|:---:|---|")
    for idx, r in enumerate(ranking["components"], 1):
        ci = f"[{r['ci_lo_pct']:+.1f}, {r['ci_hi_pct']:+.1f}]"
        lines.append(
            f"| {idx} | {r['component']} | {r['n_paired']} | {r['rescued']} | "
            f"{r['broken']} | {r['net']:+d} | {r['net_frac_pct']:+.1f}% | {ci} | {r['tier']} |"
        )
    lines.append("")
    lines.append(
        "_Tiers are ranked buckets, not significance claims. A CI spanning 0 "
        "means the component is within run-to-run noise at this N — reported as "
        "negligible rather than finely ordered._\n"
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rank ablation components into impact tiers")
    parser.add_argument("--experiments_dir", required=True,
                        help="Parent directory containing the arm subdirectories")
    parser.add_argument("--arms", nargs="+", required=True,
                        help="Arm directory names (include the full arm)")
    parser.add_argument("--full", default=None,
                        help="Name of the full-system arm (auto-detected if omitted)")
    parser.add_argument("--bootstrap_iters", type=int, default=2000)
    parser.add_argument("--out", default=None,
                        help="Optional path to write the markdown report")
    args = parser.parse_args()

    experiments_dir = Path(args.experiments_dir)
    ranking = rank_components(
        experiments_dir, args.arms, full_name=args.full,
        bootstrap_iters=args.bootstrap_iters,
    )
    md = render_markdown(ranking)
    print(md)

    json_path = experiments_dir / "ablation_ranking.json"
    json_path.write_text(json.dumps(ranking, indent=2) + "\n")
    if args.out:
        Path(args.out).write_text(md + "\n")
    print(f"\n[wrote {json_path}]", file=sys.stderr)


if __name__ == "__main__":
    main()
