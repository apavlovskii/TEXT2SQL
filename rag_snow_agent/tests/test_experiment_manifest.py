"""Tests for experiment manifest creation."""

import argparse
import json
import tempfile
from pathlib import Path

from rag_snow_agent.eval.experiment_runner import (
    apply_cli_toggles,
    load_config,
    merge_config,
    write_manifest,
)


def _make_args(**kwargs) -> argparse.Namespace:
    """Build a Namespace with default toggle values."""
    defaults = {
        "experiment": "test_exp",
        "limit": 10,
        "model": "gpt-4o-mini",
        "best_of_n": 2,
        "disable_memory": False,
        "disable_verifier": False,
        "disable_best_of_n": False,
        "disable_repair": False,
        "disable_verification": False,
        "disable_join_graph": False,
        "max_repairs": None,
        "ablation_preset": None,
        "split_jsonl": "fake.jsonl",
        "credentials": "creds.json",
        "chroma_dir": None,
        "skip_preflight": True,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_write_manifest_creates_json():
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "my_exp"
        args = _make_args(experiment="my_exp")
        config = {"llm": {"model": "gpt-4o-mini"}, "features": {}}

        path = write_manifest(exp_dir, config, args)
        assert path.exists()

        data = json.loads(path.read_text())
        assert data["experiment"] == "my_exp"
        assert data["model"] == "gpt-4o-mini"
        assert data["limit"] == 10
        assert "timestamp" in data
        assert "toggles" in data
        assert "config_snapshot" in data


def test_manifest_captures_toggles():
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "toggle_test"
        args = _make_args(
            experiment="toggle_test",
            disable_memory=True,
            disable_verifier=True,
        )
        config = {"llm": {"model": "gpt-4o-mini"}, "features": {}}

        path = write_manifest(exp_dir, config, args)
        data = json.loads(path.read_text())

        assert data["toggles"]["disable_memory"] is True
        assert data["toggles"]["disable_verifier"] is True
        assert data["toggles"]["disable_best_of_n"] is False


def test_apply_cli_toggles_disables_memory():
    config = {
        "features": {"memory": True},
        "memory": {"enabled": True},
    }
    args = _make_args(disable_memory=True)
    result = apply_cli_toggles(config, args)

    assert result["features"]["memory"] is False
    assert result["memory"]["enabled"] is False


def test_apply_cli_toggles_disables_best_of_n():
    config = {
        "features": {"best_of_n": True},
        "agent": {"best_of_n": 2},
    }
    args = _make_args(disable_best_of_n=True, best_of_n=2)
    result = apply_cli_toggles(config, args)

    assert result["features"]["best_of_n"] is False
    assert result["agent"]["best_of_n"] == 1


def test_apply_cli_toggles_disables_repair():
    config = {"features": {}, "agent": {"max_repairs": 2}}
    args = _make_args(disable_repair=True)
    result = apply_cli_toggles(config, args)

    assert result["features"]["repair"] is False
    assert result["agent"]["max_repairs"] == 0


def test_apply_cli_toggles_model_override():
    config = {"llm": {"model": "gpt-4o-mini"}}
    args = _make_args(model="gpt-4o")
    result = apply_cli_toggles(config, args)

    assert result["llm"]["model"] == "gpt-4o"


def test_merge_config_deep():
    base = {"a": {"b": 1, "c": 2}, "d": 3}
    override = {"a": {"b": 99}, "e": 4}
    merged = merge_config(base, override)

    assert merged["a"]["b"] == 99
    assert merged["a"]["c"] == 2
    assert merged["d"] == 3
    assert merged["e"] == 4


def test_manifest_config_snapshot_preserved():
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp) / "snapshot_test"
        config = {
            "llm": {"model": "gpt-4o-mini", "temperature": 0.2},
            "agent": {"best_of_n": 2},
            "features": {"memory": True},
        }
        args = _make_args(experiment="snapshot_test")

        path = write_manifest(exp_dir, config, args)
        data = json.loads(path.read_text())

        snapshot = data["config_snapshot"]
        assert snapshot["llm"]["model"] == "gpt-4o-mini"
        assert snapshot["agent"]["best_of_n"] == 2
        assert snapshot["features"]["memory"] is True
