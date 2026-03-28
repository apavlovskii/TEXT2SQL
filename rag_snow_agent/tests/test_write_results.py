"""Tests for Spider2 result writer."""

import json
import tempfile
from pathlib import Path

from rag_snow_agent.eval.write_results import write_spider2_result


def test_write_creates_valid_json():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_spider2_result(
            experiment="test_exp",
            instance_id="inst_001",
            sql="SELECT COUNT(*) FROM orders;",
            success=True,
            base_dir=tmp,
        )

        assert path.exists()
        data = json.loads(path.read_text())
        assert data["sql"] == "SELECT COUNT(*) FROM orders;"
        assert data["success"] is True


def test_write_creates_correct_path():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_spider2_result(
            experiment="snow_rag_v1",
            instance_id="sf_bq042",
            sql="SELECT 1;",
            success=False,
            base_dir=tmp,
        )

        expected = Path(tmp) / "snow_rag_v1" / "sf_bq042" / "spider" / "result.json"
        assert path == expected
        assert path.exists()


def test_write_failure_result():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_spider2_result(
            experiment="exp1",
            instance_id="inst_fail",
            sql="SELECT 1 /* failed */;",
            success=False,
            base_dir=tmp,
        )

        data = json.loads(path.read_text())
        assert data["success"] is False


def test_write_overwrites_existing():
    with tempfile.TemporaryDirectory() as tmp:
        write_spider2_result("exp", "i1", "SELECT 1;", False, base_dir=tmp)
        path = write_spider2_result("exp", "i1", "SELECT 2;", True, base_dir=tmp)

        data = json.loads(path.read_text())
        assert data["sql"] == "SELECT 2;"
        assert data["success"] is True
