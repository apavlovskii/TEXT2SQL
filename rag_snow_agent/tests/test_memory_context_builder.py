"""Tests for build_memory_context token budget and formatting."""

from rag_snow_agent.prompting.prompt_builder import build_memory_context


def _sample_traces(n=3):
    traces = []
    for i in range(n):
        traces.append(
            {
                "trace_id": f"t{i}",
                "document": f"instruction {i}\nTables: T{i}; Aggs: COUNT(*)",
                "metadata": {
                    "db_id": "TESTDB",
                    "instance_id": f"inst_{i:03d}",
                    "tables_used": f"TESTDB.PUBLIC.T{i}",
                },
                "distance": 0.1 * (i + 1),
            }
        )
    return traces


def test_build_memory_context_basic():
    traces = _sample_traces(2)
    ctx = build_memory_context(traces)
    assert "Prior successful" in ctx
    assert "t0" in ctx or "instruction 0" in ctx


def test_build_memory_context_empty():
    ctx = build_memory_context([])
    assert ctx == ""


def test_build_memory_context_respects_token_budget():
    # Create traces with long documents
    traces = []
    for i in range(10):
        traces.append(
            {
                "trace_id": f"t{i}",
                "document": f"This is a long instruction about topic {i}. " * 50,
                "metadata": {
                    "db_id": "TESTDB",
                    "instance_id": f"inst_{i:03d}",
                    "tables_used": f"TABLE_{i}",
                },
                "distance": 0.1 * (i + 1),
            }
        )
    ctx = build_memory_context(traces, max_memory_tokens=200)
    # Should be truncated; not all 10 traces should appear
    import tiktoken

    enc = tiktoken.get_encoding("cl100k_base")
    token_count = len(enc.encode(ctx))
    assert token_count <= 250  # allow small overshoot from header/footer


def test_build_memory_context_in_plan_prompt():
    """Memory context should be insertable into plan prompts."""
    from rag_snow_agent.prompting.prompt_builder import build_plan_prompt
    from rag_snow_agent.retrieval.schema_slice import (
        ColumnSlice,
        SchemaSlice,
        TableSlice,
    )

    schema = SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name="TESTDB.PUBLIC.T1",
                columns=[ColumnSlice(name="COL1", data_type="NUMBER")],
            ),
        ],
    )
    traces = _sample_traces(1)
    ctx = build_memory_context(traces)
    msgs = build_plan_prompt("count rows", schema, memory_context=ctx)
    user_msg = msgs[-1]["content"]
    assert "Prior successful" in user_msg
    assert "count rows" in user_msg
