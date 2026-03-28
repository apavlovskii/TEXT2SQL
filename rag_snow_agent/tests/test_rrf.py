"""Tests for RRF fusion and lexical tokenization."""

from rag_snow_agent.retrieval.hybrid_retriever import (
    reciprocal_rank_fusion,
    tokenize_identifier,
)


def test_tokenize_identifier_dotted():
    tokens = tokenize_identifier("TESTDB.PUBLIC.ORDERS")
    assert tokens == {"testdb", "public", "orders"}


def test_tokenize_identifier_underscored():
    tokens = tokenize_identifier("ORDER_ID")
    assert tokens == {"order", "id"}


def test_tokenize_identifier_camel():
    tokens = tokenize_identifier("orderDate")
    assert tokens == {"order", "date"}


def test_rrf_single_list():
    fused = reciprocal_rank_fusion([["a", "b", "c"]], k=60)
    ids = [item_id for item_id, _ in fused]
    assert ids == ["a", "b", "c"], "Single-list RRF preserves order"


def test_rrf_two_lists_agreement():
    """When both lists agree on order, fused order should match."""
    fused = reciprocal_rank_fusion([["a", "b", "c"], ["a", "b", "c"]], k=60)
    ids = [item_id for item_id, _ in fused]
    assert ids == ["a", "b", "c"]


def test_rrf_two_lists_disagreement():
    """Item ranked highly in both lists should win."""
    # List 1: a > b > c > d
    # List 2: c > a > d > b
    # 'a' is rank 1+2 = strong; 'c' is rank 3+1 = strong; 'b' is rank 2+4
    fused = reciprocal_rank_fusion(
        [["a", "b", "c", "d"], ["c", "a", "d", "b"]], k=60
    )
    ids = [item_id for item_id, _ in fused]
    # 'a': 1/61 + 1/62 = 0.01639 + 0.01613 = 0.03252
    # 'c': 1/63 + 1/61 = 0.01587 + 0.01639 = 0.03227
    # 'a' should beat 'c'
    assert ids[0] == "a"
    assert ids[1] == "c"


def test_rrf_deterministic():
    """Same input always produces same output."""
    lists = [["x", "y", "z"], ["z", "x", "y"]]
    r1 = reciprocal_rank_fusion(lists, k=60)
    r2 = reciprocal_rank_fusion(lists, k=60)
    assert r1 == r2
