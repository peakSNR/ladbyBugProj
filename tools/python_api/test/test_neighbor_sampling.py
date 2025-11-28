from __future__ import annotations

import numpy as np
import pytest


def test_connection_neighbor_sample(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")
    result = conn.neighbor_sample("PYG_PERSON", {"person": np.array([0, 2])}, [2], seed=42)
    assert "node_offsets" in result
    assert result["src_local_ids"].shape[0] == result["dst_local_ids"].shape[0]
    assert result["node_table_ids"].dtype == np.int64
    # rel_table_ids should be present and align with edges
    assert result["rel_table_ids"].shape[0] == result["src_local_ids"].shape[0]


def test_graph_store_neighbor_sample(conn_db_readwrite) -> None:
    conn, db = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")
    _, graph_store = db.get_torch_geometric_remote_backend(graph_name="PYG_PERSON", connection=conn)
    sample = graph_store.neighbor_sample("person", [0], [1], seed=7)
    assert sample["node_offsets"].shape[0] >= 1


def test_connection_neighbor_sample_rejects_negative_seed(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")
    with pytest.raises(RuntimeError, match="seed must be non-negative"):
        conn.neighbor_sample("PYG_PERSON", {"person": np.array([0])}, [1], seed=-1)


def test_graph_store_neighbor_sample_multiple_tables(conn_db_readwrite) -> None:
    conn, db = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_MIX', ['person', 'organisation'], ['workAt']);")
    _, graph_store = db.get_torch_geometric_remote_backend(graph_name="PYG_MIX", connection=conn)

    seeds = {"person": np.array([0], dtype=np.int64), "organisation": np.array([0], dtype=np.int64)}
    sample = graph_store.neighbor_sample(seeds, None, [1], seed=11)
    assert sample["node_offsets"].shape[0] >= 1


def test_connection_neighbor_sample_multiple_tables(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_HET', ['person', 'organisation'], ['workAt']);")
    seeds = {"person": np.array([0], dtype=np.int64), "organisation": np.array([0], dtype=np.int64)}
    result = conn.neighbor_sample("PYG_HET", seeds, [1], seed=17)
    assert result["node_offsets"].shape[0] >= 2
    unique_tables = np.unique(result["node_table_ids"])
    assert unique_tables.size >= 2


def test_connection_neighbor_sample_custom_direction(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_DIR', ['person'], ['knows']);")
    fanouts = [{"fanout": 1, "direction": "both"}]
    result = conn.neighbor_sample("PYG_DIR", {"person": np.array([0])}, fanouts, seed=21)
    assert result["num_sampled_edges_per_hop"].shape[0] == 1
    assert int(result["num_sampled_edges_per_hop"][0]) >= 1


def test_neighbor_sample_deterministic_with_seed(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE det_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE det_knows (FROM det_person TO det_person, MANY_MANY);")
    node_csv = tmp_path / "det_person.csv"
    node_csv.write_text("id\na\nb\nc\n", encoding="utf-8")
    conn.execute(f'COPY det_person FROM "{node_csv}" (HEADER=true);')
    rel_csv = tmp_path / "det_knows.csv"
    rel_csv.write_text("from_id,to_id\na,b\na,c\n", encoding="utf-8")
    conn.execute(f'COPY det_knows FROM "{rel_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('DET', ['det_person'], ['det_knows']);")

    seeds = {"det_person": np.array([0], dtype=np.int64)}
    fanouts = [1]

    sample1 = conn.neighbor_sample("DET", seeds, fanouts, seed=123)
    sample2 = conn.neighbor_sample("DET", seeds, fanouts, seed=123)

    assert np.array_equal(sample1["node_offsets"], sample2["node_offsets"])
    assert np.array_equal(sample1["src_local_ids"], sample2["src_local_ids"])
    assert np.array_equal(sample1["dst_local_ids"], sample2["dst_local_ids"])


def test_neighbor_sample_with_replacement_single_neighbor(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE wr_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE wr_edge (FROM wr_person TO wr_person, MANY_MANY);")
    node_csv = tmp_path / "wr_person.csv"
    node_csv.write_text("id\na\nb\n", encoding="utf-8")
    conn.execute(f'COPY wr_person FROM "{node_csv}" (HEADER=true);')
    rel_csv = tmp_path / "wr_edge.csv"
    rel_csv.write_text("from_id,to_id\na,b\n", encoding="utf-8")
    conn.execute(f'COPY wr_edge FROM "{rel_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('WR', ['wr_person'], ['wr_edge']);")

    sample = conn.neighbor_sample(
        "WR",
        {"wr_person": np.array([0], dtype=np.int64)},
        [4],
        replace=True,
        seed=7,
    )

    # four edges because replace=True, but all point to the lone neighbor
    assert int(sample["num_sampled_edges_per_hop"][0]) == 4
    dst_ids = sample["dst_local_ids"]
    assert dst_ids.shape[0] == 4
    assert np.unique(dst_ids).size == 1


def test_neighbor_sample_rel_override_excludes(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE fo_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE fo_knows (FROM fo_person TO fo_person, MANY_MANY);")
    conn.execute("CREATE REL TABLE fo_likes (FROM fo_person TO fo_person, MANY_MANY);")
    node_csv = tmp_path / "fo_person.csv"
    node_csv.write_text("id\na\nb\nc\n", encoding="utf-8")
    conn.execute(f'COPY fo_person FROM "{node_csv}" (HEADER=true);')
    knows_csv = tmp_path / "fo_knows.csv"
    knows_csv.write_text("from_id,to_id\na,b\n", encoding="utf-8")
    conn.execute(f'COPY fo_knows FROM "{knows_csv}" (HEADER=true);')
    likes_csv = tmp_path / "fo_likes.csv"
    likes_csv.write_text("from_id,to_id\na,c\n", encoding="utf-8")
    conn.execute(f'COPY fo_likes FROM "{likes_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('FO', ['fo_person'], ['fo_knows', 'fo_likes']);")

    fanouts = [{"fanout": -1, "rels": {"fo_knows": 0}}]
    sample = conn.neighbor_sample("FO", {"fo_person": np.array([0], dtype=np.int64)}, fanouts, seed=5)

    # Should only traverse fo_likes edge (to node offset 2)
    assert int(sample["num_sampled_edges_per_hop"][0]) == 1
    assert int(sample["dst_local_ids"][0]) == 1  # local id 1 corresponds to offset 2


def test_neighbor_sample_rel_table_ids_multiple_rels(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE rt_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE rt_r1 (FROM rt_person TO rt_person, MANY_MANY);")
    conn.execute("CREATE REL TABLE rt_r2 (FROM rt_person TO rt_person, MANY_MANY);")
    node_csv = tmp_path / "rt_person.csv"
    node_csv.write_text("id\na\nb\nc\n", encoding="utf-8")
    conn.execute(f'COPY rt_person FROM "{node_csv}" (HEADER=true);')
    r1_csv = tmp_path / "rt_r1.csv"
    r1_csv.write_text("from_id,to_id\na,b\n", encoding="utf-8")
    conn.execute(f'COPY rt_r1 FROM "{r1_csv}" (HEADER=true);')
    r2_csv = tmp_path / "rt_r2.csv"
    r2_csv.write_text("from_id,to_id\na,c\n", encoding="utf-8")
    conn.execute(f'COPY rt_r2 FROM "{r2_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('RT', ['rt_person'], ['rt_r1', 'rt_r2']);")

    sample = conn.neighbor_sample("RT", {"rt_person": np.array([0], dtype=np.int64)}, [-1], seed=9)

    assert int(sample["num_sampled_edges_per_hop"][0]) == 2
    assert np.unique(sample["rel_table_ids"]).size == 2
