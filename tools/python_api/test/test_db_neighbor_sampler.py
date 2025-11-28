from __future__ import annotations

import numpy as np
import torch
from torch_geometric.loader import NeighborLoader
from torch_geometric.sampler import NodeSamplerInput

from monad import Connection

from monad.torch_geometric_db_sampler import MonadDBSampler, make_db_neighbor_loader


def test_db_neighbor_sampler_returns_distinct_neighbors(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="PYG_PERSON",
        num_neighbors=[2],
        node_table="person",
        replace=False,
        direction="fwd",
        seed=123,
    )

    seeds = torch.tensor([0, 2], dtype=torch.long)
    out = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type=None))

    # Should include seed nodes plus sampled neighbors.
    assert out.node.numel() >= 2
    # Edges should align row/col endpoints.
    assert out.row.numel() == out.col.numel()
    assert out.num_sampled_edges.numel() == 1
    assert int(out.num_sampled_edges[0]) >= 1

    # Batch must preserve seed ownership per node.
    assert torch.equal(out.batch[: seeds.numel()], torch.arange(seeds.numel()))
    assert out.batch.max() < seeds.numel()


def test_db_neighbor_sampler_hetero_edge_index_dict(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE hx_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE hx_org(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE hx_work (FROM hx_person TO hx_org, MANY_MANY);")
    conn.execute("CREATE REL TABLE hx_meet (FROM hx_person TO hx_person, MANY_MANY);")

    person_csv = tmp_path / "hx_person.csv"
    person_csv.write_text("id\np1\np2\n", encoding="utf-8")
    org_csv = tmp_path / "hx_org.csv"
    org_csv.write_text("id\no1\n", encoding="utf-8")
    work_csv = tmp_path / "hx_work.csv"
    work_csv.write_text("from_id,to_id\np1,o1\n", encoding="utf-8")
    meet_csv = tmp_path / "hx_meet.csv"
    meet_csv.write_text("from_id,to_id\np1,p2\n", encoding="utf-8")

    conn.execute(f'COPY hx_person FROM "{person_csv}" (HEADER=true);')
    conn.execute(f'COPY hx_org FROM "{org_csv}" (HEADER=true);')
    conn.execute(f'COPY hx_work FROM "{work_csv}" (HEADER=true);')
    conn.execute(f'COPY hx_meet FROM "{meet_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('PYG_HET', ['hx_person', 'hx_org'], ['hx_work', 'hx_meet']);")

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="PYG_HET",
        num_neighbors=[1],
        node_table="hx_person",
        seed=7,
    )

    seeds = torch.tensor([0], dtype=torch.long)
    out = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type="hx_person"))

    assert hasattr(out, "edge_index_dict")
    assert len(out.edge_index_dict) == 2


def test_make_db_neighbor_loader_builds(conn_db_readwrite) -> None:
    conn, db = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")
    # Use the remote backend helpers (feature_store not used in sampling here).
    from monad.torch_geometric_feature_store import MonadFeatureStore
    from monad.torch_geometric_graph_store import MonadGraphStore

    fs = MonadFeatureStore(db, num_threads=1, connection=conn)
    gs = MonadGraphStore(db, num_threads=1, graph_name="PYG_PERSON", connection=conn, materialize_edges=False)

    loader = make_db_neighbor_loader(
        fs,
        gs,
        input_nodes=("person", torch.tensor([0], dtype=torch.long)),
        num_neighbors=[1],
        batch_size=1,
        shuffle=False,
        seed=7,
    )

    # We return a lightweight DBNeighborLoader, not PyG's NeighborLoader.
    first_batch = next(iter(loader))
    assert hasattr(first_batch, "node")


def test_db_sampler_infers_single_node_table(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="PYG_PERSON",
        num_neighbors=[1],
        node_table=None,
        replace=False,
        direction="fwd",
        seed=99,
    )

    out = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=torch.tensor([0]), input_type=None))
    assert out.node.numel() >= 1


def test_db_sampler_batch_multihop_multiseed(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE mh_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE mh_knows (FROM mh_person TO mh_person, MANY_MANY);")

    node_csv = tmp_path / "mh_person.csv"
    node_csv.write_text("id\na\nb\nc\nd\ne\nf\n", encoding="utf-8")
    conn.execute(f'COPY mh_person FROM "{node_csv}" (HEADER=true);')

    rel_csv = tmp_path / "mh_knows.csv"
    rel_csv.write_text("from_id,to_id\na,b\nb,c\nc,d\n",
                       encoding="utf-8")
    conn.execute(f'COPY mh_knows FROM "{rel_csv}" (HEADER=true);')

    conn.execute("CALL PROJECT_GRAPH('MH', ['mh_person'], ['mh_knows']);")

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="MH",
        num_neighbors=[1, 1],
        node_table="mh_person",
        seed=123,
    )

    # Seeds are offsets 0 (a) and 2 (c). Hop 1 nodes: b and d. Hop 2 node: c reached from seed 0.
    seeds = torch.tensor([0, 2], dtype=torch.long)
    out = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type="mh_person"))

    # batch vector should keep ownership per seed for deeper hops
    assert out.batch.shape[0] == out.node.shape[0]
    # seed positions are correct
    assert torch.equal(out.batch[:2], torch.tensor([0, 1]))
    # node corresponding to offset d (hop1 from seed 2) should belong to seed 1
    # offset d is 3, find its local id
    offset_tensor = out.node
    idx_d = (offset_tensor == 3).nonzero(as_tuple=True)[0]
    assert idx_d.numel() == 1
    assert int(out.batch[int(idx_d[0])]) == 1


def test_db_sampler_exposes_node_metadata(conn_db_readwrite) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CALL PROJECT_GRAPH('PYG_PERSON', ['person'], ['knows']);")

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="PYG_PERSON",
        num_neighbors=[1],
        node_table="person",
        seed=11,
    )

    seeds = torch.tensor([0], dtype=torch.long)
    out = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type="person"))

    assert hasattr(out, "node_table_ids")
    assert hasattr(out, "node_hops")
    assert out.node_table_ids.numel() == out.node.numel()
    assert out.node_hops[0] == 0  # seed hop


def test_monad_neighbor_loader_integration(tmp_path) -> None:
    from monad import Connection, Database
    from monad.torch_geometric_feature_store import MonadFeatureStore
    from monad.torch_geometric_graph_store import MonadGraphStore
    from monad.torch_geometric_db_sampler import MonadNeighborLoader

    node_csv = tmp_path / "n.csv"
    rel_csv = tmp_path / "r.csv"
    node_csv.write_text("id\na\nb\n", encoding="utf-8")
    rel_csv.write_text("from_id,to_id\na,b\n", encoding="utf-8")

    db = Database(tmp_path / "db.kz")
    conn = Connection(db)
    conn.execute("CREATE NODE TABLE n(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE r (FROM n TO n, MANY_MANY);")
    conn.execute(f'COPY n FROM "{node_csv}" (HEADER=true);')
    conn.execute(f'COPY r FROM "{rel_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('G', ['n'], ['r']);")

    fs = MonadFeatureStore(db, num_threads=1, connection=conn, graph_name="G")
    gs = MonadGraphStore(db, num_threads=1, graph_name="G", connection=conn, materialize_edges=False)

    loader = MonadNeighborLoader(
        data=(fs, gs),
        num_neighbors=[1],
        input_nodes=("n", torch.tensor([0], dtype=torch.long)),
        batch_size=1,
        shuffle=False,
        seed=7,
    )

    batch = next(iter(loader))
    # NeighborLoader returns a Data object; n_id contains sampled node ids.
    assert hasattr(batch, "n_id")
    assert batch.n_id.numel() >= 1
    assert hasattr(batch, "batch")
    assert batch.batch.numel() == batch.n_id.numel()
    assert batch.batch.max() < 1
