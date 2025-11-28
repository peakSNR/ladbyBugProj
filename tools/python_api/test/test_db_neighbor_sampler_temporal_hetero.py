from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
from torch_geometric.sampler import NodeSamplerInput

from monad import Connection
from monad.torch_geometric_db_sampler import MonadDBSampler


def _seed_temporal_hetero(conn: Connection, tmp_path: Path) -> None:
    conn.execute("CREATE NODE TABLE th_company(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE th_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE th_employs (FROM th_company TO th_person, start_at DATE, end_at DATE, MANY_MANY);")
    conn.execute("CREATE REL TABLE th_sues (FROM th_company TO th_company, start_at DATE, end_at DATE, MANY_MANY);")

    company_csv = tmp_path / "th_company.csv"
    company_csv.write_text("id\nc1\nc2\n", encoding="utf-8")
    conn.execute(f'COPY th_company FROM "{company_csv}" (HEADER=true);')

    person_csv = tmp_path / "th_person.csv"
    person_csv.write_text("id\np1\np2\n", encoding="utf-8")
    conn.execute(f'COPY th_person FROM "{person_csv}" (HEADER=true);')

    employs_csv = tmp_path / "th_employs.csv"
    employs_csv.write_text(
        "from_id,to_id,start_at,end_at\n"
        "c1,p1,2024-01-01,2024-12-31\n"
        "c1,p2,2025-01-01,\n",
        encoding="utf-8",
    )
    conn.execute(f'COPY th_employs FROM "{employs_csv}" (HEADER=true);')

    sues_csv = tmp_path / "th_sues.csv"
    sues_csv.write_text(
        "from_id,to_id,start_at,end_at\n"
        "c1,c2,2025-01-01,\n",
        encoding="utf-8",
    )
    conn.execute(f'COPY th_sues FROM "{sues_csv}" (HEADER=true);')

    conn.execute(
        "CALL PROJECT_GRAPH('TH', ['th_company', 'th_person'], ['th_employs', 'th_sues']);"
    )


def test_db_sampler_temporal_hetero_time_fields(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    _seed_temporal_hetero(conn, tmp_path)

    sampler = MonadDBSampler(
        connection=conn,
        graph_name="TH",
        num_neighbors=[-1],
        node_table="th_company",
        seed=7,
        time="2024-06-01",
        time_fields={"start": ["start_at"], "end": ["end_at"]},
    )

    seeds = torch.tensor([0], dtype=torch.long)  # c1
    out_2024 = sampler.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type="th_company"))

    sampler_2025 = MonadDBSampler(
        connection=conn,
        graph_name="TH",
        num_neighbors=[-1],
        node_table="th_company",
        seed=7,
        time="2025-06-01",
        time_fields={"start": ["start_at"], "end": ["end_at"]},
    )
    out_2025 = sampler_2025.sample_from_nodes(NodeSamplerInput(input_id=None, node=seeds, input_type="th_company"))

    # In 2024 only employs->p1 should be valid; in 2025 employs->p2 plus sues edge should be valid.
    # Edge dict appears only when multiple rel types are present in the sample; ensure rel ids distinguish rels.

    # Extract sampled dst offsets for hop1 per snapshot.
    nodes_2024 = out_2024.node.numpy()
    dst_local_2024 = out_2024.col.numpy()
    hop1_dst_offsets_2024 = nodes_2024[dst_local_2024]
    node_table_ids_2024 = out_2024.node_table_ids.numpy()

    nodes_2025 = out_2025.node.numpy()
    dst_local_2025 = out_2025.col.numpy()
    hop1_dst_offsets_2025 = nodes_2025[dst_local_2025]
    node_table_ids_2025 = out_2025.node_table_ids.numpy()

    # We expect only one employ edge active in 2024 (to p1). There should be exactly 1 edge
    # and its table should be th_person (different from company table id at seed index 0).
    assert len(dst_local_2024) == 1
    assert node_table_ids_2024[dst_local_2024[0]] != node_table_ids_2024[0]

    # In 2025 we should see at least the person neighbor and the company-company edge.
    assert len(dst_local_2025) >= 2
    # At least one neighbor should be a person (different table id from company seed).
    assert any(node_table_ids_2025[i] != node_table_ids_2025[0] for i in dst_local_2025)
    # And at least one neighbor should be another company (same table id as seed but different offset).
    assert any((node_table_ids_2025[i] == node_table_ids_2025[0] and nodes_2025[i] != nodes_2025[0]) for i in dst_local_2025)

    # Different rel ids present (employs vs sues)
    rel_ids_2025 = out_2025.edge.numpy()
    assert len(np.unique(rel_ids_2025)) >= 2
