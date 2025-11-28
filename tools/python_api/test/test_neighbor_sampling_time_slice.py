from __future__ import annotations

from pathlib import Path
import textwrap
import numpy as np


def _seed_time_sliced_graph(conn, tmp_dir: Path) -> None:
    """Create a minimal temporal graph and two projected snapshots using filters."""
    conn.execute("CREATE NODE TABLE ts_company(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE ts_person(id STRING, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE ts_partner (FROM ts_company TO ts_person, start_at DATE, end_at DATE, MANY_MANY);"
    )

    company_csv = tmp_dir / "ts_company.csv"
    company_csv.write_text("id\nc1\n", encoding="utf-8")
    conn.execute(f'COPY ts_company FROM "{company_csv}" (HEADER=true);')

    person_csv = tmp_dir / "ts_person.csv"
    person_csv.write_text("id\np1\np2\n", encoding="utf-8")
    conn.execute(f'COPY ts_person FROM "{person_csv}" (HEADER=true);')

    partner_csv = tmp_dir / "ts_partner.csv"
    partner_csv.write_text(
        textwrap.dedent(
            """\
            from_id,to_id,start_at,end_at
            c1,p1,2024-01-01,2024-12-31
            c1,p2,2025-01-01,
            """
        ),
        encoding="utf-8",
    )
    conn.execute(f'COPY ts_partner FROM "{partner_csv}" (HEADER=true);')

    # Two projected graphs filtered by edge validity date.
    conn.execute(
        """
        CALL PROJECT_GRAPH(
            'AVRA_2024',
            ['ts_company', 'ts_person'],
            {'ts_partner': 'r.start_at <= date("2024-06-01") AND (r.end_at IS NULL OR r.end_at > date("2024-06-01"))'}
        );
        """
    )
    conn.execute(
        """
        CALL PROJECT_GRAPH(
            'AVRA_2025',
            ['ts_company', 'ts_person'],
            {'ts_partner': 'r.start_at <= date("2025-06-01") AND (r.end_at IS NULL OR r.end_at > date("2025-06-01"))'}
        );
        """
    )


def test_neighbor_sample_time_slice_differs(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    _seed_time_sliced_graph(conn, tmp_path)

    seeds = {"ts_company": np.array([0], dtype=np.int64)}  # offset 0 corresponds to c1

    sample_2024 = conn.neighbor_sample("AVRA_2024", seeds, [10], seed=7)
    sample_2025 = conn.neighbor_sample("AVRA_2025", seeds, [10], seed=7)

    assert int(sample_2024["num_sampled_edges_per_hop"][0]) == 1
    assert int(sample_2025["num_sampled_edges_per_hop"][0]) == 1

    # Neighbor nodes at hop 1 should differ (p1 vs p2). Local id 0 is the seed company.
    hop1_offset_2024 = sample_2024["node_offsets"][1]
    hop1_offset_2025 = sample_2025["node_offsets"][1]
    assert hop1_offset_2024 != hop1_offset_2025
    # rel ids should be present in both samples
    assert sample_2024["rel_table_ids"].shape[0] == sample_2024["src_local_ids"].shape[0]


def test_neighbor_sample_time_option(conn_db_readwrite, tmp_path) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE ts_person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE ts_edge (FROM ts_person TO ts_person, start_at DATE, end_at DATE, MANY_MANY);")

    node_csv = tmp_path / "ts_person_nodes.csv"
    node_csv.write_text("id\na\nb\nc\n", encoding="utf-8")
    conn.execute(f'COPY ts_person FROM "{node_csv}" (HEADER=true);')

    rel_csv = tmp_path / "ts_edges.csv"
    rel_csv.write_text(
        "from_id,to_id,start_at,end_at\n"
        "a,b,2024-01-01,2024-12-31\n"
        "a,c,2025-01-01,\n",
        encoding="utf-8",
    )
    conn.execute(f'COPY ts_edge FROM "{rel_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('TS', ['ts_person'], ['ts_edge']);")

    seeds = {"ts_person": np.array([0], dtype=np.int64)}
    s1 = conn.neighbor_sample("TS", seeds, [10], time="2024-06-01")
    s2 = conn.neighbor_sample("TS", seeds, [10], time="2025-06-01")

    assert s1["dst_local_ids"].shape[0] == 1
    assert s2["dst_local_ids"].shape[0] == 1
    # Compare actual node offsets reached at hop1 to ensure temporal filter applied.
    dst1 = int(s1["dst_local_ids"][0])
    dst2 = int(s2["dst_local_ids"][0])
    assert int(s1["node_offsets"][dst1]) != int(s2["node_offsets"][dst2])
