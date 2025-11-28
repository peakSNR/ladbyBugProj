from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

import numpy as np

from monad import Connection, Database


def build_graph(db_path: Path, num_nodes: int, degree: int) -> Connection:
    db = Database(str(db_path))
    conn = Connection(db)

    conn.execute("CREATE NODE TABLE n(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE r (FROM n TO n, start_at DATE, end_at DATE, MANY_MANY);")

    node_csv = db_path.parent / "nodes.csv"
    rel_csv = db_path.parent / "rels.csv"

    # nodes
    with node_csv.open("w", encoding="utf-8") as f:
        f.write("id\n")
        for i in range(num_nodes):
            f.write(f"n{i}\n")

    # edges: each node points to the next `degree` nodes cyclically; half expire end of 2024
    with rel_csv.open("w", encoding="utf-8") as f:
        f.write("from_id,to_id,start_at,end_at\n")
        for i in range(num_nodes):
            for d in range(degree):
                j = (i + d + 1) % num_nodes
                start_at = "2023-01-01"
                end_at = "2024-12-31" if (d % 2 == 0) else ""
                f.write(f"n{i},n{j},{start_at},{end_at}\n")

    conn.execute(f'COPY n FROM "{node_csv}" (HEADER=true);')
    conn.execute(f'COPY r FROM "{rel_csv}" (HEADER=true);')
    conn.execute("CALL PROJECT_GRAPH('G', ['n'], ['r']);")
    return conn


def bench(conn: Connection, seeds: np.ndarray, fanouts: list[int], time_opt: str | None, iters: int):
    start = time.perf_counter()
    total_edges = 0
    for _ in range(iters):
        res = conn.neighbor_sample("G", {"n": seeds}, fanouts, time=time_opt)
        total_edges += int(res["num_sampled_edges_per_hop"].sum())
    elapsed = time.perf_counter() - start
    return total_edges, elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=20000)
    parser.add_argument("--degree", type=int, default=5)
    parser.add_argument("--fanout", type=int, nargs="+", default=[10])
    parser.add_argument("--iters", type=int, default=50)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "bench.kz"
        conn = build_graph(db_path, args.nodes, args.degree)

        seeds = np.arange(0, min(1024, args.nodes), dtype=np.int64)

        total_edges, elapsed = bench(conn, seeds, args.fanout, None, args.iters)
        print(f"Without as_of: edges={total_edges}, calls={args.iters}, avg_ms={(elapsed/args.iters)*1e3:.2f}, edges/s={total_edges/elapsed:,.0f}")

        total_edges_t, elapsed_t = bench(conn, seeds, args.fanout, "2024-06-01", args.iters)
        print(f"With as_of=2024-06-01: edges={total_edges_t}, calls={args.iters}, avg_ms={(elapsed_t/args.iters)*1e3:.2f}, edges/s={total_edges_t/elapsed_t:,.0f}")


if __name__ == "__main__":
    main()
