# Neighbor Sampling (Temporal) – Architecture & Performance Notes

## What lives where

- **Edges on disk**: The PyG remote backend (`MonadGraphStore`) defaults to `materialize_edges=False`, so COO is not loaded in memory. Sampling happens server-side.
- **Prepared scanners**: The sampler builds per-table scanner maps once per config/direction (reused if `preloadScanners()` is called). For temporal sampling, it also pulls `start_at` / `end_at` columns when present.
- **In-memory state per call**:
  - Seed map (`nodeID_t -> local id`)
  - BFS frontier slices (`begin/end` indices into the result node vector)
  - RNG (`std::mt19937_64`) seeded per request (optional fixed seed)
  - Edge buffers per hop (vector of edges with local IDs + relTableID)

## Temporal filtering semantics

- An optional `as_of` timestamp in `NeighborSamplingConfig`.
- Edge is included iff `start_at <= as_of < end_at` (or `end_at` is NULL). Column names matched case-insensitively to `start_at` / `end_at`.
- Works for DATE and TIMESTAMP family columns.
- Applied during scan before sampling; reservoir counts only time-valid edges.

## Sampling algorithm (per hop)

1. For each frontier node:
   - Choose direction (FWD/BWD/BOTH) as configured.
   - For each adjacent rel table:
     - Resolve fanout (default or per-rel override).
     - Collect neighbors with optional temporal filter.
     - Apply:
       - Reservoir sampling for `fanout >= 0` & `!replace`
       - Uniform w/ replacement for `fanout >= 0` & `replace`
       - All neighbors if `fanout < 0`
   - Deduplicate nodes with `nodeToLocal` map; record edges with local IDs.
2. Record per-hop sampled node/edge counts.

## API surface

- SQL: `CALL neighbor_sample('G', {person:[0]}, [10,10], {time: date('2024-06-01'), seed: 7});`
- Python: `conn.neighbor_sample("G", seeds, fanouts, time="2024-06-01", seed=7)`
- PyG: `MonadDBSampler(..., time="2024-06-01")` or `MonadNeighborLoader(..., time=...)`

## Performance considerations

- **Scanning cost**: One additional branch per neighbor to check validity window when `as_of` is set. No extra cost when `as_of` is absent.
- **Cache**: `preloadScanners()` avoids rebuilding scanner maps across calls; temporal mode reuses a temporal-aware scanner set.
- **Reservoir vs. materialization**: Reservoir sampling streams neighbors; no full materialization unless `replace=True` (which buffers all neighbors for that node).
- **Projection strategy**:
  - Many time cuts → single historical projection + `as_of` at sampling.
  - Few hot cuts → optional pre-filtered projections to remove runtime filter.

## Quick benchmark (see bench/neighbor_sampling_bench.py)

Runs a synthetic graph in-memory, compares with/without `as_of`:

```bash
cd tools/python_api
uv run python bench/neighbor_sampling_bench.py --nodes 20000 --degree 5 --iters 50
```

Reported metrics:
- edges sampled / second
- latency per call

Use this to size fanouts and decide whether to pre-filter projections for specific hot snapshots.
