from __future__ import annotations

from typing import Any

import numpy as np
import torch
from torch_geometric.loader import NeighborLoader
from torch_geometric.sampler import (
    BaseSampler,
    NodeSamplerInput,
    SamplerOutput as NodeSamplerOutput,
)

from .connection import Connection
from .torch_geometric_hetero_adapter import build_hetero_edge_index


class MonadDBSampler:
    """PyG sampler that delegates neighbor sampling to the Monad server."""

    def __init__(
        self,
        connection: Connection,
        graph_name: str,
        num_neighbors: list[int],
        *,
        node_table: str | None = None,
        replace: bool = False,
        direction: str = "fwd",
        seed: int | None = None,
        time: Any | None = None,
        time_fields: dict[str, Any] | None = None,
    ) -> None:
        self.connection = connection
        self.graph_name = graph_name
        self.num_neighbors = num_neighbors
        self.node_table = node_table
        self.replace = replace
        self.direction = direction
        self.seed = seed
        self.time = time
        self.time_fields = time_fields
        self.rel_id_to_edge_type = {
            int(item["rel_table_id"]): (item["src"], item["name"], item["dst"])
            for item in self.connection._get_graph_rel_info(graph_name)
        }

    def sample_from_nodes(self, inputs: NodeSamplerInput) -> NodeSamplerOutput:
        # inputs.node is a 1D tensor of seed node indices (offsets).
        seeds_np = inputs.node.cpu().numpy().astype(np.int64, copy=False)
        # We need a mapping from node table name to offsets; for remote backend the
        # default projected graph usually has a single node table. We pass it as None
        # to indicate "use the only node table", which is the convention for the C++ API.
        table_name = self.node_table or inputs.input_type
        sample = None

        def try_sample(table: str):
            return self.connection.neighbor_sample(
                self.graph_name,
                {table: seeds_np},
                self.num_neighbors,
                replace=self.replace,
                direction=self.direction,
                seed=self.seed,
                time=self.time,
                time_fields=self.time_fields,
            )

        if table_name is not None:
            sample = try_sample(table_name)
        else:
            node_tables = self.connection._get_node_table_names()
            last_err: Exception | None = None
            for candidate in node_tables:
                try:
                    sample = try_sample(candidate)
                    table_name = candidate
                    break
                except Exception as exc:  # noqa: PERF203 - small candidate list
                    last_err = exc
                    continue
            if sample is None:
                raise ValueError(
                    "node_table must be provided when the graph has multiple node tables."
                ) from last_err

        nodes = torch.from_numpy(sample["node_offsets"]).long()
        row = torch.from_numpy(sample["src_local_ids"]).long()
        col = torch.from_numpy(sample["dst_local_ids"]).long()
        edge_index = torch.stack([row, col], dim=0)

        node_table_ids = torch.from_numpy(sample.get("node_table_ids", np.empty(0, dtype=np.int64))).long()
        node_hops = torch.from_numpy(sample.get("node_hops", np.empty(0, dtype=np.int32))).long()

        # Batch vector marks which output nodes correspond to each seed.
        batch = torch.full((nodes.numel(),), -1, dtype=torch.long)
        seed_offsets = torch.as_tensor(seeds_np, dtype=torch.long)
        num_seeds = int(seed_offsets.numel())
        for seed_idx, seed_offset in enumerate(seed_offsets):
            matches = (nodes == seed_offset).nonzero(as_tuple=True)[0]
            if matches.numel() == 0:
                continue
            target = int(matches[0].item())
            if batch[target] < 0:
                batch[target] = seed_idx

        # Propagate seed ownership hop-by-hop using edge_hops to avoid assigning
        # deeper nodes to seed 0 when multiple seeds are present.
        edge_hops = torch.from_numpy(sample.get("edge_hops", np.empty(0, dtype=np.int32))).long()
        if edge_hops.numel() and row.numel():
            max_hop = int(edge_hops.max().item())
            for h in range(1, max_hop + 1):
                mask = edge_hops == h
                if not mask.any():
                    continue
                row_h = row[mask]
                col_h = col[mask]
                src_batch = batch[row_h]
                assignable = (batch[col_h] < 0) & (src_batch >= 0)
                if assignable.any():
                    batch[col_h[assignable]] = src_batch[assignable]

        # Any remaining -1 (isolated nodes) default to seed 0 (or 0 if no seeds).
        if batch.numel() and (batch < 0).any():
            batch[batch < 0] = 0

        num_sampled_nodes = torch.tensor(sample["num_sampled_nodes_per_hop"])
        num_sampled_edges = torch.tensor(sample["num_sampled_edges_per_hop"])

        rel_table_ids = torch.from_numpy(sample.get("rel_table_ids", np.empty(0, dtype=np.int64))).long()

        # If there are edge types, hand them back as the edge tensor so PyG hetero paths can route.
        edge_attr = rel_table_ids if rel_table_ids.numel() else None
        # Build hetero edge_index dict when multiple rel types exist.
        edge_index_dict = None
        if rel_table_ids.numel() and len(torch.unique(rel_table_ids)) > 1:
            edge_index_dict = build_hetero_edge_index(row, col, rel_table_ids, self.rel_id_to_edge_type)

        out = NodeSamplerOutput(
            node=nodes,
            row=row,
            col=col,
            edge=edge_attr,
            batch=batch,
            num_sampled_nodes=num_sampled_nodes,
            num_sampled_edges=num_sampled_edges,
        )
        # Add metadata so PyG NeighborLoader can map back to mini-batch inputs.
        input_id = inputs.input_id
        if input_id is None:
            input_id = torch.arange(num_seeds, dtype=torch.long)
        out.metadata = (input_id, inputs.time)
        # Expose hetero/type metadata for downstream feature fetching.
        if node_table_ids.numel():
            out.node_table_ids = node_table_ids  # type: ignore[attr-defined]
        if node_hops.numel():
            out.node_hops = node_hops  # type: ignore[attr-defined]
        if edge_index_dict is not None:
            # attach for consumers that want hetero outputs
            out.edge_index_dict = edge_index_dict  # type: ignore[attr-defined]
        return out


class DBNeighborLoader:
    """
    Minimal iterable that mirrors NeighborLoader but pulls samples via the DB sampler.
    Useful when you want a drop-in, no-materialization path without depending on
    PyG internals.
    """

    def __init__(
        self,
        feature_store,
        graph_store,
        *,
        input_nodes,
        num_neighbors,
        batch_size: int,
        shuffle: bool = False,
        replace: bool = False,
        direction: str = "fwd",
        seed: int | None = None,
    ):
        self.feature_store = feature_store
        self.graph_store = graph_store
        self.input_nodes = input_nodes
        self.batch_size = batch_size
        self.shuffle = shuffle
        node_table = input_nodes[0] if isinstance(input_nodes, tuple) else None
        conn = graph_store.connection or feature_store.connection  # type: ignore[attr-defined]
        if conn is None:
            db_obj = getattr(graph_store, "db", None) or getattr(feature_store, "db", None)  # type: ignore[attr-defined]
            num_threads = getattr(graph_store, "num_threads", None) or getattr(feature_store, "num_threads", None)  # type: ignore[attr-defined]
            if db_obj is None:
                raise ValueError("connection is required for DBNeighborLoader when no connection is attached to the stores")
            conn = Connection(db_obj, num_threads)
        self.sampler = MonadDBSampler(
            connection=conn,  # type: ignore[attr-defined]
            graph_name=graph_store.graph_name,  # type: ignore[attr-defined]
            num_neighbors=num_neighbors,
            node_table=node_table,
            replace=replace,
            direction=direction,
            seed=seed,
        )

    def __iter__(self):
        nodes = self.input_nodes[1] if isinstance(self.input_nodes, tuple) else self.input_nodes
        if isinstance(nodes, torch.Tensor):
            seeds = nodes
        else:  # assume list-like
            seeds = torch.tensor(nodes, dtype=torch.long)
        if self.shuffle:
            seeds = seeds[torch.randperm(seeds.numel())]
        for offset in range(0, seeds.numel(), self.batch_size):
            batch = seeds[offset : offset + self.batch_size]
            yield self.sampler.sample_from_nodes(
                NodeSamplerInput(
                    input_id=None,
                    node=batch,
                    input_type=self.input_nodes[0] if isinstance(self.input_nodes, tuple) else None,
                )
            )


def make_db_neighbor_loader(*args, **kwargs):
    return DBNeighborLoader(*args, **kwargs)


class _DBBaseSampler(BaseSampler):
    """Minimal BaseSampler wrapper to let PyG's NeighborLoader drive the DB sampler."""

    def __init__(self, sampler: MonadDBSampler):
        super().__init__()
        self._sampler = sampler

    def sample_from_nodes(self, inputs: NodeSamplerInput) -> NodeSamplerOutput:
        return self._sampler.sample_from_nodes(inputs)

    # Edge-based sampling is not supported in this DB sampler; define to satisfy BaseSampler.
    def sample_from_edges(self, inputs):  # pragma: no cover - edge sampling unsupported
        raise NotImplementedError("Edge-based sampling is not supported by MonadDBSampler")


class MonadNeighborLoader(NeighborLoader):
    """
    Drop-in PyG NeighborLoader that delegates sampling to the Monad DB sampler
    (edges/features stay on disk). Accepts the same args as NeighborLoader plus
    DB-specific direction/seed options.
    """

    def __init__(
        self,
        data,
        num_neighbors,
        *,
        input_nodes=None,
        batch_size: int,
        shuffle: bool = False,
        replace: bool = False,
        direction: str = "fwd",
        seed: int | None = None,
        connection: Connection | None = None,
        graph_name: str | None = None,
        time: Any | None = None,
        time_fields: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        feature_store, graph_store = data
        graph_name = graph_name or getattr(graph_store, "graph_name", None)
        if graph_name is None:
            raise ValueError("graph_name is required for MonadNeighborLoader")

        conn = connection or getattr(graph_store, "connection", None) or getattr(feature_store, "connection", None)
        if conn is None:
            db_obj = getattr(graph_store, "db", None) or getattr(feature_store, "db", None)
            num_threads = getattr(graph_store, "num_threads", None) or getattr(feature_store, "num_threads", None)
            if db_obj is None:
                raise ValueError("connection is required for MonadNeighborLoader when no connection is attached to the stores")
            conn = Connection(db_obj, num_threads)

        sampler = MonadDBSampler(
            connection=conn,
            graph_name=graph_name,
            num_neighbors=num_neighbors if isinstance(num_neighbors, list) else num_neighbors,  # type: ignore[arg-type]
            node_table=input_nodes[0] if isinstance(input_nodes, tuple) else None,
            replace=replace,
            direction=direction,
            seed=seed,
            time=time,
            time_fields=time_fields,
        )

        super().__init__(
            data=(feature_store, graph_store),
            num_neighbors=num_neighbors,
            input_nodes=input_nodes,
            batch_size=batch_size,
            shuffle=shuffle,
            replace=replace,
            neighbor_sampler=_DBBaseSampler(sampler),
            **kwargs,
        )


def make_monad_neighbor_loader(*args, **kwargs):
    return MonadNeighborLoader(*args, **kwargs)
