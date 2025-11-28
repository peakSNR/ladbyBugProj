from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, Tuple

import torch


def build_hetero_edge_index(
    rows: torch.Tensor,
    cols: torch.Tensor,
    rel_table_ids: torch.Tensor,
    rel_id_to_edge_type: Dict[int, Tuple[str, str, str]],
) -> Dict[Tuple[str, str, str], torch.Tensor]:
    """
    Construct a hetero edge_index dict from flat sampled edges.

    Parameters
    ----------
    rows : torch.Tensor
        Source local ids of sampled edges.
    cols : torch.Tensor
        Destination local ids of sampled edges.
    rel_table_ids : torch.Tensor
        Relationship table ids aligned with rows/cols.
    rel_id_to_edge_type : mapping[int, (src_type, rel_name, dst_type)]
        Lookup from rel table id (oid) to PyG edge_type tuple.
    """
    edge_index_dict = {}
    if rel_table_ids.numel() == 0:
        return edge_index_dict

    # Find all unique relation IDs present in this batch
    unique_rels = torch.unique(rel_table_ids)

    for rel_id_tensor in unique_rels:
        rel_id = int(rel_id_tensor.item())
        etype = rel_id_to_edge_type.get(rel_id)

        if etype is None:
            continue

        # Boolean mask for this relation type
        mask = rel_table_ids == rel_id

        # Filter rows and cols
        r = rows[mask]
        c = cols[mask]

        # Stack to create [2, num_edges] edge_index
        edge_index_dict[etype] = torch.stack([r, c], dim=0)

    return edge_index_dict
