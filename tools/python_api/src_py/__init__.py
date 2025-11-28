from .async_connection import AsyncConnection
from .connection import Connection
from .database import Database
from .prepared_statement import PreparedStatement
from .query_result import QueryResult
from .types import Type

# Optional Torch Geometric integration; keep import lazy/failable so base package
# works without heavy torch deps.
try:  # pragma: no cover - exercised when torch is available
    from .torch_geometric_feature_store import MonadFeatureStore
    from .torch_geometric_graph_store import MonadGraphStore
    from .torch_geometric_db_sampler import (
        MonadDBSampler,
        MonadNeighborLoader,
        make_db_neighbor_loader,
        make_monad_neighbor_loader,
    )
except Exception:  # noqa: BLE001 - surface torch dependency issues lazily
    MonadFeatureStore = None  # type: ignore[assignment]
    MonadGraphStore = None  # type: ignore[assignment]
    MonadDBSampler = None  # type: ignore[assignment]
    MonadNeighborLoader = None  # type: ignore[assignment]
    make_db_neighbor_loader = None  # type: ignore[assignment]
    make_monad_neighbor_loader = None  # type: ignore[assignment]

# Public version helpers
version = Database.get_version()
__version__ = version
storage_version = Database.get_storage_version()

__all__ = [
    "AsyncConnection",
    "Connection",
    "Database",
    "PreparedStatement",
    "QueryResult",
    "Type",
    "version",
    "__version__",
    "storage_version",
    "MonadFeatureStore",
    "MonadGraphStore",
    "MonadDBSampler",
    "MonadNeighborLoader",
    "make_db_neighbor_loader",
    "make_monad_neighbor_loader",
    "bench",
]
