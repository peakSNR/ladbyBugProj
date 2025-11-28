# Monad

**High-performance, embedded graph database engine powering Avra's Large Knowledge Graph.**

> The atomic unit of our GFM.

## Overview

Monad is a specialized graph database engine designed to serve as the high-performance backbone for Avra's Large Knowledge Graph and Graph Foundation Models (GFM). It is built as a fork of [Monad DB](https://github.com/MonadDB/monad) (itself a fork of [Kuzu](https://github.com/kuzudb/kuzu)), inheriting their columnar storage and vectorized processing capabilities while introducing critical optimizations for large-scale machine learning workloads.

## Key Features

Monad extends the core capabilities of Kuzu/Monad with a specific focus on **Machine Learning Operations (MLOps)** and **Distributed Training**:

- **High-Performance Neighbor Sampling**: A custom, highly optimized C++ neighbor sampler designed to feed distributed training pipelines. It pushes sampling logic (Reservoir Sampling, Temporal Filtering) down to the storage engine, minimizing data transfer and CPU overhead.
- **PyTorch Geometric (PyG) Integration**: Native support for `torch_geometric`, allowing seamless integration with GNN architectures using standard loaders (`NeighborLoader`) and data structures.
- **Temporal Graph Learning**: Built-in support for `as_of` queries and time-window filtering (`validAt`) directly in the sampling engine, enabling correct temporal graph learning without data leakage.
- **Distributed Training Ready**: Designed to be deployed as Ray Actors, supporting efficient, thread-safe, and scalable data loading across distributed workers.

## Core Foundation

Inherited from Kuzu and Monad, Monad retains:
- **Columnar Disk-Based Storage**: Efficient storage for property graphs.
- **Vectorized Query Processor**: Fast execution of analytical queries.
- **Cypher Support**: Flexible property graph data model and query language.
- **Embeddable Architecture**: Serverless integration directly into application processes.
- Native full text search and vector index
- Columnar sparse row-based (CSR) adjacency list/join indices
- Novel and very fast join algorithms
- Multi-core query parallelism
- Serializable ACID transactions
- Wasm (WebAssembly) bindings for fast, secure execution in the browser

## Installation & Usage

*Note: Monad is a private internal project at Avra.*

### Building from Source

To build from source code, Monad requires CMake (`>=3.15`), Python (`>=3.9`), and a compiler that supports C++20. The minimum supported versions of C++ compilers are GCC 12, Clang 18, and MSVC 19.20. The preferred compiler on Linux is GCC; on macOS, Apple Clang; and on Windows, MSVC. On Linux, Clang is also tested. Other compilers that support C++20 may also work, but are not tested.

To build the Python bindings with the custom neighbor sampler:

```bash
make release NUM_THREADS=$(sysctl -n hw.physicalcpu)
```

```bash
make GEN=Ninja python
```

### Python Usage (PyG)

```python
from monad import Connection, MonadNeighborLoader
import torch

# Initialize connection
conn = Connection(db_path="./monad_db")

# Create a PyG compatible loader backed by Monad
loader = MonadNeighborLoader(
    data=(feature_store, graph_store),
    num_neighbors=[10, 10],
    batch_size=1024,
    input_nodes=('paper', torch.arange(1000)),
    connection=conn,
    graph_name="my_graph",
    direction="fwd",  # or "bwd", "both"
    time=None         # Optional: timestamp for temporal sampling
)

for batch in loader:
    # batch is a standard PyG HeteroData object
    train(batch)
```

## Run Tests

### C/C++ tests

```bash
make test NUM_THREADS=X
```

For additional information regarding the tests, please refer to the documentation for Testing Framework.

### Increase `ulimit` for running tests

For some platforms, such as macOS, the default limit for the number of open files is too low for running tests, which may cause some tests to fail. To increase the limit, please run the following command before running tests.

```bash
ulimit -n 10000
```

## License

Monad is proprietary software developed by Avra.
Portions of this codebase are derived from Monad and Kuzu, licensed under the MIT License.
