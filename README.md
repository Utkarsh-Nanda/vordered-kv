# Multi-Versioning Ordered Key-Value Stores with Persistent Memory Support

Dependencies:
- PMDK
- libpmemobj-cpp

Optional depenencies for benchmarks:
- SQLite3
- RocksDB
- MPI

Approaches:
- Ephemeral skip list
- Hybrid ephemeral skip list and compact persistent memory key chain
- SQLite frontend (versioning implemented as an indexed column)
- RocksDB frontend (versioning implemented natively using timestamps)

Notes:
- all primitives are thread-safe
