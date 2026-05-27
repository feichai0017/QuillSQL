<div align="center">
  <img src="assets/rust-db.png" alt="QuillSQL Logo" width="500"/>
</div>

# QuillSQL Internals

QuillSQL is now a DataFusion, Arrow, Parquet, and MLIR query compiler research
engine. DataFusion owns the SQL front end, optimizer, and physical execution
framework. QuillSQL keeps the embedding API, CLI/server surfaces, debug views,
and the JIT research boundary.

The project deliberately does not carry a custom storage engine. Interactive
queries use DataFusion memory tables; persistent analytical data should come
from Arrow/Parquet datasets and DataFusion's file/object-store integrations.

## Core Topics

- [Overall Architecture](./architecture.md): where DataFusion, Arrow/Parquet,
  and MLIR meet.
- [Module Overview](./modules/overview.md): current Rust package layout.
- [Front-Ends](./modules/bin.md): CLI and HTTP entry points.
- [Testing](./modules/tests.md): validation commands and test scope.
