<div align="center">
  <img src="assets/rust-db.png" alt="QuillSQL Logo" width="500"/>
</div>

# QuillSQL Internals

QuillSQL is now a frontend-agnostic Arrow and MLIR query compiler research
engine. DataFusion is the first complete frontend adapter and still powers the
CLI/server SQL path. QuillSQL keeps the embedding API, debug views, neutral
`PipelineGraph`, Arrow runtime, and JIT research boundary.

The project deliberately does not carry a custom storage engine. Interactive
queries use DataFusion memory tables; persistent analytical data should come
from Arrow/Parquet datasets and DataFusion's file/object-store integrations.

## Core Topics

- [Overall Architecture](./architecture.md): where frontend adapters, Arrow, and
  MLIR meet.
- [Module Overview](./modules/overview.md): current Rust package layout.
- [Front-Ends](./modules/bin.md): CLI and HTTP entry points.
- [Testing](./modules/tests.md): validation commands and test scope.
