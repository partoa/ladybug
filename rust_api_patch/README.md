# Rust API Patch: Zero-Copy Arrow Import

These files need to be copied into `partoa/ladybug-rust` to add zero-copy
Arrow RecordBatch import support.

## Files to copy

```
rust_api_patch/include/lbug_arrow.h  →  include/lbug_arrow.h  (replace)
rust_api_patch/src/lbug_arrow.cpp    →  src/lbug_arrow.cpp    (replace)
rust_api_patch/src/ffi/arrow.rs      →  src/ffi/arrow.rs      (replace)
rust_api_patch/src/connection.rs     →  src/connection.rs     (replace)
```

## Quick apply

```bash
cd /path/to/ladybug-rust
cp ../ladybug/rust_api_patch/include/lbug_arrow.h include/
cp ../ladybug/rust_api_patch/src/lbug_arrow.cpp src/
cp ../ladybug/rust_api_patch/src/ffi/arrow.rs src/ffi/
cp ../ladybug/rust_api_patch/src/connection.rs src/
git add -A && git commit -m "feat: zero-copy Arrow import from Rust"
git push origin main
```

## What this adds

- `Connection::create_node_table_from_arrow(name, &RecordBatch)` — zero-copy read-only table
- `Connection::insert_arrow(name, &RecordBatch)` — bulk insert into existing table
- `Connection::upsert_arrow(name, &RecordBatch)` — MERGE-based upsert (respects primary keys)
- `Connection::drop_arrow_table(name)` — cleanup
