# Rust API Patch: Zero-Copy Arrow Import

These files need to be copied into `partoa/ladybug-rust` to add zero-copy
Arrow RecordBatch import support.

## Files to copy

```
rust_api_patch/include/lbug_arrow.h  ->  include/lbug_arrow.h  (replace)
rust_api_patch/src/lbug_arrow.cpp    ->  src/lbug_arrow.cpp    (replace)
rust_api_patch/src/ffi/arrow.rs      ->  src/ffi/arrow.rs      (replace)
rust_api_patch/src/connection.rs     ->  src/connection.rs     (replace)
```

**Note:** `connection.rs` in this patch is the API-only portion (no tests).
The full file with tests is in the local submodule at `tools/rust_api/`.

## Quick apply

```bash
cd /path/to/ladybug-rust
git clone -b claude/kuzudb-arrow-cpg-M5cHs --depth 1 https://github.com/partoa/ladybug /tmp/ladybug-patch

cp /tmp/ladybug-patch/rust_api_patch/include/lbug_arrow.h    include/lbug_arrow.h
cp /tmp/ladybug-patch/rust_api_patch/src/lbug_arrow.cpp      src/lbug_arrow.cpp
cp /tmp/ladybug-patch/rust_api_patch/src/ffi/arrow.rs        src/ffi/arrow.rs
cp /tmp/ladybug-patch/rust_api_patch/src/connection.rs       src/connection.rs

git add -A
git commit -m \"feat: zero-copy Arrow import from Rust\"
git push origin main
```

## Full API

### Node tables
- `create_node_table_from_arrow(name, &RecordBatch)` - zero-copy read-only table
- `copy_node_table_from_arrow(name, &RecordBatch)` - bulk insert into existing table
- `insert_arrow(name, &RecordBatch)` - bulk insert (Rust-side query construction)
- `upsert_arrow(name, &RecordBatch)` - MERGE-based upsert (respects primary keys)

### Relationship tables
- `create_rel_table_from_arrow(rel, from, to, &RecordBatch)` - zero-copy REL table
- `copy_rel_table_from_arrow(rel, from, to, &RecordBatch)` - bulk insert REL data

### Cleanup
- `drop_arrow_table(name)` - drop arrow-backed table, release memory

All require `features = [\"arrow\"]` in Cargo.toml. All 165 tests pass.
