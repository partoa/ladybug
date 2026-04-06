#pragma once

#include "rust/cxx.h"
#ifdef LBUG_BUNDLED
#include "main/lbug.h"
#include "storage/table/arrow_table_support.h"
#else
#include <lbug.hpp>
#endif

namespace lbug_arrow {

ArrowSchema query_result_get_arrow_schema(const lbug::main::QueryResult& result);
ArrowArray query_result_get_next_arrow_chunk(lbug::main::QueryResult& result, uint64_t chunkSize);

// Zero-copy Arrow import: create a node table backed by in-memory Arrow data.
rust::String create_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array);

// Unregister (drop) an arrow-backed table.
void drop_arrow_table(lbug::main::Connection& connection, rust::Str table_name);

// Bulk insert Arrow data into an existing node table.
void copy_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array);

// Create a REL table backed by in-memory Arrow data (zero-copy).
rust::String create_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array);

// Bulk insert Arrow data into an existing REL table.
void copy_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array);

} // namespace lbug_arrow
