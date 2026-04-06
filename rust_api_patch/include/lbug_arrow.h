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

rust::String create_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array);

void drop_arrow_table(lbug::main::Connection& connection, rust::Str table_name);

void copy_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array);

rust::String create_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array);

void copy_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array);

rust::String register_arrow_data(ArrowSchema schema, ArrowArray array);

void unregister_arrow_data(rust::Str arrow_id);

} // namespace lbug_arrow
