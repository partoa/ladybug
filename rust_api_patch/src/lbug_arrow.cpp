#include "lbug_arrow.h"

namespace lbug_arrow {

ArrowSchema query_result_get_arrow_schema(const lbug::main::QueryResult& result) {
    return *result.getArrowSchema();
}

ArrowArray query_result_get_next_arrow_chunk(lbug::main::QueryResult& result, uint64_t chunkSize) {
    return *result.getNextArrowChunk(chunkSize);
}

rust::String create_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array) {
    ArrowSchemaWrapper schemaWrapper;
    static_cast<ArrowSchema&>(schemaWrapper) = schema;
    schema.release = nullptr;
    ArrowArrayWrapper arrayWrapper;
    static_cast<ArrowArray&>(arrayWrapper) = array;
    array.release = nullptr;
    std::vector<ArrowArrayWrapper> arrays;
    arrays.push_back(std::move(arrayWrapper));
    std::string name(table_name.data(), table_name.size());
    auto result = lbug::ArrowTableSupport::createViewFromArrowTable(
        connection, name, std::move(schemaWrapper), std::move(arrays));
    if (!result.queryResult->isSuccess()) {
        throw std::runtime_error(result.queryResult->getErrorMessage());
    }
    return rust::String(result.arrowId);
}

void drop_arrow_table(lbug::main::Connection& connection, rust::Str table_name) {
    std::string name(table_name.data(), table_name.size());
    auto result = lbug::ArrowTableSupport::unregisterArrowTable(connection, name);
    if (!result->isSuccess()) {
        throw std::runtime_error(result->getErrorMessage());
    }
}

static std::pair<ArrowSchemaWrapper, std::vector<ArrowArrayWrapper>>
wrapArrowData(ArrowSchema& schema, ArrowArray& array) {
    ArrowSchemaWrapper sw;
    static_cast<ArrowSchema&>(sw) = schema;
    schema.release = nullptr;
    ArrowArrayWrapper aw;
    static_cast<ArrowArray&>(aw) = array;
    array.release = nullptr;
    std::vector<ArrowArrayWrapper> arrays;
    arrays.push_back(std::move(aw));
    return {std::move(sw), std::move(arrays)};
}

static std::string buildColumnList(const ArrowSchemaWrapper& schema, const std::string& alias) {
    std::string cols;
    for (int64_t i = 0; i < schema.n_children; ++i) {
        if (i > 0) cols += ", ";
        cols += alias + "." + schema.children[i]->name;
    }
    return cols;
}

void copy_node_table_from_arrow(lbug::main::Connection& connection,
    rust::Str table_name, ArrowSchema schema, ArrowArray array) {
    auto [sw, arrays] = wrapArrowData(schema, array);
    std::string colList = buildColumnList(sw, "t");
    std::string targetName(table_name.data(), table_name.size());
    std::string tempName = "_arrow_copy_tmp_" + targetName;
    auto createResult = lbug::ArrowTableSupport::createViewFromArrowTable(
        connection, tempName, std::move(sw), std::move(arrays));
    if (!createResult.queryResult->isSuccess()) {
        throw std::runtime_error(createResult.queryResult->getErrorMessage());
    }
    std::string copyQuery =
        "COPY " + targetName + " FROM (MATCH (t:" + tempName + ") RETURN " + colList + ")";
    auto copyResult = connection.query(copyQuery);
    lbug::ArrowTableSupport::unregisterArrowTable(connection, tempName);
    if (!copyResult->isSuccess()) {
        throw std::runtime_error(copyResult->getErrorMessage());
    }
}

rust::String create_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array) {
    auto [sw, arrays] = wrapArrowData(schema, array);
    std::string relName(rel_table_name.data(), rel_table_name.size());
    std::string fromName(from_table_name.data(), from_table_name.size());
    std::string toName(to_table_name.data(), to_table_name.size());
    auto result = lbug::ArrowTableSupport::createRelTableFromArrowTable(
        connection, relName, fromName, toName, std::move(sw), std::move(arrays));
    if (!result.queryResult->isSuccess()) {
        lbug::ArrowTableSupport::unregisterArrowData(result.arrowId);
        throw std::runtime_error(result.queryResult->getErrorMessage());
    }
    return rust::String(result.arrowId);
}

void copy_rel_table_from_arrow(lbug::main::Connection& connection,
    rust::Str rel_table_name, rust::Str from_table_name, rust::Str to_table_name,
    ArrowSchema schema, ArrowArray array) {
    auto [sw, arrays] = wrapArrowData(schema, array);
    std::string colList = buildColumnList(sw, "t");
    std::string relName(rel_table_name.data(), rel_table_name.size());
    std::string tempName = "_arrow_copy_tmp_" + relName;
    auto createResult = lbug::ArrowTableSupport::createViewFromArrowTable(
        connection, tempName, std::move(sw), std::move(arrays));
    if (!createResult.queryResult->isSuccess()) {
        throw std::runtime_error(createResult.queryResult->getErrorMessage());
    }
    std::string copyQuery =
        "COPY " + relName + " FROM (MATCH (t:" + tempName + ") RETURN " + colList + ")";
    auto copyResult = connection.query(copyQuery);
    lbug::ArrowTableSupport::unregisterArrowTable(connection, tempName);
    if (!copyResult->isSuccess()) {
        throw std::runtime_error(copyResult->getErrorMessage());
    }
}

rust::String register_arrow_data(ArrowSchema schema, ArrowArray array) {
    auto [sw, arrays] = wrapArrowData(schema, array);
    auto id = lbug::ArrowTableSupport::registerArrowData(std::move(sw), std::move(arrays));
    return rust::String(id);
}

void unregister_arrow_data(rust::Str arrow_id) {
    std::string id(arrow_id.data(), arrow_id.size());
    lbug::ArrowTableSupport::unregisterArrowData(id);
}

} // namespace lbug_arrow
