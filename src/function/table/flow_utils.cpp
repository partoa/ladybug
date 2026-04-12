#include "function/table/flow_utils.h"

#include "common/exception/runtime.h"
#include "main/query_result.h"
#include "processor/result/flat_tuple.h"

namespace lbug {
namespace function {

// Build a Cypher WHERE clause from the edge filter.
// Returns "" if no filters, or " WHERE r.instance_id = 42 AND r.field_name_id = 7" etc.
static std::string buildFilterClause(const EdgeFilter& filter) {
    std::vector<std::string> conditions;
    if (filter.instanceId.has_value()) {
        conditions.push_back("r.instance_id = " + std::to_string(*filter.instanceId));
    }
    if (filter.fieldNameId.has_value()) {
        conditions.push_back("r.field_name_id = " + std::to_string(*filter.fieldNameId));
    }
    if (filter.callSiteId.has_value()) {
        conditions.push_back("r.call_site_id = " + std::to_string(*filter.callSiteId));
    }
    if (conditions.empty()) {
        return "";
    }
    std::string clause = " WHERE ";
    for (size_t i = 0; i < conditions.size(); ++i) {
        if (i > 0) {
            clause += " AND ";
        }
        clause += conditions[i];
    }
    return clause;
}

FlowNetwork buildFlowNetworkFromQuery(main::ClientContext* context,
    const std::string& relTableName, const EdgeFilter& filter) {
    auto whereClause = buildFilterClause(filter);
    auto query = "MATCH (a)-[r:" + relTableName + "]->(b)" + whereClause +
                 " RETURN offset(id(a)), offset(id(b))";
    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw common::RuntimeException("Failed to query edges from '" + relTableName + "': " +
                                       result->getErrorMessage());
    }

    uint64_t maxOffset = 0;
    std::vector<std::pair<uint64_t, uint64_t>> edges;
    while (result->hasNext()) {
        auto tuple = result->getNext();
        auto srcOffset = tuple->getValue(0)->getValue<int64_t>();
        auto dstOffset = tuple->getValue(1)->getValue<int64_t>();
        edges.emplace_back(static_cast<uint64_t>(srcOffset), static_cast<uint64_t>(dstOffset));
        maxOffset = std::max(maxOffset, static_cast<uint64_t>(srcOffset));
        maxOffset = std::max(maxOffset, static_cast<uint64_t>(dstOffset));
    }

    FlowNetwork net(maxOffset + 1);
    for (auto [src, dst] : edges) {
        net.addEdge(src, dst, 1.0);
    }
    return net;
}

std::vector<WeightedEdge> buildEdgeListFromQuery(main::ClientContext* context,
    const std::string& relTableName, uint64_t& outMaxOffset, const EdgeFilter& filter) {
    auto whereClause = buildFilterClause(filter);
    auto query = "MATCH (a)-[r:" + relTableName + "]->(b)" + whereClause +
                 " RETURN offset(id(a)), offset(id(b))";
    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw common::RuntimeException("Failed to query edges from '" + relTableName + "': " +
                                       result->getErrorMessage());
    }

    outMaxOffset = 0;
    std::vector<WeightedEdge> edges;
    while (result->hasNext()) {
        auto tuple = result->getNext();
        auto srcOffset = tuple->getValue(0)->getValue<int64_t>();
        auto dstOffset = tuple->getValue(1)->getValue<int64_t>();
        auto src = static_cast<uint64_t>(srcOffset);
        auto dst = static_cast<uint64_t>(dstOffset);
        edges.push_back({src, dst, 1.0});
        outMaxOffset = std::max(outMaxOffset, src);
        outMaxOffset = std::max(outMaxOffset, dst);
    }
    return edges;
}

} // namespace function
} // namespace lbug
