#include "function/table/flow_utils.h"

#include "common/exception/runtime.h"
#include "main/query_result.h"
#include "processor/result/flat_tuple.h"

namespace lbug {
namespace function {

FlowNetwork buildFlowNetworkFromQuery(main::ClientContext* context,
    const std::string& relTableName, const std::string& filterExpr) {
    std::string query = "MATCH (a)-[r:" + relTableName + "]->(b)";
    if (!filterExpr.empty()) {
        query += " WHERE " + filterExpr;
    }
    query += " RETURN offset(id(a)), offset(id(b))";

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
    const std::string& relTableName, uint64_t& outMaxOffset,
    const std::string& filterExpr) {
    std::string query = "MATCH (a)-[r:" + relTableName + "]->(b)";
    if (!filterExpr.empty()) {
        query += " WHERE " + filterExpr;
    }
    query += " RETURN offset(id(a)), offset(id(b))";

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
