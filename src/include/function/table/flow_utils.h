#pragma once

#include "function/table/flow_graph.h"
#include "main/client_context.h"

#include <string>

namespace lbug {
namespace function {

// Build a FlowNetwork from a relationship table. Each matching edge gets capacity 1.0.
// filterExpr is an optional Cypher predicate on `r` (the relationship variable).
// Examples: "r.weight > 0.5", "r.instance_id = 42 AND r.channel = 3", ""
FlowNetwork buildFlowNetworkFromQuery(main::ClientContext* context,
    const std::string& relTableName, const std::string& filterExpr = "");

// Build a weighted edge list from a relationship table. Used for cycle detection.
std::vector<WeightedEdge> buildEdgeListFromQuery(main::ClientContext* context,
    const std::string& relTableName, uint64_t& outMaxOffset,
    const std::string& filterExpr = "");

} // namespace function
} // namespace lbug
