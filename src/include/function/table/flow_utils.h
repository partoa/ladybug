#pragma once

#include "function/table/flow_graph.h"
#include "main/client_context.h"

namespace lbug {
namespace function {

// Build a FlowNetwork by querying all edges from a relationship table.
// Each edge gets capacity 1.0.
FlowNetwork buildFlowNetworkFromQuery(main::ClientContext* context,
    const std::string& relTableName);

// Build a weighted edge list by querying all edges from a relationship table.
// Used for cycle detection.
std::vector<WeightedEdge> buildEdgeListFromQuery(main::ClientContext* context,
    const std::string& relTableName, uint64_t& outMaxOffset);

} // namespace function
} // namespace lbug
