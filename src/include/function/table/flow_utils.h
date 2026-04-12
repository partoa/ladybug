#pragma once

#include "function/table/flow_graph.h"
#include "main/client_context.h"

#include <optional>
#include <string>

namespace lbug {
namespace function {

// Optional edge filters for instance-aware flow analysis.
// When set, only edges matching the filter are included in the flow network.
struct EdgeFilter {
    std::optional<int64_t> instanceId;  // filter by allocation site / object instance
    std::optional<int64_t> fieldNameId; // filter by field identifier
    std::optional<int64_t> callSiteId;  // filter by call site
};

// Build a FlowNetwork by querying all edges from a relationship table.
// Each matching edge gets capacity 1.0.
// If filter is provided, only edges with matching property values are included.
// The filter columns must exist on the relationship table.
FlowNetwork buildFlowNetworkFromQuery(main::ClientContext* context,
    const std::string& relTableName, const EdgeFilter& filter = {});

// Build a weighted edge list by querying all edges from a relationship table.
// Used for cycle detection.
std::vector<WeightedEdge> buildEdgeListFromQuery(main::ClientContext* context,
    const std::string& relTableName, uint64_t& outMaxOffset,
    const EdgeFilter& filter = {});

} // namespace function
} // namespace lbug
