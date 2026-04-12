#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"
#include "main/query_result.h"
#include "processor/result/flat_tuple.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════════
// GOMORY_HU(relTable STRING)
// Returns: (node_u INT64, node_v INT64, cut_weight DOUBLE)
//
// Computes the Gomory-Hu tree using Gusfield's algorithm (1990).
// Returns n-1 tree edges. The min-cut between any pair (a,b) equals
// the minimum cut_weight on the path from a to b in this tree.
//
// Gusfield's algorithm: n-1 max-flow computations on the UNDIRECTED
// version of the graph (each directed edge becomes two undirected edges).
//
// Usage: CALL gomory_hu('ValueFlow') RETURN node_u, node_v, cut_weight
//        ORDER BY cut_weight ASC;
// ═══════════════════════════════════════════════════════════════════

struct GHEdge {
    uint64_t u;
    uint64_t v;
    double weight;
};

struct GomoryHuBindData final : TableFuncBindData {
    std::vector<GHEdge> treeEdges;

    GomoryHuBindData(std::vector<GHEdge> treeEdges, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(treeEdges.size())},
          treeEdges{std::move(treeEdges)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GomoryHuBindData>(treeEdges, columns);
    }
};

// Build undirected flow network: for each directed edge, add both directions.
static FlowNetwork buildUndirectedFlowNetwork(main::ClientContext* context,
    const std::string& relTableName, uint64_t& numNodes) {
    auto query = "MATCH (a)-[:" + relTableName + "]->(b) "
                 "RETURN offset(id(a)), offset(id(b))";
    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw common::RuntimeException("Failed to query edges: " + result->getErrorMessage());
    }

    uint64_t maxOffset = 0;
    std::vector<std::pair<uint64_t, uint64_t>> edges;
    while (result->hasNext()) {
        auto tuple = result->getNext();
        auto src = static_cast<uint64_t>(tuple->getValue(0)->getValue<int64_t>());
        auto dst = static_cast<uint64_t>(tuple->getValue(1)->getValue<int64_t>());
        edges.emplace_back(src, dst);
        maxOffset = std::max(maxOffset, src);
        maxOffset = std::max(maxOffset, dst);
    }

    numNodes = maxOffset + 1;
    // For Gomory-Hu on directed graphs, we treat each directed edge as undirected
    // by adding both directions.
    FlowNetwork net(numNodes);
    for (auto [src, dst] : edges) {
        net.addEdge(src, dst, 1.0);
        net.addEdge(dst, src, 1.0);
    }
    return net;
}

// Gusfield's algorithm for Gomory-Hu tree construction.
// Algorithm 3.8 from Williamson's "Network Flow Algorithms" (2019).
static std::vector<GHEdge> computeGomoryHuTree(main::ClientContext* context,
    const std::string& relTableName) {

    uint64_t numNodes = 0;
    // We need to know the edges to rebuild the network for each max-flow call.
    auto query = "MATCH (a)-[:" + relTableName + "]->(b) "
                 "RETURN offset(id(a)), offset(id(b))";
    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw common::RuntimeException("Failed to query edges: " + result->getErrorMessage());
    }

    uint64_t maxOffset = 0;
    std::vector<std::pair<uint64_t, uint64_t>> edgeList;
    while (result->hasNext()) {
        auto tuple = result->getNext();
        auto src = static_cast<uint64_t>(tuple->getValue(0)->getValue<int64_t>());
        auto dst = static_cast<uint64_t>(tuple->getValue(1)->getValue<int64_t>());
        edgeList.emplace_back(src, dst);
        maxOffset = std::max(maxOffset, src);
        maxOffset = std::max(maxOffset, dst);
    }
    numNodes = maxOffset + 1;

    if (numNodes <= 1) {
        return {};
    }

    // Collect actual node IDs that participate in edges.
    std::vector<bool> nodeExists(numNodes, false);
    for (auto [src, dst] : edgeList) {
        nodeExists[src] = true;
        nodeExists[dst] = true;
    }
    std::vector<uint64_t> activeNodes;
    for (uint64_t i = 0; i < numNodes; ++i) {
        if (nodeExists[i]) {
            activeNodes.push_back(i);
        }
    }
    if (activeNodes.size() <= 1) {
        return {};
    }

    // Gusfield's algorithm:
    // parent[i] = parent of node i in the tree (initially all point to activeNodes[0])
    // cutValue[i] = min-cut value between i and parent[i]
    std::vector<uint64_t> parent(numNodes, activeNodes[0]);
    std::vector<double> cutValue(numNodes, 0.0);

    auto buildFreshNetwork = [&]() -> FlowNetwork {
        FlowNetwork net(numNodes);
        for (auto [src, dst] : edgeList) {
            net.addEdge(src, dst, 1.0);
            net.addEdge(dst, src, 1.0);
        }
        return net;
    };

    for (size_t idx = 1; idx < activeNodes.size(); ++idx) {
        uint64_t t = activeNodes[idx];
        uint64_t s = parent[t];

        // Compute max-flow / min-cut between t and s.
        auto net = buildFreshNetwork();
        double flow = net.maxFlow(t, s);
        cutValue[t] = flow;

        // Find which nodes are on t's side of the cut.
        std::vector<uint8_t> side;
        net.minCutSides(t, side);

        // Gusfield's parent reassignment:
        // For all nodes j > t in the active list, if parent[j] == s and j is on t's side,
        // reassign parent[j] = t.
        for (size_t jIdx = idx + 1; jIdx < activeNodes.size(); ++jIdx) {
            uint64_t j = activeNodes[jIdx];
            if (parent[j] == s && side[j] == 0) {
                parent[j] = t;
            }
        }

        // If parent[s] is on t's side, swap the parent relationship.
        if (parent[s] < numNodes && side[parent[s]] == 0) {
            parent[t] = parent[s];
            parent[s] = t;
            cutValue[t] = cutValue[s];
            cutValue[s] = flow;
        }
    }

    // Build tree edges from parent pointers.
    std::vector<GHEdge> treeEdges;
    for (auto node : activeNodes) {
        if (node != activeNodes[0] && parent[node] != node) {
            treeEdges.push_back({node, parent[node], cutValue[node]});
        }
    }
    return treeEdges;
}

static offset_t gomoryHuTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bindData = input.bindData->constPtrCast<GomoryHuBindData>();
    auto numToOutput = morsel.getMorselSize();
    for (auto i = 0u; i < numToOutput; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i,
            static_cast<int64_t>(bindData->treeEdges[idx].u));
        output.getValueVectorMutable(1).setValue<int64_t>(i,
            static_cast<int64_t>(bindData->treeEdges[idx].v));
        output.getValueVectorMutable(2).setValue<double>(i,
            bindData->treeEdges[idx].weight);
    }
    return numToOutput;
}

static std::unique_ptr<TableFuncBindData> gomoryHuBindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
    auto relTable = input->getLiteralVal<std::string>(0);

    auto treeEdges = computeGomoryHuTree(
        const_cast<main::ClientContext*>(context), relTable);

    std::vector<std::string> columnNames = {"node_u", "node_v", "cut_weight"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::DOUBLE());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<GomoryHuBindData>(std::move(treeEdges), columns);
}

function_set GomoryHuFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->tableFunc = SimpleTableFunc::getTableFunc(gomoryHuTableFunc);
    func->bindFunc = gomoryHuBindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

} // namespace function
} // namespace lbug
