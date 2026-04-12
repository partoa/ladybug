#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/table/bind_data.h"
#include "main/query_result.h"
#include "processor/result/flat_tuple.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// GOMORY_HU(relTable STRING)
// GOMORY_HU(relTable STRING, filter STRING)
// Returns: (node_u INT64, node_v INT64, cut_weight DOUBLE)
//
// Gusfield's algorithm (1990) for Gomory-Hu tree construction.
// n-1 max-flow computations. The tree encodes all pairwise min-cuts.

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

static std::vector<GHEdge> computeGomoryHuTree(main::ClientContext* context,
    const std::string& relTableName, const std::string& filterExpr) {
    // Query all edges once, cache them for rebuilding networks.
    std::string query = "MATCH (a)-[r:" + relTableName + "]->(b)";
    if (!filterExpr.empty()) {
        query += " WHERE " + filterExpr;
    }
    query += " RETURN offset(id(a)), offset(id(b))";

    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw RuntimeException("Failed to query edges: " + result->getErrorMessage());
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
    uint64_t numNodes = maxOffset + 1;

    if (numNodes <= 1 || edgeList.empty()) {
        return {};
    }

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

        auto net = buildFreshNetwork();
        double flow = net.maxFlow(t, s);
        cutValue[t] = flow;

        std::vector<uint8_t> side;
        net.minCutSides(t, side);

        for (size_t jIdx = idx + 1; jIdx < activeNodes.size(); ++jIdx) {
            uint64_t j = activeNodes[jIdx];
            if (parent[j] == s && side[j] == 0) {
                parent[j] = t;
            }
        }

        if (parent[s] < numNodes && side[parent[s]] == 0) {
            parent[t] = parent[s];
            parent[s] = t;
            cutValue[t] = cutValue[s];
            cutValue[s] = flow;
        }
    }

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

static std::unique_ptr<TableFuncBindData> gomoryHuBindImpl(
    const main::ClientContext* context, const TableFuncBindInput* input,
    const std::string& filterExpr) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto treeEdges = computeGomoryHuTree(
        const_cast<main::ClientContext*>(context), relTable, filterExpr);

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
    auto make = [](table_func_bind_t bindFn, std::vector<LogicalTypeID> params) {
        auto func = std::make_unique<TableFunction>(GomoryHuFunction::name, std::move(params));
        func->tableFunc = SimpleTableFunc::getTableFunc(gomoryHuTableFunc);
        func->bindFunc = std::move(bindFn);
        func->initSharedStateFunc = SimpleTableFunc::initSharedState;
        func->initLocalStateFunc = TableFunction::initEmptyLocalState;
        func->canParallelFunc = [] { return false; };
        return func;
    };
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) { return gomoryHuBindImpl(ctx, in, ""); },
        {LogicalTypeID::STRING}));
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) {
            return gomoryHuBindImpl(ctx, in, in->getLiteralVal<std::string>(1));
        },
        {LogicalTypeID::STRING, LogicalTypeID::STRING}));
    return result;
}

} // namespace function
} // namespace lbug
