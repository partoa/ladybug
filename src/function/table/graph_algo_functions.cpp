#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"
#include "function/table/graph_algorithms.h"

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/table/bind_data.h"
#include "main/client_context.h"
#include "main/query_result.h"
#include "processor/result/flat_tuple.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// Shared helper: build AdjGraph from a relationship table
static AdjGraph buildAdjGraphFromQuery(main::ClientContext* context,
    const std::string& relTableName, const std::string& filterExpr = "") {
    std::string query = "MATCH (a)-[r:" + relTableName + "]->(b)";
    if (!filterExpr.empty()) query += " WHERE " + filterExpr;
    query += " RETURN offset(id(a)), offset(id(b))";

    auto result = context->query(query);
    if (!result->isSuccess()) {
        throw RuntimeException("Failed to query edges from '" + relTableName + "': " +
                               result->getErrorMessage());
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

    AdjGraph g(maxOffset + 1);
    for (auto [src, dst] : edges) {
        g.addEdge(src, dst, 1.0);
    }
    return g;
}

// ═══════════════════════════════════════════════════════════════
// DOMINATORS(relTable STRING, entry INT64)
// Returns: (node_offset INT64, idom_offset INT64, depth INT64)
// ═══════════════════════════════════════════════════════════════

struct DomBindData final : TableFuncBindData {
    struct Row { int64_t node; int64_t idom; int64_t depth; };
    std::vector<Row> rows;
    DomBindData(std::vector<Row> rows, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(rows.size())},
          rows{std::move(rows)} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DomBindData>(rows, columns);
    }
};

static offset_t domTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<DomBindData>();
    auto n = morsel.getMorselSize();
    for (auto i = 0u; i < n; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i, bd->rows[idx].node);
        output.getValueVectorMutable(1).setValue<int64_t>(i, bd->rows[idx].idom);
        output.getValueVectorMutable(2).setValue<int64_t>(i, bd->rows[idx].depth);
    }
    return n;
}

static std::unique_ptr<TableFuncBindData> domBindImpl(
    const main::ClientContext* ctx, const TableFuncBindInput* input,
    const std::string& filter) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto entry = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
    auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable, filter);
    auto dom = computeDominators(g, entry);

    // Compute depths
    std::vector<int64_t> depth(g.numNodes, -1);
    depth[entry] = 0;
    bool changed = true;
    while (changed) {
        changed = false;
        for (uint64_t i = 0; i < g.numNodes; ++i) {
            if (dom.idom[i] >= 0 && depth[dom.idom[i]] >= 0 && depth[i] < 0) {
                depth[i] = depth[dom.idom[i]] + 1;
                changed = true;
            }
        }
    }

    std::vector<DomBindData::Row> rows;
    for (uint64_t i = 0; i < g.numNodes; ++i) {
        if (dom.idom[i] >= 0 || i == entry) {
            rows.push_back({static_cast<int64_t>(i), dom.idom[i], depth[i]});
        }
    }

    std::vector<std::string> colNames = {"node_offset", "idom_offset", "depth"};
    std::vector<LogicalType> colTypes;
    colTypes.emplace_back(LogicalType::INT64());
    colTypes.emplace_back(LogicalType::INT64());
    colTypes.emplace_back(LogicalType::INT64());
    colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
    auto columns = input->binder->createVariables(colNames, colTypes);
    return std::make_unique<DomBindData>(std::move(rows), columns);
}

function_set DominatorsFunction::getFunctionSet() {
    function_set result;
    auto make = [](table_func_bind_t bindFn, std::vector<LogicalTypeID> params) {
        auto func = std::make_unique<TableFunction>(DominatorsFunction::name, std::move(params));
        func->tableFunc = SimpleTableFunc::getTableFunc(domTableFunc);
        func->bindFunc = std::move(bindFn);
        func->initSharedStateFunc = SimpleTableFunc::initSharedState;
        func->initLocalStateFunc = TableFunction::initEmptyLocalState;
        func->canParallelFunc = [] { return false; };
        return func;
    };
    result.push_back(make(
        [](const main::ClientContext* c, const TableFuncBindInput* i) { return domBindImpl(c, i, ""); },
        {LogicalTypeID::STRING, LogicalTypeID::INT64}));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// FEEDBACK_ARC_SET(relTable STRING)
// Returns: (src_offset INT64, dst_offset INT64)
// ═══════════════════════════════════════════════════════════════

struct FASBindData final : TableFuncBindData {
    std::vector<std::pair<int64_t, int64_t>> arcs;
    FASBindData(std::vector<std::pair<int64_t, int64_t>> arcs, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(arcs.size())},
          arcs{std::move(arcs)} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FASBindData>(arcs, columns);
    }
};

static offset_t fasTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<FASBindData>();
    auto n = morsel.getMorselSize();
    for (auto i = 0u; i < n; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i, bd->arcs[idx].first);
        output.getValueVectorMutable(1).setValue<int64_t>(i, bd->arcs[idx].second);
    }
    return n;
}

static std::unique_ptr<TableFuncBindData> fasBindImpl(
    const main::ClientContext* ctx, const TableFuncBindInput* input,
    const std::string& filter) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable, filter);
    auto fas = computeFeedbackArcSet(g);

    std::vector<std::pair<int64_t, int64_t>> arcs;
    for (auto& [u, v] : fas.feedbackArcs) {
        arcs.emplace_back(static_cast<int64_t>(u), static_cast<int64_t>(v));
    }

    std::vector<std::string> colNames = {"src_offset", "dst_offset"};
    std::vector<LogicalType> colTypes;
    colTypes.emplace_back(LogicalType::INT64());
    colTypes.emplace_back(LogicalType::INT64());
    colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
    auto columns = input->binder->createVariables(colNames, colTypes);
    return std::make_unique<FASBindData>(std::move(arcs), columns);
}

function_set FeedbackArcSetFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(FeedbackArcSetFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->tableFunc = SimpleTableFunc::getTableFunc(fasTableFunc);
    func->bindFunc = [](const main::ClientContext* c, const TableFuncBindInput* i) {
        return fasBindImpl(c, i, "");
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// ARTICULATION_POINTS(relTable STRING)
// Returns: (node_offset INT64)
// ═══════════════════════════════════════════════════════════════

struct APBindData final : TableFuncBindData {
    std::vector<int64_t> points;
    APBindData(std::vector<int64_t> points, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(points.size())},
          points{std::move(points)} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<APBindData>(points, columns);
    }
};

static offset_t apTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<APBindData>();
    auto n = morsel.getMorselSize();
    for (auto i = 0u; i < n; ++i) {
        output.getValueVectorMutable(0).setValue<int64_t>(i, bd->points[morsel.startOffset + i]);
    }
    return n;
}

function_set ArticulationPointsFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(ArticulationPointsFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->tableFunc = SimpleTableFunc::getTableFunc(apTableFunc);
    func->bindFunc = [](const main::ClientContext* ctx, const TableFuncBindInput* input)
        -> std::unique_ptr<TableFuncBindData> {
        auto relTable = input->getLiteralVal<std::string>(0);
        auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable);
        auto bc = computeArticulationAndBridges(g);
        std::vector<int64_t> pts;
        for (auto p : bc.articulationPoints) pts.push_back(static_cast<int64_t>(p));
        std::vector<std::string> colNames = {"node_offset"};
        std::vector<LogicalType> colTypes;
        colTypes.emplace_back(LogicalType::INT64());
        colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
        auto columns = input->binder->createVariables(colNames, colTypes);
        return std::make_unique<APBindData>(std::move(pts), columns);
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// BRIDGES(relTable STRING)
// Returns: (src_offset INT64, dst_offset INT64)
// ═══════════════════════════════════════════════════════════════

function_set BridgesFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(BridgesFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->tableFunc = SimpleTableFunc::getTableFunc(fasTableFunc); // same output schema as FAS
    func->bindFunc = [](const main::ClientContext* ctx, const TableFuncBindInput* input)
        -> std::unique_ptr<TableFuncBindData> {
        auto relTable = input->getLiteralVal<std::string>(0);
        auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable);
        auto bc = computeArticulationAndBridges(g);
        std::vector<std::pair<int64_t, int64_t>> arcs;
        for (auto& [u, v] : bc.bridges) {
            arcs.emplace_back(static_cast<int64_t>(u), static_cast<int64_t>(v));
        }
        std::vector<std::string> colNames = {"src_offset", "dst_offset"};
        std::vector<LogicalType> colTypes;
        colTypes.emplace_back(LogicalType::INT64());
        colTypes.emplace_back(LogicalType::INT64());
        colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
        auto columns = input->binder->createVariables(colNames, colTypes);
        return std::make_unique<FASBindData>(std::move(arcs), columns);
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// K_HOP_SUBGRAPH(relTable STRING, center INT64, k INT64)
// Returns: (node_offset INT64, distance INT64)
// ═══════════════════════════════════════════════════════════════

struct KHopBindData final : TableFuncBindData {
    std::vector<std::pair<int64_t, int64_t>> rows; // (node, dist)
    KHopBindData(std::vector<std::pair<int64_t, int64_t>> rows, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(rows.size())},
          rows{std::move(rows)} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<KHopBindData>(rows, columns);
    }
};

static offset_t khopTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<KHopBindData>();
    auto n = morsel.getMorselSize();
    for (auto i = 0u; i < n; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i, bd->rows[idx].first);
        output.getValueVectorMutable(1).setValue<int64_t>(i, bd->rows[idx].second);
    }
    return n;
}

function_set KHopSubgraphFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(KHopSubgraphFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                   LogicalTypeID::INT64});
    func->tableFunc = SimpleTableFunc::getTableFunc(khopTableFunc);
    func->bindFunc = [](const main::ClientContext* ctx, const TableFuncBindInput* input)
        -> std::unique_ptr<TableFuncBindData> {
        auto relTable = input->getLiteralVal<std::string>(0);
        auto center = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
        auto k = input->getLiteralVal<int64_t>(2);
        auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable);
        auto kh = extractKHop(g, center, k);
        std::vector<std::pair<int64_t, int64_t>> rows;
        for (size_t i = 0; i < kh.nodes.size(); ++i) {
            rows.emplace_back(static_cast<int64_t>(kh.nodes[i]), kh.distances[i]);
        }
        std::vector<std::string> colNames = {"node_offset", "distance"};
        std::vector<LogicalType> colTypes;
        colTypes.emplace_back(LogicalType::INT64());
        colTypes.emplace_back(LogicalType::INT64());
        colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
        auto columns = input->binder->createVariables(colNames, colTypes);
        return std::make_unique<KHopBindData>(std::move(rows), columns);
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// CALL_PATHS(relTable STRING, source INT64, target INT64)
// Returns: (path_id INT64, step INT64, node_offset INT64)
// ═══════════════════════════════════════════════════════════════

struct CallPathsBindData final : TableFuncBindData {
    struct Row { int64_t pathId; int64_t step; int64_t nodeOffset; };
    std::vector<Row> rows;
    CallPathsBindData(std::vector<Row> rows, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(rows.size())},
          rows{std::move(rows)} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CallPathsBindData>(rows, columns);
    }
};

static offset_t callPathsTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<CallPathsBindData>();
    auto n = morsel.getMorselSize();
    for (auto i = 0u; i < n; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i, bd->rows[idx].pathId);
        output.getValueVectorMutable(1).setValue<int64_t>(i, bd->rows[idx].step);
        output.getValueVectorMutable(2).setValue<int64_t>(i, bd->rows[idx].nodeOffset);
    }
    return n;
}

function_set CallPathsFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(CallPathsFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                   LogicalTypeID::INT64});
    func->tableFunc = SimpleTableFunc::getTableFunc(callPathsTableFunc);
    func->bindFunc = [](const main::ClientContext* ctx, const TableFuncBindInput* input)
        -> std::unique_ptr<TableFuncBindData> {
        auto relTable = input->getLiteralVal<std::string>(0);
        auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
        auto target = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));
        auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable);
        auto paths = enumerateCallPaths(g, source, target, 15);
        std::vector<CallPathsBindData::Row> rows;
        for (int64_t pid = 0; pid < static_cast<int64_t>(paths.size()); ++pid) {
            for (int64_t step = 0; step < static_cast<int64_t>(paths[pid].nodes.size()); ++step) {
                rows.push_back({pid, step, static_cast<int64_t>(paths[pid].nodes[step])});
            }
        }
        std::vector<std::string> colNames = {"path_id", "step", "node_offset"};
        std::vector<LogicalType> colTypes;
        colTypes.emplace_back(LogicalType::INT64());
        colTypes.emplace_back(LogicalType::INT64());
        colTypes.emplace_back(LogicalType::INT64());
        colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
        auto columns = input->binder->createVariables(colNames, colTypes);
        return std::make_unique<CallPathsBindData>(std::move(rows), columns);
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// CONSTRAINED_REACHABLE(relTable STRING, source INT64, target INT64, must_pass INT64)
// Returns: (reachable BOOL, reachable_through BOOL, shortest_path INT64, constrained_path INT64)
// ═══════════════════════════════════════════════════════════════

struct CRBindData final : TableFuncBindData {
    ReachabilityResult res;
    CRBindData(ReachabilityResult res, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1}, res{res} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CRBindData>(res, columns);
    }
};

static offset_t crTableFunc(const TableFuncMorsel&,
    const TableFuncInput& input, DataChunk& output) {
    auto bd = input.bindData->constPtrCast<CRBindData>();
    output.getValueVectorMutable(0).setValue<bool>(0, bd->res.reachable);
    output.getValueVectorMutable(1).setValue<bool>(0, bd->res.reachableThrough);
    output.getValueVectorMutable(2).setValue<int64_t>(0, bd->res.shortestPathLength);
    output.getValueVectorMutable(3).setValue<int64_t>(0, bd->res.shortestConstrainedPathLength);
    return 1;
}

function_set ConstrainedReachableFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(ConstrainedReachableFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                   LogicalTypeID::INT64, LogicalTypeID::INT64});
    func->tableFunc = SimpleTableFunc::getTableFunc(crTableFunc);
    func->bindFunc = [](const main::ClientContext* ctx, const TableFuncBindInput* input)
        -> std::unique_ptr<TableFuncBindData> {
        auto relTable = input->getLiteralVal<std::string>(0);
        auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
        auto target = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));
        auto mustPass = static_cast<uint64_t>(input->getLiteralVal<int64_t>(3));
        auto g = buildAdjGraphFromQuery(const_cast<main::ClientContext*>(ctx), relTable);
        auto res = constrainedReachability(g, source, target, mustPass);
        std::vector<std::string> colNames = {
            "reachable", "reachable_through", "shortest_path", "constrained_path"};
        std::vector<LogicalType> colTypes;
        colTypes.emplace_back(LogicalType::BOOL());
        colTypes.emplace_back(LogicalType::BOOL());
        colTypes.emplace_back(LogicalType::INT64());
        colTypes.emplace_back(LogicalType::INT64());
        colNames = TableFunction::extractYieldVariables(colNames, input->yieldVariables);
        auto columns = input->binder->createVariables(colNames, colTypes);
        return std::make_unique<CRBindData>(res, columns);
    };
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

// ═══════════════════════════════════════════════════════════════
// STRUCTURAL_DISTANCE is a scalar UDF, not a table function.
// Skip for now — requires ScalarFunction registration pattern.
// ═══════════════════════════════════════════════════════════════

function_set StructuralDistanceFunction::getFunctionSet() {
    // Placeholder — structural_distance needs ScalarFunction, not TableFunction.
    // Will be implemented separately.
    return {};
}

} // namespace function
} // namespace lbug
