#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/simple_table_function.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════════
// MIN_CUT(relTable STRING, source INT64, sink INT64)
// Returns: (node_offset INT64, side INT64, flow_value DOUBLE)
//   side = 0 -> source side, side = 1 -> sink side
//   flow_value = max-flow = min-cut capacity (same for all rows)
//
// Usage: CALL min_cut('ValueFlow', 0, 5) RETURN node_offset, side, flow_value;
// ═══════════════════════════════════════════════════════════════════

struct MinCutBindData final : TableFuncBindData {
    std::vector<uint64_t> offsets;
    std::vector<uint8_t> sides;
    double flowValue;

    MinCutBindData(std::vector<uint64_t> offsets, std::vector<uint8_t> sides,
        double flowValue, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(offsets.size())},
          offsets{std::move(offsets)}, sides{std::move(sides)}, flowValue{flowValue} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<MinCutBindData>(offsets, sides, flowValue, columns);
    }
};

static offset_t minCutTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bindData = input.bindData->constPtrCast<MinCutBindData>();
    auto numToOutput = morsel.getMorselSize();
    for (auto i = 0u; i < numToOutput; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i,
            static_cast<int64_t>(bindData->offsets[idx]));
        output.getValueVectorMutable(1).setValue<int64_t>(i,
            static_cast<int64_t>(bindData->sides[idx]));
        output.getValueVectorMutable(2).setValue<double>(i, bindData->flowValue);
    }
    return numToOutput;
}

static std::unique_ptr<TableFuncBindData> minCutBindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
    auto sink = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));

    auto net = buildFlowNetworkFromQuery(
        const_cast<main::ClientContext*>(context), relTable);
    double flow = net.maxFlow(source, sink);

    std::vector<uint8_t> cutSides;
    net.minCutSides(source, cutSides);

    std::vector<uint64_t> offsets;
    std::vector<uint8_t> sides;
    for (uint64_t i = 0; i < cutSides.size(); ++i) {
        offsets.push_back(i);
        sides.push_back(cutSides[i]);
    }

    std::vector<std::string> columnNames = {"node_offset", "side", "flow_value"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::DOUBLE());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<MinCutBindData>(
        std::move(offsets), std::move(sides), flow, columns);
}

function_set MinCutFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                   LogicalTypeID::INT64});
    func->tableFunc = SimpleTableFunc::getTableFunc(minCutTableFunc);
    func->bindFunc = minCutBindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

} // namespace function
} // namespace lbug
