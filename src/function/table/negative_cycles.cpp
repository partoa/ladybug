#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "function/table/bind_data.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// NEGATIVE_CYCLES(relTable STRING)
// NEGATIVE_CYCLES(relTable STRING, filter STRING)
// Returns: (cycle_id INT64, position INT64, node_offset INT64)

struct CycleRow {
    int64_t cycleId;
    int64_t position;
    int64_t nodeOffset;
};

struct NegativeCyclesBindData final : TableFuncBindData {
    std::vector<CycleRow> rows;

    NegativeCyclesBindData(std::vector<CycleRow> rows, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), static_cast<row_idx_t>(rows.size())},
          rows{std::move(rows)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<NegativeCyclesBindData>(rows, columns);
    }
};

static offset_t negativeCyclesTableFunc(const TableFuncMorsel& morsel,
    const TableFuncInput& input, DataChunk& output) {
    auto bindData = input.bindData->constPtrCast<NegativeCyclesBindData>();
    auto numToOutput = morsel.getMorselSize();
    for (auto i = 0u; i < numToOutput; ++i) {
        auto idx = morsel.startOffset + i;
        output.getValueVectorMutable(0).setValue<int64_t>(i, bindData->rows[idx].cycleId);
        output.getValueVectorMutable(1).setValue<int64_t>(i, bindData->rows[idx].position);
        output.getValueVectorMutable(2).setValue<int64_t>(i, bindData->rows[idx].nodeOffset);
    }
    return numToOutput;
}

static std::unique_ptr<TableFuncBindData> negativeCyclesBindImpl(
    const main::ClientContext* context, const TableFuncBindInput* input,
    const std::string& filterExpr) {
    auto relTable = input->getLiteralVal<std::string>(0);

    uint64_t maxOffset = 0;
    auto edges = buildEdgeListFromQuery(
        const_cast<main::ClientContext*>(context), relTable, maxOffset, filterExpr);

    auto cycles = detectAllSimpleCycles(maxOffset + 1, edges, 10);

    std::vector<CycleRow> rows;
    for (int64_t cycleIdx = 0; cycleIdx < static_cast<int64_t>(cycles.size()); ++cycleIdx) {
        for (int64_t pos = 0; pos < static_cast<int64_t>(cycles[cycleIdx].nodeOffsets.size());
             ++pos) {
            rows.push_back({cycleIdx, pos,
                static_cast<int64_t>(cycles[cycleIdx].nodeOffsets[pos])});
        }
    }

    std::vector<std::string> columnNames = {"cycle_id", "position", "node_offset"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnTypes.emplace_back(LogicalType::INT64());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<NegativeCyclesBindData>(std::move(rows), columns);
}

function_set NegativeCyclesFunction::getFunctionSet() {
    function_set result;
    auto make = [](table_func_bind_t bindFn, std::vector<LogicalTypeID> params) {
        auto func = std::make_unique<TableFunction>(
            NegativeCyclesFunction::name, std::move(params));
        func->tableFunc = SimpleTableFunc::getTableFunc(negativeCyclesTableFunc);
        func->bindFunc = std::move(bindFn);
        func->initSharedStateFunc = SimpleTableFunc::initSharedState;
        func->initLocalStateFunc = TableFunction::initEmptyLocalState;
        func->canParallelFunc = [] { return false; };
        return func;
    };
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) { return negativeCyclesBindImpl(ctx, in, ""); },
        {LogicalTypeID::STRING}));
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) {
            return negativeCyclesBindImpl(ctx, in, in->getLiteralVal<std::string>(1));
        },
        {LogicalTypeID::STRING, LogicalTypeID::STRING}));
    return result;
}

} // namespace function
} // namespace lbug
