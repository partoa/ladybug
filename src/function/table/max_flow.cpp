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
// MAX_FLOW(relTable STRING, source INT64, sink INT64) -> flow_value DOUBLE
//
// Computes the maximum flow from source to sink in the graph defined
// by the given relationship table. Each edge has unit capacity.
//
// Usage: CALL max_flow('ValueFlow', 0, 5) RETURN flow_value;
// ═══════════════════════════════════════════════════════════════════

struct MaxFlowBindData final : TableFuncBindData {
    double flowValue;

    MaxFlowBindData(double flowValue, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1}, flowValue{flowValue} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<MaxFlowBindData>(flowValue, columns);
    }
};

static offset_t maxFlowTableFunc(const TableFuncMorsel&,
    const TableFuncInput& input, DataChunk& output) {
    auto bindData = input.bindData->constPtrCast<MaxFlowBindData>();
    output.getValueVectorMutable(0).setValue<double>(0, bindData->flowValue);
    return 1;
}

static std::unique_ptr<TableFuncBindData> maxFlowBindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
    auto sink = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));

    auto net = buildFlowNetworkFromQuery(
        const_cast<main::ClientContext*>(context), relTable);
    double flow = net.maxFlow(source, sink);

    std::vector<std::string> columnNames = {"flow_value"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::DOUBLE());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<MaxFlowBindData>(flow, columns);
}

function_set MaxFlowFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
                                   LogicalTypeID::INT64});
    func->tableFunc = SimpleTableFunc::getTableFunc(maxFlowTableFunc);
    func->bindFunc = maxFlowBindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    result.push_back(std::move(func));
    return result;
}

} // namespace function
} // namespace lbug
