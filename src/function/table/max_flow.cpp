#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "function/table/bind_data.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════════
// MAX_FLOW(relTable, source, sink) -> flow_value
// MAX_FLOW(relTable, source, sink, instance_id, field_name_id, call_site_id)
//
// Computes the maximum flow from source to sink. Each matching edge
// has unit capacity. Optional filter parameters restrict which edges
// are included in the flow network (-1 means no filter).
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

static EdgeFilter extractFilter(const TableFuncBindInput* input, int startIdx) {
    EdgeFilter filter;
    auto instId = input->getLiteralVal<int64_t>(startIdx);
    auto fieldId = input->getLiteralVal<int64_t>(startIdx + 1);
    auto callId = input->getLiteralVal<int64_t>(startIdx + 2);
    if (instId >= 0) filter.instanceId = instId;
    if (fieldId >= 0) filter.fieldNameId = fieldId;
    if (callId >= 0) filter.callSiteId = callId;
    return filter;
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

static std::unique_ptr<TableFuncBindData> maxFlowFilteredBindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
    auto sink = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));
    auto filter = extractFilter(input, 3);

    auto net = buildFlowNetworkFromQuery(
        const_cast<main::ClientContext*>(context), relTable, filter);
    double flow = net.maxFlow(source, sink);

    std::vector<std::string> columnNames = {"flow_value"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(LogicalType::DOUBLE());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<MaxFlowBindData>(flow, columns);
}

static std::unique_ptr<TableFunction> makeBaseFunc(const char* funcName,
    table_func_bind_t bindFunc, std::vector<LogicalTypeID> paramTypes) {
    auto func = std::make_unique<TableFunction>(funcName, std::move(paramTypes));
    func->tableFunc = SimpleTableFunc::getTableFunc(maxFlowTableFunc);
    func->bindFunc = std::move(bindFunc);
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    return func;
}

function_set MaxFlowFunction::getFunctionSet() {
    function_set result;
    // Overload 1: unfiltered (3 params)
    result.push_back(makeBaseFunc(name, maxFlowBindFunc,
        {LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64}));
    // Overload 2: filtered (6 params)
    result.push_back(makeBaseFunc(name, maxFlowFilteredBindFunc,
        {LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64,
         LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64}));
    return result;
}

} // namespace function
} // namespace lbug
