#include "function/table/simple_table_function.h"
#include "function/table/flow_utils.h"

#include "binder/binder.h"
#include "function/table/bind_data.h"

using namespace lbug::common;
using namespace lbug::function;

namespace lbug {
namespace function {

// MAX_FLOW(relTable STRING, source INT64, sink INT64) -> flow_value DOUBLE
// MAX_FLOW(relTable STRING, source INT64, sink INT64, filter STRING)
//
// filter is a Cypher predicate on the relationship variable r.
// Example: 'r.instance_id = 42 AND r.weight > 0.5'

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

static std::unique_ptr<TableFuncBindData> maxFlowBindImpl(
    const main::ClientContext* context, const TableFuncBindInput* input,
    const std::string& filterExpr) {
    auto relTable = input->getLiteralVal<std::string>(0);
    auto source = static_cast<uint64_t>(input->getLiteralVal<int64_t>(1));
    auto sink = static_cast<uint64_t>(input->getLiteralVal<int64_t>(2));

    auto net = buildFlowNetworkFromQuery(
        const_cast<main::ClientContext*>(context), relTable, filterExpr);
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
    auto make = [](table_func_bind_t bindFn, std::vector<LogicalTypeID> params) {
        auto func = std::make_unique<TableFunction>(MaxFlowFunction::name, std::move(params));
        func->tableFunc = SimpleTableFunc::getTableFunc(maxFlowTableFunc);
        func->bindFunc = std::move(bindFn);
        func->initSharedStateFunc = SimpleTableFunc::initSharedState;
        func->initLocalStateFunc = TableFunction::initEmptyLocalState;
        func->canParallelFunc = [] { return false; };
        return func;
    };
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) { return maxFlowBindImpl(ctx, in, ""); },
        {LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64}));
    result.push_back(make(
        [](const main::ClientContext* ctx, const TableFuncBindInput* in) {
            return maxFlowBindImpl(ctx, in, in->getLiteralVal<std::string>(3));
        },
        {LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64,
         LogicalTypeID::STRING}));
    return result;
}

} // namespace function
} // namespace lbug
