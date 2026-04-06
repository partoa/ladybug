#include "function/table/simple_table_function.h"

#include "binder/binder.h"
#include "common/arrow/arrow_converter.h"
#include "common/arrow/arrow_nullmask_tree.h"
#include "common/exception/runtime.h"
#include "function/table/bind_data.h"
#include "storage/table/arrow_table_support.h"

using namespace lbug::common;

namespace lbug {
namespace function {

struct ArrowScanBindData final : TableFuncBindData {
    std::string arrowId;
    // Pointers into the global registry (owned by registry, not by us).
    ArrowSchemaWrapper* schema = nullptr;
    std::vector<ArrowArrayWrapper>* arrays = nullptr;

    ArrowScanBindData(std::string arrowId, ArrowSchemaWrapper* schema,
        std::vector<ArrowArrayWrapper>* arrays, binder::expression_vector columns,
        row_idx_t numRows)
        : TableFuncBindData{std::move(columns), numRows}, arrowId{std::move(arrowId)},
          schema{schema}, arrays{arrays} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ArrowScanBindData>(arrowId, schema, arrays, columns, numRows);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* /*context*/,
    const TableFuncBindInput* input) {
    auto arrowId = input->getLiteralVal<std::string>(0);

    ArrowSchemaWrapper* schema = nullptr;
    std::vector<ArrowArrayWrapper>* arrays = nullptr;
    if (!ArrowTableSupport::getArrowData(arrowId, schema, arrays)) {
        throw RuntimeException("Arrow registry ID '" + arrowId + "' not found.");
    }

    // Build output column schema from the Arrow schema.
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    for (int64_t i = 0; i < schema->n_children; ++i) {
        columnNames.emplace_back(schema->children[i]->name);
        columnTypes.push_back(ArrowConverter::fromArrowSchema(schema->children[i]));
    }

    // Count total rows across all arrays (batches).
    row_idx_t totalRows = 0;
    for (auto& arr : *arrays) {
        totalRows += arr.length;
    }

    columnNames =
        TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ArrowScanBindData>(arrowId, schema, arrays, columns, totalRows);
}

static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    auto* bindData = input.bindData->constPtrCast<ArrowScanBindData>();
    auto& arrays = *bindData->arrays;
    auto* schema = bindData->schema;

    auto startRow = morsel.startOffset;
    auto numRowsToOutput = morsel.getMorselSize();
    if (numRowsToOutput == 0) {
        return 0;
    }

    // Find which batch and offset the morsel starts at.
    // Walk through batches until we reach startRow.
    uint64_t batchIdx = 0;
    uint64_t offsetInBatch = startRow;
    for (; batchIdx < arrays.size(); ++batchIdx) {
        auto batchLen = static_cast<uint64_t>(arrays[batchIdx].length);
        if (offsetInBatch < batchLen) {
            break;
        }
        offsetInBatch -= batchLen;
    }

    // Copy data from Arrow arrays to output vectors, potentially spanning batches.
    uint64_t rowsCopied = 0;
    while (rowsCopied < numRowsToOutput && batchIdx < arrays.size()) {
        auto& batch = arrays[batchIdx];
        auto batchLen = static_cast<uint64_t>(batch.length);
        auto rowsAvailable = batchLen - offsetInBatch;
        auto rowsToCopy = std::min(rowsAvailable, numRowsToOutput - rowsCopied);

        for (int64_t col = 0; col < schema->n_children; ++col) {
            auto* childSchema = schema->children[col];
            auto* childArray = batch.children[col];
            auto& outputVector = output.getValueVectorMutable(col);

            ArrowNullMaskTree nullMask(childSchema, childArray, childArray->offset + offsetInBatch,
                rowsToCopy);
            ArrowConverter::fromArrowArray(childSchema, childArray, outputVector, &nullMask,
                childArray->offset + offsetInBatch, rowsCopied, rowsToCopy);
        }

        rowsCopied += rowsToCopy;
        offsetInBatch = 0;
        ++batchIdx;
    }

    return rowsCopied;
}

function_set ArrowScanFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    function->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace lbug
