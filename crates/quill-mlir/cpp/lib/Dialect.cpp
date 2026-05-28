#include "Quill/IR/Dialect.h"

#include "llvm/ADT/TypeSwitch.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/DialectImplementation.h"
#include "mlir/IR/OpImplementation.h"

using namespace mlir;
using namespace mlir::quill;

#include "Quill/IR/QuillOpsDialect.cpp.inc"

void QuillDialect::initialize() {
  addTypes<
#define GET_TYPEDEF_LIST
#include "Quill/IR/QuillOpsTypes.cpp.inc"
      >();
  addOperations<
#define GET_OP_LIST
#include "Quill/IR/QuillOps.cpp.inc"
      >();
}

#define GET_TYPEDEF_CLASSES
#include "Quill/IR/QuillOpsTypes.cpp.inc"

namespace {

LogicalResult verifySingleRowRegion(Operation *op, Region &region,
                                    StringRef regionName) {
  if (!llvm::hasSingleElement(region))
    return op->emitOpError() << regionName << " region must have one block";

  Block &block = region.front();
  if (block.getNumArguments() != 1)
    return op->emitOpError()
           << regionName << " region must have exactly one row argument";

  if (!isa<RowType>(block.getArgument(0).getType()))
    return op->emitOpError()
           << regionName << " region argument must be !quill.row";

  if (block.empty())
    return op->emitOpError() << regionName << " region must terminate";

  if (!isa<YieldOp>(block.back()))
    return op->emitOpError()
           << regionName << " region must terminate with quill.yield";

  return success();
}

LogicalResult verifyYieldCount(Operation *op, YieldOp yield,
                               unsigned expectedCount, StringRef regionName) {
  if (yield.getValues().size() != expectedCount)
    return op->emitOpError()
           << regionName << " region must yield " << expectedCount
           << " value(s)";
  return success();
}

} // namespace

LogicalResult FilterOp::verify() {
  if (failed(verifySingleRowRegion(getOperation(), getPredicate(),
                                   "predicate")))
    return failure();

  auto yield = cast<YieldOp>(getPredicate().front().back());
  if (failed(verifyYieldCount(getOperation(), yield, 1, "predicate")))
    return failure();

  if (!yield.getValues()[0].getType().isInteger(1))
    return emitOpError("predicate region must yield i1");

  return success();
}

LogicalResult ProjectOp::verify() {
  if (failed(verifySingleRowRegion(getOperation(), getProjector(),
                                   "projector")))
    return failure();

  auto yield = cast<YieldOp>(getProjector().front().back());
  if (yield.getValues().empty())
    return emitOpError("projector region must yield at least one value");

  return success();
}

LogicalResult RecordBatchSinkOp::verify() { return success(); }

LogicalResult PlainSumSinkOp::verify() {
  if (failed(verifySingleRowRegion(getOperation(), getMeasure(), "measure")))
    return failure();

  auto yield = cast<YieldOp>(getMeasure().front().back());
  if (failed(verifyYieldCount(getOperation(), yield, 1, "measure")))
    return failure();

  Type valueType = yield.getValues()[0].getType();
  if (!valueType.isIntOrIndexOrFloat())
    return emitOpError("measure region must yield a numeric scalar");

  return success();
}

LogicalResult GroupAggregateSinkOp::verify() {
  if (failed(verifySingleRowRegion(getOperation(), getState(), "state")))
    return failure();

  auto yield = cast<YieldOp>(getState().front().back());
  if (yield.getValues().size() < 2)
    return emitOpError("state region must yield at least one key and one aggregate value");

  for (Value value : yield.getValues()) {
    Type type = value.getType();
    if (!type.isIntOrIndexOrFloat())
      return emitOpError("state region values must be fixed-width scalar types");
  }

  return success();
}

LogicalResult ColumnOp::verify() {
  if (getIndex() < 0)
    return emitOpError("column index must be non-negative");
  return success();
}

#define GET_OP_CLASSES
#include "Quill/IR/QuillOps.cpp.inc"
