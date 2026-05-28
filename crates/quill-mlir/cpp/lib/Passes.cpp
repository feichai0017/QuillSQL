#include "Quill/IR/Dialect.h"

#include "mlir/Dialect/Arith/IR/Arith.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
#include "mlir/Dialect/SCF/IR/SCF.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/Pass/Pass.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Pass/PassRegistry.h"
#include "llvm/ADT/SmallVector.h"

#include <map>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif

namespace {

struct ColumnInfo {
  int64_t index;
  mlir::Type type;
  mlir::Value pointer;
};

struct ColumnAccess {
  int64_t index;
  mlir::Type type;
};

static mlir::LogicalResult collectColumns(mlir::Region &region,
                                          std::map<int64_t, mlir::Type> &columns,
                                          mlir::Operation *owner) {
  auto walkResult = region.walk([&](mlir::quill::ColumnOp column) {
    int64_t index = static_cast<int64_t>(column.getIndex());
    mlir::Type type = column.getResult().getType();
    auto [it, inserted] = columns.try_emplace(index, type);
    if (!inserted && it->second != type) {
      column.emitOpError("has inconsistent type for repeated column index ")
          << index;
      return mlir::WalkResult::interrupt();
    }
    return mlir::WalkResult::advance();
  });
  if (walkResult.wasInterrupted())
    return mlir::failure();
  if (columns.empty())
    return owner->emitOpError("lowering requires at least one column access");
  return mlir::success();
}

static mlir::LogicalResult collectSingleI64Column(mlir::Region &region,
                                                  ColumnAccess &column,
                                                  mlir::Operation *owner) {
  std::map<int64_t, mlir::Type> columns;
  if (mlir::failed(collectColumns(region, columns, owner)))
    return mlir::failure();
  if (columns.size() != 1)
    return owner->emitOpError("lowering requires exactly one i64 column");

  auto [index, type] = *columns.begin();
  if (!type.isInteger(64))
    return owner->emitOpError("lowering requires an i64 column");

  column = ColumnAccess{index, type};
  return mlir::success();
}

static mlir::LogicalResult singleYieldType(mlir::Region &region,
                                           mlir::Type &type,
                                           mlir::Operation *owner,
                                           llvm::StringRef regionName) {
  mlir::Block &block = region.front();
  auto yield = llvm::dyn_cast<mlir::quill::YieldOp>(block.getTerminator());
  if (!yield || yield.getValues().size() != 1)
    return owner->emitOpError() << regionName << " region must yield one value";
  type = yield.getValues().front().getType();
  return mlir::success();
}

static mlir::Value loadColumn(mlir::OpBuilder &builder, mlir::Location loc,
                              mlir::Value index, const ColumnInfo &column) {
  auto ptrType = mlir::LLVM::LLVMPointerType::get(builder.getContext());
  llvm::SmallVector<mlir::LLVM::GEPArg> indices;
  indices.push_back(index);
  auto elementPtr = builder.create<mlir::LLVM::GEPOp>(
      loc, ptrType, column.type, column.pointer, indices);
  return builder.create<mlir::LLVM::LoadOp>(loc, column.type, elementPtr);
}

static mlir::LogicalResult cloneRegionValue(
    mlir::OpBuilder &builder, mlir::Region &region,
    const llvm::DenseMap<int64_t, mlir::Value> &loadedColumns,
    mlir::Operation *owner, mlir::Value &result) {
  mlir::Block &block = region.front();
  mlir::IRMapping mapping;

  for (mlir::Operation &op : block.without_terminator()) {
    if (auto column = llvm::dyn_cast<mlir::quill::ColumnOp>(op)) {
      auto loaded = loadedColumns.find(static_cast<int64_t>(column.getIndex()));
      if (loaded == loadedColumns.end())
        return column.emitOpError("has no loaded value for column index ")
               << column.getIndex();
      if (loaded->second.getType() != column.getResult().getType())
        return column.emitOpError("loaded column type does not match result");
      mapping.map(column.getResult(), loaded->second);
      continue;
    }

    if (op.getDialect()->getNamespace() == "quill")
      return op.emitOpError("cannot be cloned into loop body");

    mlir::Operation *cloned = builder.clone(op, mapping);
    mapping.map(op.getResults(), cloned->getResults());
  }

  auto yield = llvm::dyn_cast<mlir::quill::YieldOp>(block.getTerminator());
  if (!yield || yield.getValues().size() != 1)
    return owner->emitOpError("region must yield one value");

  result = mapping.lookupOrDefault(yield.getValues().front());
  return mlir::success();
}

static mlir::LogicalResult lowerFilterProject(mlir::func::FuncOp func) {
  if (func.isExternal() || !llvm::hasSingleElement(func.getBody()))
    return mlir::failure();

  mlir::quill::FilterOp filter;
  mlir::quill::ProjectOp project;
  mlir::quill::RecordBatchSinkOp sink;
  func.walk([&](mlir::quill::FilterOp op) {
    if (!filter)
      filter = op;
  });
  func.walk([&](mlir::quill::ProjectOp op) {
    if (!project)
      project = op;
  });
  func.walk([&](mlir::quill::RecordBatchSinkOp op) {
    if (!sink)
      sink = op;
  });

  if (!filter || !project || !sink)
    return mlir::failure();

  ColumnAccess predicateColumn;
  if (mlir::failed(collectSingleI64Column(
          filter.getPredicate(), predicateColumn, filter.getOperation())))
    return mlir::failure();

  ColumnAccess projectionColumn;
  if (mlir::failed(collectSingleI64Column(
          project.getProjector(), projectionColumn, project.getOperation())))
    return mlir::failure();

  mlir::Type projectionType;
  if (mlir::failed(singleYieldType(project.getProjector(), projectionType,
                                  project.getOperation(), "projector")))
    return mlir::failure();
  if (!projectionType.isInteger(64))
    return project.emitOpError("lowering requires one i64 projection result");

  mlir::Region predicateRegion;
  mlir::IRMapping predicateMapping;
  filter.getPredicate().cloneInto(&predicateRegion, predicateMapping);
  mlir::Region projectorRegion;
  mlir::IRMapping projectorMapping;
  project.getProjector().cloneInto(&projectorRegion, projectorMapping);

  mlir::MLIRContext *context = func.getContext();
  mlir::OpBuilder builder(context);
  mlir::Location loc = func.getLoc();
  auto i64Type = builder.getI64Type();
  auto i32Type = builder.getI32Type();
  auto ptrType = mlir::LLVM::LLVMPointerType::get(context);

  llvm::SmallVector<mlir::Type> inputTypes{
      i64Type, ptrType, ptrType, ptrType, ptrType};

  func.eraseBody();
  func.setFunctionType(mlir::FunctionType::get(context, inputTypes, i32Type));
  func->setAttr("llvm.emit_c_interface", builder.getUnitAttr());
  mlir::Block *entry = func.addEntryBlock();
  builder.setInsertionPointToStart(entry);

  mlir::Value len = entry->getArgument(0);
  mlir::Value predicatePtr = entry->getArgument(1);
  mlir::Value projectionPtr = entry->getArgument(2);
  mlir::Value outputPtr = entry->getArgument(3);
  mlir::Value outputLen = entry->getArgument(4);

  ColumnInfo predicateInfo{predicateColumn.index, predicateColumn.type,
                           predicatePtr};
  ColumnInfo projectionInfo{projectionColumn.index, projectionColumn.type,
                            projectionPtr};

  mlir::Value zeroI64 = builder.create<mlir::arith::ConstantIntOp>(loc, 0, 64);
  mlir::Value oneI64 = builder.create<mlir::arith::ConstantIntOp>(loc, 1, 64);

  bool cloneFailed = false;
  auto loop = builder.create<mlir::scf::ForOp>(
      loc, zeroI64, len, oneI64, mlir::ValueRange{zeroI64},
      [&](mlir::OpBuilder &bodyBuilder, mlir::Location bodyLoc,
          mlir::Value iv, mlir::ValueRange iterArgs) {
        llvm::DenseMap<int64_t, mlir::Value> predicateValues;
        predicateValues.try_emplace(
            predicateInfo.index, loadColumn(bodyBuilder, bodyLoc, iv, predicateInfo));

        mlir::Value predicate;
        if (mlir::failed(cloneRegionValue(bodyBuilder, predicateRegion,
                                          predicateValues, func.getOperation(),
                                          predicate))) {
          cloneFailed = true;
          bodyBuilder.create<mlir::scf::YieldOp>(bodyLoc, iterArgs);
          return;
        }

        auto branch = bodyBuilder.create<mlir::scf::IfOp>(
            bodyLoc, mlir::TypeRange{i64Type}, predicate, true);
        {
          mlir::OpBuilder thenBuilder = branch.getThenBodyBuilder();
          llvm::DenseMap<int64_t, mlir::Value> projectionValues;
          projectionValues.try_emplace(
              projectionInfo.index,
              loadColumn(thenBuilder, bodyLoc, iv, projectionInfo));

          mlir::Value projected;
          if (mlir::failed(cloneRegionValue(thenBuilder, projectorRegion,
                                            projectionValues,
                                            func.getOperation(), projected))) {
            cloneFailed = true;
            thenBuilder.create<mlir::scf::YieldOp>(bodyLoc, iterArgs);
            return;
          }

          auto outPtr = thenBuilder.create<mlir::LLVM::GEPOp>(
              bodyLoc, ptrType, i64Type, outputPtr,
              llvm::SmallVector<mlir::LLVM::GEPArg>{iterArgs[0]});
          thenBuilder.create<mlir::LLVM::StoreOp>(bodyLoc, projected, outPtr);
          mlir::Value nextCount =
              thenBuilder.create<mlir::arith::AddIOp>(bodyLoc, iterArgs[0],
                                                      oneI64);
          thenBuilder.create<mlir::scf::YieldOp>(
              bodyLoc, mlir::ValueRange{nextCount});
        }
        {
          mlir::OpBuilder elseBuilder = branch.getElseBodyBuilder();
          elseBuilder.create<mlir::scf::YieldOp>(bodyLoc, iterArgs);
        }
        bodyBuilder.create<mlir::scf::YieldOp>(bodyLoc, branch.getResults());
      });
  if (cloneFailed)
    return mlir::failure();

  builder.create<mlir::LLVM::StoreOp>(loc, loop.getResult(0), outputLen);
  mlir::Value ok = builder.create<mlir::arith::ConstantIntOp>(loc, 0, 32);
  builder.create<mlir::func::ReturnOp>(loc, ok);
  return mlir::success();
}

static mlir::Value zeroForType(mlir::OpBuilder &builder, mlir::Location loc,
                               mlir::Type type) {
  if (auto integerType = llvm::dyn_cast<mlir::IntegerType>(type)) {
    return builder.create<mlir::arith::ConstantIntOp>(
        loc, 0, integerType.getWidth());
  }
  if (auto floatType = llvm::dyn_cast<mlir::FloatType>(type)) {
    return builder.create<mlir::arith::ConstantFloatOp>(
        loc, floatType, llvm::APFloat(0.0));
  }
  return {};
}

static mlir::Value addValues(mlir::OpBuilder &builder, mlir::Location loc,
                             mlir::Value lhs, mlir::Value rhs) {
  mlir::Type type = lhs.getType();
  if (llvm::isa<mlir::FloatType>(type))
    return builder.create<mlir::arith::AddFOp>(loc, lhs, rhs);
  return builder.create<mlir::arith::AddIOp>(loc, lhs, rhs);
}

static mlir::LogicalResult lowerFilterPlainSum(mlir::func::FuncOp func) {
  if (func.isExternal() || !llvm::hasSingleElement(func.getBody()))
    return mlir::failure();

  mlir::quill::FilterOp filter;
  mlir::quill::PlainSumSinkOp plainSum;
  func.walk([&](mlir::quill::FilterOp op) {
    if (!filter)
      filter = op;
  });
  func.walk([&](mlir::quill::PlainSumSinkOp op) {
    if (!plainSum)
      plainSum = op;
  });

  if (!filter || !plainSum)
    return mlir::failure();

  std::map<int64_t, mlir::Type> columnTypes;
  if (mlir::failed(collectColumns(filter.getPredicate(), columnTypes, filter)))
    return mlir::failure();
  if (mlir::failed(collectColumns(plainSum.getMeasure(), columnTypes, plainSum)))
    return mlir::failure();

  mlir::Block &measureBlock = plainSum.getMeasure().front();
  auto measureYield =
      llvm::dyn_cast<mlir::quill::YieldOp>(measureBlock.getTerminator());
  if (!measureYield || measureYield.getValues().size() != 1)
    return plainSum.emitOpError("measure region must yield one value");
  mlir::Type sumType = measureYield.getValues().front().getType();
  if (!llvm::isa<mlir::IntegerType, mlir::FloatType>(sumType))
    return plainSum.emitOpError("measure region must yield an integer or float");

  mlir::Region predicateRegion;
  mlir::IRMapping predicateMapping;
  filter.getPredicate().cloneInto(&predicateRegion, predicateMapping);
  mlir::Region measureRegion;
  mlir::IRMapping measureMapping;
  plainSum.getMeasure().cloneInto(&measureRegion, measureMapping);

  mlir::MLIRContext *context = func.getContext();
  mlir::OpBuilder builder(context);
  mlir::Location loc = func.getLoc();
  auto i64Type = builder.getI64Type();
  auto i32Type = builder.getI32Type();
  auto ptrType = mlir::LLVM::LLVMPointerType::get(context);

  llvm::SmallVector<mlir::Type> inputTypes;
  inputTypes.push_back(i64Type);
  for (const auto &[_, type] : columnTypes)
    inputTypes.push_back(ptrType);
  inputTypes.push_back(ptrType);
  inputTypes.push_back(ptrType);

  func.eraseBody();
  func.setFunctionType(mlir::FunctionType::get(context, inputTypes, i32Type));
  func->setAttr("llvm.emit_c_interface", builder.getUnitAttr());
  mlir::Block *entry = func.addEntryBlock();
  builder.setInsertionPointToStart(entry);

  mlir::Value len = entry->getArgument(0);
  llvm::SmallVector<ColumnInfo> columns;
  size_t argIndex = 1;
  for (const auto &[index, type] : columnTypes)
    columns.push_back(ColumnInfo{index, type, entry->getArgument(argIndex++)});
  mlir::Value outSum = entry->getArgument(argIndex++);
  mlir::Value outCount = entry->getArgument(argIndex++);

  mlir::Value zeroI64 = builder.create<mlir::arith::ConstantIntOp>(loc, 0, 64);
  mlir::Value oneI64 = builder.create<mlir::arith::ConstantIntOp>(loc, 1, 64);
  mlir::Value zeroSum = zeroForType(builder, loc, sumType);
  if (!zeroSum)
    return plainSum.emitOpError("unsupported SUM state type");

  bool cloneFailed = false;
  auto loop = builder.create<mlir::scf::ForOp>(
      loc, zeroI64, len, oneI64, mlir::ValueRange{zeroSum, zeroI64},
      [&](mlir::OpBuilder &bodyBuilder, mlir::Location bodyLoc,
          mlir::Value iv, mlir::ValueRange iterArgs) {
        llvm::DenseMap<int64_t, mlir::Value> loadedColumns;
        for (const ColumnInfo &column : columns)
          loadedColumns.try_emplace(column.index,
                                    loadColumn(bodyBuilder, bodyLoc, iv, column));

        mlir::Value predicate;
        if (mlir::failed(cloneRegionValue(bodyBuilder, predicateRegion,
                                          loadedColumns, func.getOperation(),
                                          predicate))) {
          cloneFailed = true;
          bodyBuilder.create<mlir::scf::YieldOp>(
              bodyLoc, mlir::ValueRange{iterArgs[0], iterArgs[1]});
          return;
        }

        auto branch = bodyBuilder.create<mlir::scf::IfOp>(
            bodyLoc, mlir::TypeRange{sumType, i64Type}, predicate, true);
        {
          mlir::OpBuilder thenBuilder = branch.getThenBodyBuilder();
          mlir::Value measure;
          if (mlir::failed(cloneRegionValue(thenBuilder, measureRegion,
                                            loadedColumns, func.getOperation(),
                                            measure))) {
            cloneFailed = true;
            thenBuilder.create<mlir::scf::YieldOp>(
                bodyLoc, mlir::ValueRange{iterArgs[0], iterArgs[1]});
            return;
          }
          mlir::Value nextSum =
              addValues(thenBuilder, bodyLoc, iterArgs[0], measure);
          mlir::Value nextCount =
              thenBuilder.create<mlir::arith::AddIOp>(bodyLoc, iterArgs[1],
                                                      oneI64);
          thenBuilder.create<mlir::scf::YieldOp>(
              bodyLoc, mlir::ValueRange{nextSum, nextCount});
        }
        {
          mlir::OpBuilder elseBuilder = branch.getElseBodyBuilder();
          elseBuilder.create<mlir::scf::YieldOp>(
              bodyLoc, mlir::ValueRange{iterArgs[0], iterArgs[1]});
        }
        bodyBuilder.create<mlir::scf::YieldOp>(bodyLoc, branch.getResults());
      });
  if (cloneFailed)
    return mlir::failure();

  builder.create<mlir::LLVM::StoreOp>(loc, loop.getResult(0), outSum);
  builder.create<mlir::LLVM::StoreOp>(loc, loop.getResult(1), outCount);
  mlir::Value ok = builder.create<mlir::arith::ConstantIntOp>(loc, 0, 32);
  builder.create<mlir::func::ReturnOp>(loc, ok);
  return mlir::success();
}

struct QuillCanonicalizePipelinePass
    : public mlir::PassWrapper<QuillCanonicalizePipelinePass,
                               mlir::OperationPass<mlir::ModuleOp>> {
  MLIR_DEFINE_EXPLICIT_INTERNAL_INLINE_TYPE_ID(QuillCanonicalizePipelinePass)

  llvm::StringRef getArgument() const final {
    return "quill-canonicalize-pipeline";
  }

  llvm::StringRef getDescription() const final {
    return "canonicalize Quill pipeline graph before loop lowering";
  }

  void runOnOperation() final {}
};

struct ConvertQuillToLoopsPass
    : public mlir::PassWrapper<ConvertQuillToLoopsPass,
                               mlir::OperationPass<mlir::ModuleOp>> {
  MLIR_DEFINE_EXPLICIT_INTERNAL_INLINE_TYPE_ID(ConvertQuillToLoopsPass)

  llvm::StringRef getArgument() const final { return "convert-quill-to-loops"; }

  llvm::StringRef getDescription() const final {
    return "lower Quill pipeline operations to loop-level MLIR";
  }

  void runOnOperation() final {
    for (auto func : getOperation().getOps<mlir::func::FuncOp>()) {
      bool hasPlainSum = false;
      func.walk([&](mlir::quill::PlainSumSinkOp) { hasPlainSum = true; });
      bool hasRecordSink = false;
      func.walk([&](mlir::quill::RecordBatchSinkOp) { hasRecordSink = true; });

      mlir::LogicalResult result = mlir::success();
      if (hasPlainSum)
        result = lowerFilterPlainSum(func);
      else if (hasRecordSink)
        result = lowerFilterProject(func);

      if (mlir::failed(result)) {
        signalPassFailure();
        return;
      }
    }
  }
};

} // namespace

extern "C" void quillMlirRegisterPasses() {
  static const bool registered = [] {
    mlir::PassRegistration<QuillCanonicalizePipelinePass>();
    mlir::PassRegistration<ConvertQuillToLoopsPass>();
    return true;
  }();
  (void)registered;
}

#ifdef __clang__
#pragma clang diagnostic pop
#endif
