#include "Quill/IR/QuillDialect.h"

#include "mlir/IR/BuiltinOps.h"
#include "mlir/Pass/Pass.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Pass/PassRegistry.h"

namespace {

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

  void runOnOperation() final {}
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
