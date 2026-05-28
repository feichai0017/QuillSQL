#include "Quill/IR/Dialect.h"

#include "mlir-c/IR.h"
#include "mlir/CAPI/IR.h"
#include "mlir/IR/DialectRegistry.h"
#include "mlir/IR/MLIRContext.h"

extern "C" void quillMlirRegisterDialect(MlirContext context) {
  mlir::DialectRegistry registry;
  registry.insert<mlir::quill::QuillDialect>();
  unwrap(context)->appendDialectRegistry(registry);
  unwrap(context)->getOrLoadDialect<mlir::quill::QuillDialect>();
}

extern "C" void quillMlirRegisterPasses();
