#ifndef QUILL_IR_QUILLDIALECT_H
#define QUILL_IR_QUILLDIALECT_H

#include "mlir/Bytecode/BytecodeImplementation.h"
#include "mlir/Bytecode/BytecodeOpInterface.h"
#include "mlir/IR/Dialect.h"
#include "mlir/IR/OpDefinition.h"
#include "mlir/IR/Types.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"

#define GET_TYPEDEF_CLASSES
#include "Quill/IR/QuillOpsTypes.h.inc"

#define GET_OP_CLASSES
#include "Quill/IR/QuillOps.h.inc"

#include "Quill/IR/QuillOpsDialect.h.inc"

#endif // QUILL_IR_QUILLDIALECT_H
