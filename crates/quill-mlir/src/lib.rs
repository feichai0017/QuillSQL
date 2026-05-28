use melior::Context;
use mlir_sys::MlirContext;

extern "C" {
    fn quillMlirRegisterDialect(context: MlirContext);
    fn quillMlirRegisterPasses();
}

pub fn register_dialect(context: &Context) {
    unsafe { quillMlirRegisterDialect(context.to_raw()) };
}

pub fn register_passes() {
    unsafe { quillMlirRegisterPasses() };
}
