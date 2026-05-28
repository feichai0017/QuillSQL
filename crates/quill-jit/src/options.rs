#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JitOptions {
    pub mlir_execution: bool,
}

impl JitOptions {
    pub fn mlir_execution() -> Self {
        Self {
            mlir_execution: true,
        }
    }

    pub fn from_env() -> Self {
        std::env::var("QUILL_JIT")
            .ok()
            .as_deref()
            .and_then(Self::parse)
            .unwrap_or_default()
    }

    pub(crate) fn mlir_execution_enabled(self) -> bool {
        self.mlir_execution && cfg!(feature = "jit-mlir")
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "mlir" | "compiled" | "on" | "1" | "true" => Some(Self::mlir_execution()),
            "" | "runtime" | "off" | "0" | "false" => Some(Self::default()),
            _ => None,
        }
    }
}
