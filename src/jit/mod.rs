use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug, Default)]
pub struct MlirJitRule;

impl MlirJitRule {
    pub fn new() -> Self {
        Self
    }

    pub fn enabled() -> bool {
        cfg!(feature = "jit-mlir")
    }
}

impl PhysicalOptimizerRule for MlirJitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "mlir_jit_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
pub struct MlirBackend;

impl MlirBackend {
    pub fn new() -> Self {
        Self
    }

    pub fn is_available(&self) -> bool {
        cfg!(feature = "jit-mlir")
    }
}
