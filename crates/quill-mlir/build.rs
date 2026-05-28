use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let prefix = mlir_prefix();
    let llvm_dir = prefix.join("lib/cmake/llvm");
    let mlir_dir = prefix.join("lib/cmake/mlir");

    if !mlir_dir.exists() {
        panic!(
            "MLIR CMake package not found at {}. Set MLIR_SYS_220_PREFIX or LLVM_SYS_220_PREFIX.",
            mlir_dir.display()
        );
    }

    let dst = cmake::Config::new("native")
        .define("CMAKE_BUILD_TYPE", "Release")
        .define("LLVM_DIR", &llvm_dir)
        .define("MLIR_DIR", &mlir_dir)
        .build();

    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build/lib").display()
    );
    println!("cargo:rustc-link-lib=static=quill_mlir");

    rerun("native/CMakeLists.txt");
    rerun("native/include/Quill/IR/CMakeLists.txt");
    rerun("native/include/Quill/IR/QuillDialect.h");
    rerun("native/include/Quill/IR/QuillOps.td");
    rerun("native/lib/CMakeLists.txt");
    rerun("native/lib/QuillCAPI.cpp");
    rerun("native/lib/QuillDialect.cpp");
    rerun("native/lib/QuillPasses.cpp");
}

fn mlir_prefix() -> PathBuf {
    ["MLIR_SYS_220_PREFIX", "LLVM_SYS_220_PREFIX"]
        .into_iter()
        .filter_map(|key| env::var_os(key).map(PathBuf::from))
        .find(|path| path.exists())
        .or_else(|| {
            let homebrew = PathBuf::from("/opt/homebrew/opt/llvm");
            homebrew.exists().then_some(homebrew)
        })
        .unwrap_or_else(|| {
            panic!("MLIR toolchain not found. Set MLIR_SYS_220_PREFIX or LLVM_SYS_220_PREFIX.")
        })
}

fn rerun(path: impl AsRef<Path>) {
    println!("cargo:rerun-if-changed={}", path.as_ref().display());
}
