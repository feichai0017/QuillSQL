# Contributor's Guide

Welcome, and thank you for your interest in contributing to QuillSQL! Whether you're fixing a bug, adding a new feature, or improving the documentation, this guide will help you get started.

## 1. Getting Started: Your Development Environment

### Prerequisites

- **Rust**: QuillSQL is written in Rust. If you don't have it yet, install it via [rustup](https://rustup.rs/). This will provide you with `rustc` (the compiler) and `cargo` (the package manager and build tool).
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Build Essentials**: Ensure you have a C++ compiler like `gcc` or `clang` installed, which is a common dependency for some Rust crates.

### Setup

1.  **Fork the Repository**: Start by forking the main QuillSQL repository to your own GitHub account.

2.  **Clone Your Fork**: Clone your forked repository to your local machine.
    ```bash
    git clone https://github.com/feichai0017/QuillSQL.git
    cd QuillSQL
    ```

3.  **Build the Project**: Compile the entire project to ensure everything is set up correctly.
    ```bash
    cargo build
    ```

## 2. Development Workflow

### Running Tests

Before and after making any changes, it's crucial to run the test suite to ensure you haven't broken anything.

- **Run all unit and integration tests**:
  ```bash
  cargo test
  ```

- **Run the benchmark suite**:
  ```bash
  cargo bench
  ```

### Code Style and Quality

We adhere to the standard Rust coding style and use tools to enforce it.

- **Formatting**: Before committing, please format your code with `rustfmt`.
  ```bash
  cargo fmt --all
  ```

- **Linting**: We use `clippy` to catch common mistakes and improve code quality. Please ensure `clippy` passes without warnings.
  ```bash
  cargo clippy --all-targets -- -D warnings
  ```

### Submitting Your Contribution

1.  **Create a New Branch**: Create a descriptive branch name for your feature or bugfix.
    ```bash
    git checkout -b my-awesome-feature
    ```

2.  **Make Your Changes**: Write your code. Add new tests to cover your changes. Ensure all existing tests still pass.

3.  **Format and Lint**: Run `cargo fmt` and `cargo clippy` as described above.

4.  **Commit Your Work**: Write a clear and concise commit message.
    ```bash
    git add .
    git commit -m "feat: Add support for window functions"
    ```

5.  **Push to Your Fork**: Push your branch to your fork on GitHub.
    ```bash
    git push -u origin my-awesome-feature
    ```

6.  **Open a Pull Request**: Go to the original QuillSQL repository on GitHub. You should see a prompt to open a Pull Request from your new branch. Fill out the PR template with a description of your changes.

## 3. Working on the Documentation

The documentation is built using `mdbook`. To preview your changes locally, you'll need to install it and the `mermaid` plugin.

1.  **Install `mdbook` and `mdbook-mermaid`**:
    ```bash
    cargo install mdbook
    cargo install mdbook-mermaid
    ```

2.  **Serve the Book Locally**: Run the following command from the root of the project.
    ```bash
    mdbook serve docs
    ```
    This will build the book and start a local web server. You can open your browser to `http://localhost:3000` to see the live-previewed documentation.
