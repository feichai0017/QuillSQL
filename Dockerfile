FROM rust:1.82 as builder

# Install and use the nightly toolchain to support Edition 2024 dependencies
RUN rustup toolchain install nightly
RUN rustup default nightly
WORKDIR /app

# Pre-cache deps
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && echo "fn main(){}" > src/bin/dummy.rs && cargo build --release || true

# Build
COPY . .
RUN cargo build --release --bin server

FROM gcr.io/distroless/cc-debian12:nonroot
USER nonroot
WORKDIR /app
COPY --from=builder /app/target/release/server /usr/local/bin/server
COPY --from=builder /app/public /app/public
COPY --from=builder /app/docs /app/docs
ENV QUILL_HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
CMD ["/usr/local/bin/server"]


