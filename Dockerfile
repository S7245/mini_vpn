# ── Stage 1: Build ────────────────────────────────────────────────────────────
# Use the official Rust image to compile a fully static binary via musl.
# 中文要点：musl 静态链接让最终镜像不依赖任何 OS 动态库。
FROM rust:1.78-slim AS builder

# Install musl toolchain for fully static linking.
RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/* && \
    rustup target add x86_64-unknown-linux-musl

WORKDIR /app

# Cache dependency compilation separately from source changes.
# 中文要点：先只复制 Cargo 清单、编译空 lib，让依赖层被 Docker 缓存。
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'pub mod shared;' > src/lib.rs && \
    mkdir -p src/shared && \
    touch src/shared/mod.rs && \
    cargo build --release --target x86_64-unknown-linux-musl || true && \
    rm -rf src

# Now copy real source and build the final binary.
COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

# ── Stage 2: Minimal runtime image ───────────────────────────────────────────
# scratch base = zero OS layer, only the static binary.
# 中文要点：scratch 镜像大小仅取决于二进制本身，通常 < 15MB。
FROM scratch

COPY --from=builder \
    /app/target/x86_64-unknown-linux-musl/release/mini_vpn \
    /mini_vpn

# Relay server listens on 443 by default in production.
EXPOSE 443

ENTRYPOINT ["/mini_vpn", "server"]
