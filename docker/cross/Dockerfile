FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gcc g++ \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*
# LoongArch: Debian bookworm 无 loongarch64 交叉工具链。
# 可选：通过 LOONGARCH_TOOLCHAIN ARG 注入自备工具链 tarball，
# 或在文档中说明自行安装 loongarch64-linux-gnu-gcc。
# ARG LOONGARCH_TOOLCHAIN
# RUN if [ -n "${LOONGARCH_TOOLCHAIN}" ]; then ...; fi
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
