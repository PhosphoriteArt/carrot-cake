#!/usr/bin/env bash
set -euo pipefail

export PATH="$(brew --prefix llvm)/bin:$(brew --prefix lld)/bin:$PATH"
# musl targets bundle libc; native-tls-vendored also bundles OpenSSL on Linux.
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc
export CC_x86_64_unknown_linux_musl=x86_64-linux-musl-gcc
export CXX_x86_64_unknown_linux_musl=x86_64-linux-musl-g++
export AR_x86_64_unknown_linux_musl=x86_64-linux-musl-ar

export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc
export CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc
export CXX_aarch64_unknown_linux_musl=aarch64-linux-musl-g++
export AR_aarch64_unknown_linux_musl=aarch64-linux-musl-ar

mkdir -p dist

cargo build --target aarch64-apple-darwin --target-dir dist/aarch64-apple-darwin --release --locked &
cargo build --target aarch64-unknown-linux-musl --target-dir dist/aarch64-unknown-linux-musl --release --locked &
cargo build --target x86_64-unknown-linux-musl --target-dir dist/x86_64-unknown-linux-musl --release --locked &
cargo xwin build --cross-compiler clang-cl --target x86_64-pc-windows-msvc --target-dir dist/x86_64-pc-windows-msvc --release --locked &

wait

cd dist
rm -rf out
mkdir -p out
cp aarch64-apple-darwin/aarch64-apple-darwin/release/carrot_cake out/carrot-cake-macos-arm64
cp aarch64-unknown-linux-musl/aarch64-unknown-linux-musl/release/carrot_cake out/carrot-cake-linux-arm64
cp x86_64-unknown-linux-musl/x86_64-unknown-linux-musl/release/carrot_cake out/carrot-cake-linux-x86_64
cp x86_64-pc-windows-msvc/x86_64-pc-windows-msvc/release/carrot_cake.exe out/carrot-cake-windows-x86_64.exe
cd out
zip ../out.zip \
  carrot-cake-macos-arm64 \
  carrot-cake-linux-arm64 \
  carrot-cake-linux-x86_64 \
  carrot-cake-windows-x86_64.exe
