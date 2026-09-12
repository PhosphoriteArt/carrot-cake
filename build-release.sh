#!/usr/bin/env bash
set -euo pipefail

export PATH="$(brew --prefix llvm)/bin:$(brew --prefix lld)/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
export CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++
export AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar

export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
export CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++
export AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar

cargo build --target aarch64-apple-darwin --release
cargo build --target aarch64-unknown-linux-gnu --release
cargo build --target x86_64-unknown-linux-gnu --release
cargo xwin build --cross-compiler clang-cl --target x86_64-pc-windows-msvc --release

cd target
rm -rf dist
mkdir -p dist
mv aarch64-apple-darwin/release/carrot_cake dist/carrot-cake-macos-arm64
mv aarch64-unknown-linux-gnu/release/carrot_cake dist/carrot-cake-linux-arm64
mv x86_64-unknown-linux-gnu/release/carrot_cake dist/carrot-cake-linux-x86_64
mv x86_64-pc-windows-msvc/release/carrot_cake.exe dist/carrot-cake-windows-x86_64.exe
cd dist
zip dist.zip \
  carrot-cake-macos-arm64 \
  carrot-cake-linux-arm64 \
  carrot-cake-linux-x86_64 \
  carrot-cake-windows-x86_64.exe