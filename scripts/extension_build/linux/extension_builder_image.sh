#!/bin/bash
set -eo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_DIR="$SCRIPT_DIR/../../../"


# Define variables from the matrix and inputs
VCPKG_TARGET_TRIPLET=x64-linux
DUCKDB_PLATFORM=linux_amd64
ENABLE_RUST=0
CC=$([[ $DUCKDB_PLATFORM == "linux_arm64" ]] && echo "aarch64-linux-gnu-gcc" || echo "gcc")
CXX=$([[ $duckdb_platform == "linux_arm64" ]] && echo "aarch64-linux-gnu-g++" || echo "g++")
RUN_TESTS=$([[ $duckdb_platform != "linux_arm64" ]] && echo "1" || echo "")
AARCH64_CROSS_COMPILE=$([[ $duckdb_platform == "linux_arm64" ]] && echo "1" || echo "")

# Build the docker image
DOCKER_BUILDKIT=1 docker buildx build \
    --platform linux/amd64 \
    --build-arg VCPKG_BUILD=$VCPKG_BUILD \
    --build-arg PROJECT_DIR=$PROJECT_DIR \
    --build-arg VCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET \
    --build-arg VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH \
    --build-arg DUCKDB_PLATFORM=$DUCKDB_PLATFORM \
    --build-arg ENABLE_RUST=$ENABLE_RUST \
    --build-arg CC=$CC \
    --build-arg CXX=$CCX \
    --build-arg RUN_TESTS=$RUN_TESTS \
    --build-arg AARCH64_CROSS_COMPILE=$AARCH64_CROSS_COMPILE \
    --load \
    -t duckdb_extension_build:master \
    -f $SCRIPT_DIR/linux.Dockerfile $PROJECT_DIR

# Define variables from the matrix and inputs
VCPKG_TARGET_TRIPLET=x64-linux
DUCKDB_PLATFORM=linux_amd64
ENABLE_RUST=0
CC=$([[ $DUCKDB_PLATFORM == "linux_arm64" ]] && echo "aarch64-linux-gnu-gcc" || echo "gcc")
CXX=$([[ $duckdb_platform == "linux_arm64" ]] && echo "aarch64-linux-gnu-g++" || echo "g++")
RUN_TESTS=$([[ $duckdb_platform != "linux_arm64" ]] && echo "1" || echo "")
AARCH64_CROSS_COMPILE=$([[ $duckdb_platform == "linux_arm64" ]] && echo "1" || echo "")    
    
# Build the docker image
DOCKER_BUILDKIT=1 docker buildx build \
    --platform linux/amd64 \
    --build-arg VCPKG_BUILD=$VCPKG_BUILD \
    --build-arg PROJECT_DIR=$PROJECT_DIR \
    --build-arg VCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET \
    --build-arg VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH \
    --build-arg DUCKDB_PLATFORM=$DUCKDB_PLATFORM \
    --build-arg ENABLE_RUST=$ENABLE_RUST \
    --build-arg CC=$CC \
    --build-arg CXX=$CCX \
    --build-arg RUN_TESTS=$RUN_TESTS \
    --build-arg AARCH64_CROSS_COMPILE=$AARCH64_CROSS_COMPILE \
    --build-arg CONTAINER=$CONTAINER \
    --load \
    -t duckdb_extension_build:arm64 \
    -f $SCRIPT_DIR/linux.Dockerfile $PROJECT_DIR
