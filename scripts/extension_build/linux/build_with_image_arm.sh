#!/bin/bash
set -exo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_DIR="$SCRIPT_DIR/../../../"
echo script dir $PROJECT_DIR


docker run \
        -ti \
        -e VCPKG_FORCE_SYSTEM_BINARIES=1 \
        -e VCPKG_TOOLCHAIN_PATH=/usr/app/vcpkg/scripts/buildsystems/vcpkg.cmake \
        -v $PROJECT_DIR/.git:/usr/app/.git:rw \
        -v $PROJECT_DIR/build_docker:/usr/app/build:rw \
        -v $PROJECT_DIR/src:/usr/app/src \
        -v $PROJECT_DIR/test:/usr/app/test \
        -v $PROJECT_DIR/duckdb:/usr/app/duckdb:rw \
        -v $PROJECT_DIR/extension-ci-tools:/usr/app/extension-ci-tools:rw \
        -v $PROJECT_DIR/vcpkg/build:/usr/app/vcpkg/build:rw \
        -v $PROJECT_DIR/vcpkg/packages:/usr/app/vcpkg/packages:rw \
        -v $PROJECT_DIR/vcpkg/downloads:/usr/app/vcpkg/downloads:rw \
        -v $PROJECT_DIR/vcpkg/buildtrees:/usr/app/vcpkg/buildtrees:rw \
        -v $PROJECT_DIR/vcpkg/ports:/usr/app/vcpkg/ports:rw \
        -v $PROJECT_DIR/vcpkg/installed:/usr/app/vcpkg/installed:rw \
        -v $PROJECT_DIR/vcpkg.json:/usr/app/vcpkg.json \
        -v $PROJECT_DIR/CMakeLists.txt:/usr/app/CMakeLists.txt \
        -v $PROJECT_DIR/extension_config.cmake:/usr/app/extension_config.cmake \
        -v $PROJECT_DIR/Makefile:/usr/app/Makefile \
        duckdb_extension_build:arm64
