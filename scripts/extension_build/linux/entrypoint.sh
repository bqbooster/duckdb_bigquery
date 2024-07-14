#!/bin/bash
set -eo pipefail

# Build the extension
cd /usr/app 
GEN=ninja make release

# Test the extension
if [ -n "${RUN_TESTS}" ]; then \
    cd /workspace && \
    make test; \
fi