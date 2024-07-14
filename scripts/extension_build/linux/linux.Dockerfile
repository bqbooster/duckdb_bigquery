# Use an official Ubuntu as a parent image
FROM ubuntu:24.04

# Define build arguments
ARG VCPKG_TARGET_TRIPLET
ARG VCPKG_TOOLCHAIN_PATH=/usr/app/vcpkg/scripts/buildsystems/vcpkg.cmake
ARG DUCKDB_PLATFORM
ARG ENABLE_RUST=0
ARG CC
ARG CXX
ARG RUN_TESTS
ARG AARCH64_CROSS_COMPILE
ARG PROJECT_DIR
ARG BUILD_SHELL=0

# Set environment variables using the build arguments
ENV VCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET
ENV VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH
ENV BUILD_SHELL=$BUILD_SHELL
ENV DUCKDB_PLATFORM=$DUCKDB_PLATFORM
ENV ENABLE_RUST=$ENABLE_RUST
ENV CC=$CC
ENV CXX=$CXX
ENV RUN_TESTS=$RUN_TESTS
ENV AARCH64_CROSS_COMPILE=$AARCH64_CROSS_COMPILE

# Install necessary packages
RUN apt-get update -y -qq && \
    apt-get install -y -qq software-properties-common && \
    add-apt-repository ppa:git-core/ppa && \
    apt-get update -y -qq && \
    apt-get install -y -qq --fix-missing \
    ninja-build \
    make \
    libssl-dev \
    wget \
    openjdk-8-jdk \
    zip \
    maven \
    unixodbc-dev \
    libssl-dev \
    libcurl4-gnutls-dev \
    libexpat1-dev \
    gettext \
    unzip \
    build-essential \
    checkinstall \
    libffi-dev \
    curl \
    libz-dev \
    openssh-client \
    flex \
    bison \
    git \
    pkg-config && \
    if [ "${DUCKDB_PLATFORM}" = "linux_arm64" ]; then \
    export CMAKE_ARCH=aarch64; \
    else \
    export CMAKE_ARCH=x86_64; \
    fi && \
    wget https://github.com/Kitware/CMake/releases/download/v3.30.0/cmake-3.30.0-linux-$CMAKE_ARCH.sh && \
    chmod +x cmake-3.30.0-linux-$CMAKE_ARCH.sh && \
    ./cmake-3.30.0-linux-$CMAKE_ARCH.sh --skip-license --prefix=/usr/local && \
    cmake --version

RUN if [ "$AARCH64_CROSS_COMPILE" = "1" ]; then \
apt-get install -y -qq \
gcc-aarch64-linux-gnu \
g++-aarch64-linux-gnu; \
else \
apt-get install -y -qq --fix-missing \
gcc-multilib \
g++-multilib \ 
libc6-dev-i386 \
lib32readline6-dev; \
fi
    

# Set the working directory
WORKDIR /usr/app

# Clone the vcpkg repository if vcpkg is not copied in /usr/app/vcpkg directory
RUN if [ ! -d "/usr/app/vcpkg" ]; then \
    git clone https://github.com/microsoft/vcpkg.git && \
    cd vcpkg && \
    git fetch && \
    git checkout master; \
    fi

# Bootstrap vcpkg
RUN cd /usr/app && \
     export VCPKG_FORCE_SYSTEM_BINARIES=1 && \
    ./vcpkg/bootstrap-vcpkg.sh

# Clone and setup the DuckDB repository
# RUN git clone https://github.com/duckdb/duckdb.git && \
#     cd duckdb && \
#     git checkout ${REPO_REF}

# Install specific version of Git
# RUN if [ "${DUCKDB_PLATFORM}" != "linux_amd64_gcc4" ]; then \
#     wget https://github.com/git/git/archive/refs/tags/v2.45.2.tar.gz && \
#     tar xvf v2.45.2.tar.gz && \
#     cd git-2.45.2 && \
#     make && \
#     make prefix=/usr install && \
#     git --version && \
#     cd ..; \
#     fi

# Setup ManyLinux2014 if necessary
RUN if [ "${DUCKDB_PLATFORM}" = "linux_amd64_gcc4" ]; then \
    ./duckdb/scripts/setup_manylinux2014.sh general aws-cli ccache ssh python_alias openssl vcpkg; \
    fi

# Install Rust if necessary
RUN if [ "${ENABLE_RUST}" != "0" ]; then \
    curl --proto "https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
    fi

# Checkout the required git reference
RUN if [ -n "${REPO_REF}" ]; then \
    cd duckdb && \
    git checkout ${REPO_REF}; \
    fi

COPY scripts/extension_build/linux/entrypoint.sh /usr/app/scripts/extension_build/linux/entrypoint.sh

CMD ["/usr/app/scripts/extension_build/linux/entrypoint.sh"]