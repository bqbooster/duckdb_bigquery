cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME duckdb_bigquery)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(google_cloud_cpp_bigquery REQUIRED)
find_package(Threads)

project(${TARGET_NAME})
add_subdirectory(src)

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

set(BIGQUERY_INCLUDE_DIR
${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include/)
message(STATUS "BIGQUERY_INCLUDE_DIR: ${BIGQUERY_INCLUDE_DIR}")
message(STATUS "Current working directory: ${CMAKE_CURRENT_SOURCE_DIR}")

include_directories(${BIGQUERY_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME}
                           PRIVATE include ${BIGQUERY_INCLUDE_DIR})

target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto google-cloud-cpp::bigquery Threads::Threads)
