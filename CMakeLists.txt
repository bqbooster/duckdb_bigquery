cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME duckdb_bigquery)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(google_cloud_cpp_bigquery CONFIG REQUIRED)
find_package(google_cloud_cpp_bigquery_mocks CONFIG REQUIRED)
find_package(google_cloud_cpp_common CONFIG REQUIRED)
find_package(google_cloud_cpp_googleapis CONFIG REQUIRED)
find_package(google_cloud_cpp_grpc_utils CONFIG REQUIRED)

project(${TARGET_NAME})
#include_directories(src/include)
add_subdirectory(src)

#set(PARAMETERS "-no-warnings")
#set(EXTENSION_SOURCES src/bigquery_extension.cpp)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} " " ${ALL_OBJECT_FILES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::bigquery google-cloud-cpp::bigquery_protos google-cloud-cpp::cloud_bigquery_protos)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::bigquery google-cloud-cpp::bigquery_protos google-cloud-cpp::cloud_bigquery_protos)

target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::bigquery_mocks)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::bigquery_mocks)

target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::common)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::common)

target_link_libraries(${EXTENSION_NAME} gRPC::grpc gRPC::grpc++ google-cloud-cpp::api_log_protos google-cloud-cpp::api_auth_protos)
target_link_libraries(${LOADABLE_EXTENSION_NAME} gRPC::grpc gRPC::grpc++ google-cloud-cpp::api_log_protos google-cloud-cpp::api_auth_protos)

target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::grpc_utils)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::grpc_utils)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
