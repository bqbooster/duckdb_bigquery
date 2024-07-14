cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME duckdb_bigquery)
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 17)

find_package(google_cloud_cpp_bigquery REQUIRED)
find_package(google_cloud_cpp_common REQUIRED)
find_package(google_cloud_cpp_grpc_utils REQUIRED)
find_package(google_cloud_cpp_storage REQUIRED)
find_package(cpprestsdk REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(Arrow REQUIRED)
find_package(Threads)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(BIGQUERY_INCLUDE_DIR
${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include/)
include_directories(${BIGQUERY_INCLUDE_DIR})

add_subdirectory(src)

# set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})

# Loadable binary
target_include_directories(${LOADABLE_EXTENSION_NAME}
                           PRIVATE include ${BIGQUERY_INCLUDE_DIR})
target_link_libraries(${LOADABLE_EXTENSION_NAME} 
OpenSSL::SSL OpenSSL::Crypto
google-cloud-cpp::bigquery google-cloud-cpp::common
google-cloud-cpp::grpc_utils google-cloud-cpp::storage
cpprestsdk::cpprest
nlohmann_json::nlohmann_json
Threads::Threads
Arrow::arrow_static
)

add_subdirectory(test)