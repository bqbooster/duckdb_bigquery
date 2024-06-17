include_directories(include)

add_subdirectory(storage)

add_library(
  bigquery_ext_library OBJECT
  bigquery_connection.cpp
  bigquery_execute.cpp
  bigquery_extension.cpp
  bigquery_filter_pushdown.cpp
  bigquery_query.cpp
  bigquery_scanner.cpp
  bigquery_storage.cpp
  bigquery_utils.cpp)

#  set(ALL_OBJECT_FILES
#    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bigquery_ext_library>
#    PARENT_SCOPE)

# Ensure storage objects are included
set(ALL_OBJECT_FILES
${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bigquery_ext_library> $<TARGET_OBJECTS:bigquery_ext_storage>
PARENT_SCOPE)