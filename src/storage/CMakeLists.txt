add_library(
  bigquery_ext_storage OBJECT
  bigquery_catalog.cpp
  bigquery_catalog_set.cpp
  bigquery_clear_cache.cpp
  bigquery_execute_query.cpp
  bigquery_index.cpp
  bigquery_index_entry.cpp
  bigquery_index_set.cpp
  bigquery_insert.cpp
  bigquery_optimizer.cpp
  bigquery_result.cpp
  bigquery_schema_entry.cpp
  bigquery_schema_set.cpp
  bigquery_table_entry.cpp
  bigquery_table_set.cpp
  bigquery_transaction.cpp
  bigquery_transaction_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bigquery_ext_storage>
    PARENT_SCOPE)
