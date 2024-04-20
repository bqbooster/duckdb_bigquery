#include "storage/bigquery_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

BigQueryIndexEntry::BigQueryIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
                                 string table_name_p)
    : IndexCatalogEntry(catalog, schema, info), table_name(std::move(table_name_p)) {
}

string BigQueryIndexEntry::GetSchemaName() const {
	return schema.name;
}

string BigQueryIndexEntry::GetTableName() const {
	return table_name;
}

} // namespace duckdb
