//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_index_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

class BigQueryIndexEntry : public IndexCatalogEntry {
public:
	BigQueryIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info, string table_name);

	string table_name;

public:
	string GetSchemaName() const override;
	string GetTableName() const override;
};

} // namespace duckdb
