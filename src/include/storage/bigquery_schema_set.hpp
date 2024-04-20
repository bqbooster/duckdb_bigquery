//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class BigQuerySchemaSet : public BigQueryCatalogSet {
public:
	explicit BigQuerySchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
