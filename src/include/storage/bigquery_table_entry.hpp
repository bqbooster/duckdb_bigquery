//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct BigQueryTableInfo {
	BigQueryTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
	}
	BigQueryTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
	}
	BigQueryTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
};

class BigQueryTableEntry : public TableCatalogEntry {
public:
	BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BigQueryTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                                   ClientContext &context) override;
};

} // namespace duckdb
