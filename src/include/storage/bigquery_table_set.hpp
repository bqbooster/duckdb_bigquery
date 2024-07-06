//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class BigQueryConnection;
class BigQueryResult;
class BigQuerySchemaEntry;

class BigQueryTableSet : public BigQueryInSchemaSet {
public:
	explicit BigQueryTableSet(BigQuerySchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<BigQueryTableInfo> GetTableInfo(ClientContext &context, BigQuerySchemaEntry &schema,
	                                               const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, BigQueryResult &result, BigQueryTableInfo &table_info,
	                      idx_t column_offset = 0);
};

} // namespace duckdb
