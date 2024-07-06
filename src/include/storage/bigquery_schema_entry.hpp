//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/bigquery_table_set.hpp"
#include "storage/bigquery_index_set.hpp"

namespace duckdb {
class BigQueryCatalog;
class BigQueryTableEntry;
class BigQueryTransaction;

class BigQuerySchemaEntry : public SchemaCatalogEntry {
public:
	BigQuerySchemaEntry(Catalog &catalog, CreateSchemaInfo &info);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;

private:
	void AlterTable(BigQueryTransaction &transaction, RenameTableInfo &info);
	void AlterTable(BigQueryTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(BigQueryTransaction &transaction, AddColumnInfo &info);
	void AlterTable(BigQueryTransaction &transaction, RemoveColumnInfo &info);

	void TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name);

	BigQueryCatalogSet &GetCatalogSet(CatalogType type);

private:
	BigQueryTableSet tables;
	//BigQueryIndexSet indexes;
};

} // namespace duckdb
