#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "bigquery_scanner.hpp"

namespace duckdb {

static bool TableIsInternal(const SchemaCatalogEntry &schema, const string &name) {
	if (schema.name != "INFORMATION_SCHEMA") {
		return false;
	}
	// this is a list of all system tables
	// TODO fill the system tables list
	unordered_set<string> system_tables {"partitions"};
	return system_tables.find(name) != system_tables.end();
}

BigQueryTableEntry::BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = TableIsInternal(schema, name);
}

BigQueryTableEntry::BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BigQueryTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = TableIsInternal(schema, name);
}

unique_ptr<BaseStatistics> BigQueryTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void BigQueryTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                                   ClientContext &context) {
}

TableFunction BigQueryTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	Printer::Print("BigQueryTableEntry::GetScanFunction");

	auto scan_bind_data = make_uniq<BigQueryScanBindData>(*this);

	for (auto &col : columns.Logical()) {
		scan_bind_data->column_types.push_back(col.GetType());
		scan_bind_data->column_names.push_back(col.GetName());
	}

	bind_data = std::move(scan_bind_data);

	auto function = BigQueryScanFunction();
	Value filter_pushdown;
	if (context.TryGetCurrentSetting("bigquery_experimental_filter_pushdown", filter_pushdown)) {
		function.filter_pushdown = BooleanValue::Get(filter_pushdown);
	}
	return function;
}

TableStorageInfo BigQueryTableEntry::GetStorageInfo(ClientContext &context) {
	//auto &transaction = Transaction::Get(context, catalog).Cast<BigQueryTransaction>();
	//auto &db = transaction.GetConnection();
	TableStorageInfo result;
	result.cardinality = 0;
	//result.index_info = db.GetIndexInfo(name);
	return result;
}

} // namespace duckdb
