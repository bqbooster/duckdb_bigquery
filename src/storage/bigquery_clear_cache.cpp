#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "bigquery_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/bigquery_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ClearCacheFunctionData>();
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void ClearBigQueryCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = db_ref.get();
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "bigquery") {
			continue;
		}
		catalog.Cast<BigQueryCatalog>().ClearCache();
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	ClearBigQueryCaches(context);
	data.finished = true;
}

void BigQueryClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ClearBigQueryCaches(context);
}

BigQueryClearCacheFunction::BigQueryClearCacheFunction()
    : TableFunction("bigquery_clear_cache", {}, ClearCacheFunction, ClearCacheBind) {
}
} // namespace duckdb
