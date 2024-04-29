#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "bigquery_scanner.hpp"
#include "bigquery_storage.hpp"
#include "duckdb_bigquery_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_optimizer.hpp"

//#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

using namespace duckdb;

unique_ptr<BaseSecret> CreateBigQuerySecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "bigquery", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else if (lower_name == "socket") {
			result->secret_map["socket"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateBigQuerySecretFunction: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetBigQuerySecretParameters(CreateSecretFunction &function) {
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR;
	function.named_parameters["socket"] = LogicalType::VARCHAR;
}

static void LoadInternal(DatabaseInstance &db) {

	BigQueryClearCacheFunction clear_cache_func;
	ExtensionUtil::RegisterFunction(db, clear_cache_func);

	BigQueryExecuteFunction execute_function;
	ExtensionUtil::RegisterFunction(db, execute_function);

	BigQueryQueryFunction query_function;
	ExtensionUtil::RegisterFunction(db, query_function);

	SecretType secret_type;
	secret_type.name = "bigquery";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(db, secret_type);

	CreateSecretFunction bigquery_secret_function = {"bigquery", "config", CreateBigQuerySecretFunction};
	SetBigQuerySecretParameters(bigquery_secret_function);
	ExtensionUtil::RegisterFunction(db, bigquery_secret_function);

	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["duckdb_bigquery"] = make_uniq<BigQueryStorageExtension>();

	config.AddExtensionOption("bigquery_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown (currently experimental)", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));
	// config.AddExtensionOption("bigquery_debug_show_queries", "DEBUG SETTING: print all queries sent to BigQuery to stdout",
	//                           LogicalType::BOOLEAN, Value::BOOLEAN(false), SetBigQueryDebugQueryPrint);
	config.AddExtensionOption("bigquery_tinyint1_as_boolean", "Whether or not to convert TINYINT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), BigQueryClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("bigquery_bit1_as_boolean", "Whether or not to convert BIT(1) columns to BOOLEAN",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), BigQueryClearCacheFunction::ClearCacheOnSetting);

	OptimizerExtension bigquery_optimizer;
	bigquery_optimizer.optimize_function = BigQueryOptimizer::Optimize;
	config.optimizer_extensions.push_back(std::move(bigquery_optimizer));
}

void DuckdbBigqueryExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

extern "C" {

DUCKDB_EXTENSION_API void duckdb_bigquery_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *duckdb_bigquery_version() {
	return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void duckdb_bigquery_storage_init(DBConfig &config) {
    config.storage_extensions["duckdb_bigquery"] = make_uniq<BigQueryStorageExtension>();
}

}
