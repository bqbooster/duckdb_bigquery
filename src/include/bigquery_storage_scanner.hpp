//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_storage_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "bigquery_utils.hpp"
#include "bigquery_connection.hpp"

namespace duckdb {
class BigQueryTableEntry;
class BigQueryTransaction;

struct BigQueryStorageScannerBindData : public FunctionData {
	explicit BigQueryStorageScannerBindData(string database_name, string schema_name, string table_name, string execution_project):
	database_name(database_name), schema_name(schema_name), table_name(table_name), execution_project(execution_project) {}

	string database_name;
	string schema_name;
	string table_name;
	string execution_project;
	//BigQueryTableEntry &table;
	//vector<string> column_names;
	//vector<LogicalType> column_types;
	//string limit;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("BigQueryBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

class BigQueryStorageScannerFunction : public TableFunction {
public:
	BigQueryStorageScannerFunction();
};

class BigQueryClearCacheFunction : public TableFunction {
public:
	BigQueryClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class BigQueryExecuteFunction : public TableFunction {
public:
	BigQueryExecuteFunction();
};

} // namespace duckdb
