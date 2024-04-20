//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_scanner.hpp
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

struct BigQueryBindData : public FunctionData {
	explicit BigQueryBindData(BigQueryTableEntry &table) : table(table) {
	}

	BigQueryTableEntry &table;
	vector<BigQueryType> bigquery_types;
	vector<string> names;
	vector<LogicalType> types;
	string limit;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("BigQueryBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

class BigQueryScanFunction : public TableFunction {
public:
	BigQueryScanFunction();
};

class BigQueryQueryFunction : public TableFunction {
public:
	BigQueryQueryFunction();
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
