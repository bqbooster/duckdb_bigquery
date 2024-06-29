//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

namespace duckdb {

class BigQuerySchemaEntry;
class BigQueryTableEntry;
class BigQueryTransaction;
class BigQueryResult;

class BigQueryUtils {
public:

	static unique_ptr<BigQueryTableEntry> BigQueryCreateBigQueryTableEntry(
	Catalog &catalog,
	BigQuerySchemaEntry * schema_entry,
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table);

	static unique_ptr<BigQueryResult> BigQueryReadTable(
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table,
	const vector<string> &column_names);

	static optional_ptr<ColumnList> BigQueryReadColumnListForTable(
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table);

	//static BigQueryConnectionParameters ParseConnectionParameters(const string &dsn);
	//static BIGQUERY *Connect(const string &dsn);

	//static LogicalType ToBigQueryType(const LogicalType &input);
	static LogicalType TypeToLogicalType(const std::string &bq_type);
	//static LogicalType FieldToLogicalType(ClientContext &context, BIGQUERY_FIELD *field);
	// static string TypeToString(const LogicalType &input);

	static string WriteIdentifier(const string &identifier);
	static string WriteLiteral(const string &identifier);
	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
};

} // namespace duckdb
