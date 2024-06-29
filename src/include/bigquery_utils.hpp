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
#include "bigquery_result.hpp"
//#include "bigquery.h"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

// typedef struct BIGQUERY_FIELD {
//   char *name;               /* Name of column */
//   char *org_name;           /* Original column name, if an alias */
//   char *table;              /* Table of column if column was a field */
//   char *org_table;          /* Org table name, if table was an alias */
//   char *db;                 /* Database for table */
//   char *catalog;            /* Catalog for table */
//   char *def;                /* Default value (set by mysql_list_fields) */
//   unsigned long length;     /* Width of column (create length) */
//   unsigned long max_length; /* Max width for selected set */
//   unsigned int name_length;
//   unsigned int org_name_length;
//   unsigned int table_length;
//   unsigned int org_table_length;
//   unsigned int db_length;
//   unsigned int catalog_length;
//   unsigned int def_length;
//   unsigned int flags;         /* Div flags */
//   unsigned int decimals;      /* Number of decimals in field */
//   unsigned int charsetnr;     /* Character set */
// //   enum enum_field_types type; /* Type of field. See mysql_com.h for types */
// //   void *extension;
// } BIGQUERY_FIELD;

//typedef void* BIGQUERY_RES; // TODO this is a placeholder
//typedef char ** BIGQUERY_ROW; // TODO this is a placeholder
//typedef void* BIGQUERY; // TODO this is a placeholder
//static void bigquery_close(BIGQUERY *connection) { return; } // TODO this is a placeholder

class BigQuerySchemaEntry;
class BigQueryTransaction;

// struct BigQueryTypeData {
// 	string type_name;
// 	string column_type;
// 	int64_t precision;
// 	int64_t scale;
// };

//enum class BigQueryTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

// struct BigQueryType {
// 	idx_t oid = 0;
// 	BigQueryTypeAnnotation info = BigQueryTypeAnnotation::STANDARD;
// 	vector<BigQueryType> children;
// };

// struct BigQueryConnectionParameters {
// 	string host;
// 	string user;
// 	string passwd;
// 	string db;
// 	uint32_t port = 0;
// 	string unix_socket;
// 	//idx_t client_flag = CLIENT_COMPRESS | CLIENT_IGNORE_SIGPIPE | CLIENT_MULTI_STATEMENTS;
// };

class BigQueryUtils {
public:

	static optional_ptr<BigQueryResult> BigQueryReadTable(
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
