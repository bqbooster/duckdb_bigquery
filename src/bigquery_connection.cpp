#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "bigquery_connection.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static bool debug_bigquery_print_queries = false;

// static int bigquery_real_query(BIGQUERY *bigquery, const char *q, unsigned long length){return 0;} // TODO implement
// const char* bigquery_error(BIGQUERY *bigquery){return new char[1];} //TODO implement
// BIGQUERY_RES * bigquery_store_result(BIGQUERY *bigquery){return nullptr;} //TODO implement
// unsigned int bigquery_field_count(BIGQUERY *bigquery){return 0;} //TODO implement
// uint64_t bigquery_affected_rows(BIGQUERY *bigquery){return 0;} //TODO implement
// BIGQUERY_FIELD * bigquery_fetch_field_direct(BIGQUERY_RES *res,
//                                               unsigned int fieldnr){
// 												  return new BIGQUERY_FIELD();
// 											  } //TODO implement


// BigQueryConnection::BigQueryConnection(shared_ptr<OwnedBigQueryConnection> connection_p):
//  connection(std::move(connection_p)) {}

// BigQueryConnection::~BigQueryConnection() {
// 	Close();
// }

// BigQueryConnection::BigQueryConnection(BigQueryConnection &&other) noexcept {
// 	std::swap(connection, other.connection);
// 	std::swap(dsn, other.dsn);
// }

// BigQueryConnection &BigQueryConnection::operator=(BigQueryConnection &&other) noexcept {
// 	std::swap(connection, other.connection);
// 	std::swap(dsn, other.dsn);
// 	return *this;
// }

// BigQueryConnection BigQueryConnection::Open(const string &connection_string) {
// 	BigQueryConnection result;
// 	//result.connection = make_shared_ptr<OwnedBigQueryConnection>(BigQueryUtils::Connect(connection_string));
// 	result.dsn = connection_string;
// 	result.Execute("SET character_set_results = 'utf8mb4';");
// 	return result;
// }

// BIGQUERY_RES *BigQueryConnection::BigQueryExecute(const string &query) {
// 	if (BigQueryConnection::DebugPrintQueries()) {
// 		Printer::Print(query + "\n");
// 	}
// 	auto con = GetConn();
// 	int res = bigquery_real_query(con, query.c_str(), query.size());
// 	if (res != 0) {
// 		throw IOException("Failed to run query \"%s\": %s\n", query.c_str(), bigquery_error(con));
// 	}
// 	return bigquery_store_result(con);
// }

// unique_ptr<BigQueryResult> BigQueryConnection::Query(const string &query, optional_ptr<ClientContext> context) {
// 	auto con = GetConn();
// 	auto result = BigQueryExecute(query);
// 	auto field_count = bigquery_field_count(con);
// 	if (!result) {
// 		// no result set
// 		// this can happen in case of a statement like CREATE TABLE, INSERT, etc
// 		// check if this is the case with bigquery_field_count
// 		if (field_count != 0) {
// 			// no result but we expected a result
// 			throw IOException("Failed to fetch result for query \"%s\": %s\n", query.c_str(), bigquery_error(con));
// 		}
// 		// get the affected rows
// 		return make_uniq<BigQueryResult>(bigquery_affected_rows(con));
// 	} else {
// 		// result set
// 		if (!context) {
// 			return make_uniq<BigQueryResult>(result, field_count);
// 		}
// 		vector<BigQueryField> fields;
// 		for (idx_t i = 0; i < field_count; i++) {
// 			auto field = bigquery_fetch_field_direct(result, i);
// 			BigQueryField bigquery_field;
// 			if (field->name && field->name_length > 0) {
// 				bigquery_field.name = string(field->name, field->name_length);
// 			}
// 			//TODO bigquery_field.type = BigQueryUtils::FieldToLogicalType(*context, field);
// 			fields.push_back(std::move(bigquery_field));
// 		}

// 		return make_uniq<BigQueryResult>(result, std::move(fields));
// 	}
// }

// void BigQueryConnection::Execute(const string &query) {
// 	Query(query);
// }

// bool BigQueryConnection::IsOpen() {
// 	return connection.get();
// }

// void BigQueryConnection::Close() {
// 	if (!IsOpen()) {
// 		return;
// 	}
// 	connection = nullptr;
// }

// vector<IndexInfo> BigQueryConnection::GetIndexInfo(const string &table_name) {
// 	return vector<IndexInfo>();
// }

// void BigQueryConnection::DebugSetPrintQueries(bool print) {
// 	debug_bigquery_print_queries = print;
// }

// bool BigQueryConnection::DebugPrintQueries() {
// 	return debug_bigquery_print_queries;
// }

} // namespace duckdb
