//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "bigquery_utils.hpp"
#include <cstdio>
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

	// static void * bigquery_free_session(
	// 	bigquery_storage_read::ReadSession &read_session,
	// 	bigquery_storage::BigQueryReadClient &client
	// ) {
	// 	 return nullptr;
	// } // TODO: Implement this function
	// static BIGQUERY_ROW bigquery_fetch_row(BIGQUERY_RES *res) {
	// 	// Allocate an array of char* pointers
    // char **rows = new char*[10]; // for example, an array of 10 elements

    // // Initialize the array with C-style strings
    // for (int i = 0; i < 10; ++i) {
    //     rows[i] = new char[20]; // allocate space for each string
    //     snprintf(rows[i], 20, "Row %d", i); // fill each string with some data
    // }
    // // Return the pointer to the array
    // return rows;
	// } // TODO: Implement this function

	// static unsigned long * bigquery_fetch_lengths(BIGQUERY_RES *res) {
	// 	// Allocate an array of unsigned long
	// 	unsigned long *lengths = new unsigned long[10]; // for example, an array of 10 elements
	// 	// Initialize the array
	// 	for (int i = 0; i < 10; ++i) {
	// 		lengths[i] = i;
	// 	}
	// 	// Return the pointer to the array
	// 	return lengths;
	// } // TODO: Implement this function

// struct BigQueryField {
// 	string name;
// 	LogicalType type;
// };

class BigQueryResult {
public:
	BigQueryResult(
		bigquery_storage_read::ReadSession &read_session_p,
	 	bigquery_storage::BigQueryReadClient &client_p):
		 read_session(read_session_p), client(client_p) {}

	// BigQueryResult(BIGQUERY_RES *res_p, vector<BigQueryField> fields_p)
	//     : res(res_p), field_count(fields_p.size()), fields(std::move(fields_p)) {
	// }
	//BigQueryResult(idx_t affected_rows) : affected_rows(affected_rows) {}
	~BigQueryResult() {
		// if (res) {
		// 	bigquery_free_session(res);
		// }
	}

public:
	// string GetString(idx_t col) {
	// 	D_ASSERT(res);
	// 	return string(GetNonNullValue(col), lengths[col]);
	// }
	// string_t GetStringT(idx_t col) {
	// 	D_ASSERT(res);
	// 	return string_t(GetNonNullValue(col), lengths[col]);
	// }
	// int32_t GetInt32(idx_t col) {
	// 	return atoi(GetNonNullValue(col));
	// }
	// int64_t GetInt64(idx_t col) {
	// 	return atoll(GetNonNullValue(col));
	// }
	// bool GetBool(idx_t col) {
	// 	return strcmp(GetNonNullValue(col), "t");
	// }
	// bool IsNull(idx_t col) {
	// 	return !GetValueInternal(col);
	// }
	// bool Next() {
	// 	if (!res) {
	// 		throw InternalException("BigQueryResult::Next called without result");
	// 	}
	// 	bigquery_row = bigquery_fetch_row(res);
	// 	lengths = bigquery_fetch_lengths(res);
	// 	return bigquery_row;
	// }
	// idx_t AffectedRows() {
	// 	if (affected_rows == idx_t(-1)) {
	// 		throw InternalException("BigQueryResult::AffectedRows called for result "
	// 		                        "that didn't affect any rows");
	// 	}
	// 	return affected_rows;
	// }
	// idx_t ColumnCount() {
	// 	return field_count;
	// }
	// const vector<BigQueryField> &Fields() {
	// 	return fields;
	// }

private:
	bigquery_storage::BigQueryReadClient &client;
	bigquery_storage_read::ReadSession &read_session;
	//BIGQUERY_RES *res = nullptr;
	//idx_t affected_rows = idx_t(-1);
	//BIGQUERY_ROW bigquery_row = nullptr;
	//unsigned long *lengths = nullptr;
	//idx_t field_count = 0;
	//vector<BigQueryField> fields;

	// char *GetNonNullValue(idx_t col) {
	// 	auto val = GetValueInternal(col);
	// 	if (!val) {
	// 		throw InternalException("BigQueryResult::GetNonNullValue called for a NULL value");
	// 	}
	// 	return val;
	// }

	// char *GetValueInternal(idx_t col) {
	// 	if (!bigquery_row) {
	// 		throw InternalException("BigQueryResult::GetValueInternal called without row");
	// 	}
	// 	if (col >= field_count) {
	// 		throw InternalException("BigQueryResult::GetValueInternal row out of range of field count");
	// 	}
	// 	return bigquery_row[col];
	// }
};

} // namespace duckdb
