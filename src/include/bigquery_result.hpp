//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdio>
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

class BigQueryResult {
public:
	BigQueryResult(
		bigquery_storage_read::ReadSession &read_session_p,
	 	bigquery_storage::BigQueryReadClient &client_p):
		 read_session(read_session_p), client(client_p) {}
	~BigQueryResult() {}

public:

private:
	bigquery_storage::BigQueryReadClient &client;
	bigquery_storage_read::ReadSession &read_session;
};

} // namespace duckdb
