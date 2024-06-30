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
#include <arrow/api.h>

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

class BigQueryResult {
public:
	string execution_project;
	string storage_project;
	string dataset;
	string table;
	std::shared_ptr<arrow::Schema> schema;
	unique_ptr<bigquery_storage::BigQueryReadClient> client;
	unique_ptr<bigquery_storage_read::ReadSession> read_session;

	BigQueryResult(
		string execution_project,
		string storage_project,
		string dataset,
		string table,
		std::shared_ptr<arrow::Schema> schema_p,
		unique_ptr<bigquery_storage_read::ReadSession> read_session_p,
	 	unique_ptr<bigquery_storage::BigQueryReadClient> client_p):
		  execution_project(execution_project),
		  storage_project(storage_project),
		  dataset(dataset),
		  table(table),
		  schema(std::move(schema_p)),
		  read_session(std::move(read_session_p)),
		  client(std::move(client_p)) {}
	~BigQueryResult() {}

public:
	std::shared_ptr<arrow::RecordBatch> GetArrowRecordBatch(
    ::google::cloud::bigquery::storage::v1::ArrowRecordBatch const&
        record_batch_in,
    std::shared_ptr<arrow::Schema> schema);
	void PrintColumnNames(std::shared_ptr<arrow::RecordBatch> record_batch);
	void ProcessRecordBatch(std::shared_ptr<arrow::Schema> schema,
                        std::shared_ptr<arrow::RecordBatch> record_batch,
                        std::int64_t num_rows);
	void process();
private:

};

} // namespace duckdb
