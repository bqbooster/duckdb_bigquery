#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "bigquery_scanner.hpp"
#include "bigquery_query.hpp"
#include "bigquery_result.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_table_set.hpp"
#include "bigquery_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

struct BigQueryScannerLocalState : public LocalTableFunctionState {};

struct BigQueryScannerGlobalState : public GlobalTableFunctionState {
	explicit BigQueryScannerGlobalState(unique_ptr<BigQueryResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<BigQueryResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> BigQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("Unimplemented BigQueryBind for BigQueryScanFunction");
}

static unique_ptr<GlobalTableFunctionState> BigQueryInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	Printer::Print("BigQueryInitGlobalState");
	auto &bind_data = input.bind_data->Cast<BigQueryScanBindData>();
	auto &entry = bind_data.table;
	auto &bigquery_catalog = entry.catalog.Cast<BigQueryCatalog>();
	auto bigquery_result = BigQueryUtils::BigQueryReadTable(
		bigquery_catalog.execution_project,
		bigquery_catalog.storage_project,
		entry.schema.name,
		entry.name,
		bind_data.column_names);

	return make_uniq<BigQueryScannerGlobalState>(std::move(bigquery_result));
}

static unique_ptr<LocalTableFunctionState> BigQueryInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	Printer::Print("BigQueryInitLocalState");
	return make_uniq<BigQueryScannerLocalState>();
}

static void BigQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Printer::Print("BigQueryScan");
	auto &gstate = data.global_state->Cast<BigQueryScannerGlobalState>();
	auto &result = gstate.result;
	auto &read_session = result->read_session;
	auto &client = result->client;
	auto &schema = result->schema;

	Printer::Print("BigQueryResult");


	// Read rows from the ReadSession.
	constexpr int kRowOffset = 0;
	auto read_rows = client->ReadRows(read_session->streams(0).name(), kRowOffset);

	std::int64_t num_rows = 0;
	std::int64_t record_batch_count = 0;
	for (auto const& read_rows_response : read_rows) {
		if (read_rows_response.ok()) {
		std::shared_ptr<arrow::RecordBatch> record_batch =
			result->GetArrowRecordBatch(read_rows_response->arrow_record_batch(), schema);

		if (record_batch_count == 0) {
			result->PrintColumnNames(record_batch);
		}

		result->ProcessRecordBatch(schema, record_batch, num_rows);
		num_rows += read_rows_response->row_count();
		++record_batch_count;
		}
	}
  	return;

	//gstate.result->process();
	idx_t r;
	//gstate.varchar_chunk.Reset();
	// for (r = 0; r < STANDARD_VECTOR_SIZE; r++) {
	// 	if (!gstate.result->Next()) {
	// 		// exhausted result
	// 		break;
	// 	}
	// 	for (idx_t c = 0; c < output.ColumnCount(); c++) {
	// 		auto &vec = gstate.varchar_chunk.data[c];
	// 		if (gstate.result->IsNull(c)) {
	// 			FlatVector::SetNull(vec, r, true);
	// 		} else {
	// 			auto string_data = FlatVector::GetData<string_t>(vec);
	// 			string_data[r] = StringVector::AddStringOrBlob(vec, gstate.result->GetStringT(c));
	// 		}
	// 	}
	// }
	if (r == 0) {
		// done
		return;
	}
	//D_ASSERT(output.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	// for (idx_t c = 0; c < output.ColumnCount(); c++) {
	// 	switch (output.data[c].GetType().id()) {
	// 	case LogicalTypeId::BLOB:
	// 		// blobs are sent over the wire as-is
	// 		output.data[c].Reinterpret(gstate.varchar_chunk.data[c]);
	// 		break;
	// 	case LogicalTypeId::BOOLEAN:
	// 		// booleans can be sent either as numbers ('0' or '1') or as bits ('\0' or
	// 		// '\1')
	// 		//CastBoolFromBigQuery(context, gstate.varchar_chunk.data[c], output.data[c], r);
	// 		break;
	// 	default: {
	// 		string error;
	// 		VectorOperations::TryCast(context, gstate.varchar_chunk.data[c], output.data[c], r, &error);
	// 		break;
	// 	}
	// 	}
	// }
	output.SetCardinality(r);
}

static string BigQueryScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<BigQueryScanBindData>();
	return bind_data.table.name;
}

static void BigQueryScanSerialize(Serializer &serializer,
								  const optional_ptr<FunctionData> bind_data_p,
                                  const TableFunction &function) {
	throw NotImplementedException("BigQueryScanSerialize");
}

static unique_ptr<FunctionData> BigQueryScanDeserialize(Deserializer &deserializer,
														TableFunction &function) {
	throw NotImplementedException("BigQueryScanDeserialize");
}

BigQueryScanFunction::BigQueryScanFunction(): TableFunction(
	"bigquery_scan",
	{LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	BigQueryScan, BigQueryBind, BigQueryInitGlobalState, BigQueryInitLocalState) {
	to_string = BigQueryScanToString;
	serialize = BigQueryScanSerialize;
	deserialize = BigQueryScanDeserialize;
	projection_pushdown = false;
}

} // namespace duckdb
