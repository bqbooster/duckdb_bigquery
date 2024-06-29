#include "duckdb.hpp"

#include "bigquery_storage_scanner.hpp"
#include "bigquery_query.hpp"
#include "bigquery_result.hpp"
#include "bigquery_utils.hpp"
#include "bigquery_filter_pushdown.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_table_set.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

struct BigQueryStorageScannerLocalState : public LocalTableFunctionState {

};

struct BigQueryStorageScannerGlobalState : public GlobalTableFunctionState {
	explicit BigQueryStorageScannerGlobalState(unique_ptr<BigQueryResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<BigQueryResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> BigQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("Unimplemented BigQueryBind for BigQueryStorageScannerFunction");
}

static unique_ptr<GlobalTableFunctionState> BigQueryInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	Printer::Print("BigQueryInitGlobalState");
	auto &bind_data = input.bind_data->Cast<BigQueryStorageScannerBindData>();

	auto column_list = BigQueryUtils::BigQueryReadColumnListForTable(
	bind_data.execution_project,
	bind_data.database_name,
	bind_data.schema_name,
	bind_data.table_name
	);

	Printer::Print("column_list done");

	if(column_list){
	optional_ptr<BigQueryResult> bq_result = BigQueryUtils::BigQueryReadTable(
		bind_data.execution_project,
		bind_data.database_name,
		bind_data.schema_name,
		bind_data.table_name,
		column_list->GetColumnNames()
	);

	Printer::Print("created bq result done");

	//unique_ptr<BigQueryResult> bq_result_uniq = make_uniq<BigQueryResult>(bq_result);

		if(bq_result){
			 unique_ptr<BigQueryResult> res = unique_ptr<BigQueryResult>(bq_result.get());
			 Printer::Print("Preparing BigQueryStorageScannerGlobalState");
			return make_uniq<BigQueryStorageScannerGlobalState>(std::move(res));
		}
	}

	return nullptr;
	// auto &catalog = bind_data.table.catalog.Cast<BigQueryCatalog>();
}

static unique_ptr<LocalTableFunctionState> BigQueryInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	Printer::Print("BigQueryInitLocalState");
	return make_uniq<BigQueryStorageScannerLocalState>();
}

static void BigQueryStorageScanner(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Printer::Print("BigQueryStorageScanner main function");
	auto &gstate = data.global_state->Cast<BigQueryStorageScannerGlobalState>();
	auto &bind = data.bind_data->Cast<BigQueryStorageScannerBindData>();
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

static string BigQueryStorageScannerToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<BigQueryStorageScannerBindData>();
	// return database.schema.table
	return bind_data.database_name + "." + bind_data.schema_name + "." + bind_data.table_name;
}

static void BigQueryStorageScannerSerialize(Serializer &serializer,
								  const optional_ptr<FunctionData> bind_data_p,
                                  const TableFunction &function) {
	throw NotImplementedException("BigQueryStorageScannerSerialize");
}

static unique_ptr<FunctionData> BigQueryStorageScannerDeserialize(Deserializer &deserializer,
														TableFunction &function) {
	throw NotImplementedException("BigQueryStorageScannerDeserialize");
}

BigQueryStorageScannerFunction::BigQueryStorageScannerFunction(): TableFunction(
	"bigquery_storage_scanner",
	{LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, // arguments: database name, table name, execution_project
	BigQueryStorageScanner, BigQueryBind, BigQueryInitGlobalState, BigQueryInitLocalState) {
	to_string = BigQueryStorageScannerToString;
	serialize = BigQueryStorageScannerSerialize;
	deserialize = BigQueryStorageScannerDeserialize;
	projection_pushdown = false;
}

} // namespace duckdb
