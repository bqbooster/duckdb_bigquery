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
#include <string>
#include <string_view>
#include <algorithm>

#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include "google/cloud/credentials.h"
#include "bigquery_utils.hpp"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

struct BigQueryScannerLocalState : public LocalTableFunctionState {};

struct BigQueryScannerGlobalState : public GlobalTableFunctionState {
	explicit BigQueryScannerGlobalState(string execution_project,
		string storage_project,
		string dataset,
		string table,
		unique_ptr<bigquery_storage_read::ReadSession> read_session_p,
		std::shared_ptr<google::cloud::bigquery_storage_v1::BigQueryReadConnection> connection_p,
		idx_t limit,
		idx_t offset,
		bool has_limit
		):
		  execution_project(execution_project),
		  storage_project(storage_project),
		  dataset(dataset),
		  table(table),
		  connection(std::move(connection_p)),
		  read_session(std::move(read_session_p)),
		  current_offset(offset),
		  limit(limit),
		  global_row_count(0),
		  has_limit(has_limit)
		  {}

	string execution_project;
	string storage_project;
	string dataset;
	string table;
	std::shared_ptr<arrow::Schema> schema;
	std::shared_ptr<google::cloud::bigquery_storage_v1::BigQueryReadConnection> connection;
	unique_ptr<bigquery_storage_read::ReadSession> read_session;
	idx_t limit;
	bool has_limit;

	idx_t current_offset;
	idx_t global_row_count;

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
	// Prepare the BigQuery Client
	//Printer::Print("BigQueryInitGlobalState");
	auto &bind_data = input.bind_data->Cast<BigQueryScanBindData>();
	auto &entry = bind_data.table;
	auto &bigquery_catalog = entry.catalog.Cast<BigQueryCatalog>();

	auto execution_project = bigquery_catalog.execution_project;
	auto storage_project = bigquery_catalog.storage_project;
	auto dataset = entry.schema.name;
	auto table = entry.name;
	auto column_names = bind_data.column_names;
	auto limit = bind_data.limit;
	auto offset = bind_data.offset;
	auto has_limit = bind_data.has_limit;
	auto service_account_json = bigquery_catalog.service_account_json;

	//Printer::Print("BigQueryReadTable: " + execution_project + " " + storage_project + "." + dataset + "." + table);
	//Printer::Print("limit: " + to_string(limit) + " offset: " + to_string(offset) + " has_limit: " + to_string(has_limit));
  	// table_name should be in the format:
  	// "projects/<project-table-resides-in>/datasets/<dataset-table_resides-in>/tables/<table
  	// name>" The project values in project_name and table_name do not have to be
  	// identical.
  	std::string const table_name = "projects/" + storage_project + "/datasets/" + dataset + "/tables/" + table;

	//constexpr int max_streams = 1;

	std::shared_ptr<google::cloud::bigquery_storage_v1::BigQueryReadConnection> connection;
	if(service_account_json.empty()){
		connection = bigquery_storage::MakeBigQueryReadConnection();
	}
	else {
		//Printer::Print("GetAccessToken service_account_json: " + service_account_json);
		auto options = google::cloud::Options{}
	.set<google::cloud::UnifiedCredentialsOption>(
		google::cloud::MakeServiceAccountCredentials(service_account_json));
		connection = bigquery_storage::MakeBigQueryReadConnection(options);
	}

	// Create the ReadSession.
	auto read_session = make_uniq<bigquery_storage_read::ReadSession>();
	read_session->set_data_format(google::cloud::bigquery::storage::v1::DataFormat::ARROW);
	read_session->set_table(table_name);
	for(auto &column_id : input.column_ids){
			auto column_name = bind_data.column_names[column_id];
			//Printer::Print("Adding column: " + column_name);
			read_session->mutable_read_options()->add_selected_fields(column_name);
	}
	auto filters = BigQueryFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.column_names);
	//Printer::Print("filters: " + filters);
	if(!filters.empty()){
		read_session->mutable_read_options()->set_row_restriction(filters);
	}
	//Printer::Print("column_names size: " + to_string(column_names.size()));

	return make_uniq<BigQueryScannerGlobalState>(
			execution_project,
			storage_project,
			dataset,
			table,
			std::move(read_session),
			std::move(connection),
			limit,
			offset,
			has_limit
	);
}

static unique_ptr<LocalTableFunctionState> BigQueryInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	//Printer::Print("BigQueryInitLocalState");
	return make_uniq<BigQueryScannerLocalState>();
}

static void BigQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	//Printer::Print("BigQueryScan");
	auto &gstate = data.global_state->Cast<BigQueryScannerGlobalState>();
	auto connection = gstate.connection;
	auto execution_project = gstate.execution_project;
	auto storage_project = gstate.storage_project;
	auto dataset = gstate.dataset;
	auto table = gstate.table;
	auto &read_session = gstate.read_session;

	//Printer::Print("gstate.has_limit: " + to_string(gstate.has_limit));

	//Printer::Print("BigQueryScan: got rows response");
	std::string const project_name = "projects/" + execution_project;
	auto client = bigquery_storage::BigQueryReadClient(connection);
	auto session =
      client.CreateReadSession(project_name, *read_session, 1);

	if (!session) {
		//Printer::Print("Error creating ReadSession: " + session.status().message());
		throw std::move(session).status();
	}

	idx_t duckdb_row_idx = 0;
	//Printer::Print("BigQueryScan: created ReadSession with offset: " + to_string(gstate.current_offset));
	auto read_rows = client.ReadRows(session->streams(0).name(), gstate.current_offset);

	// Get schema.
	auto schema = BigQueryUtils::GetArrowSchema(session->arrow_schema());
	//Printer::Print("Got schema");

  	for (auto const& read_rows_response : read_rows) {
	//Printer::Print("---- read rows response ----");
    if (read_rows_response.ok()) {
      auto record_batch =
          BigQueryResult::GetArrowRecordBatch(
			read_rows_response->arrow_record_batch(), schema);

	  std::int64_t batch_num_rows = record_batch->num_rows();
	  std::int64_t limit_rows = gstate.limit - gstate.global_row_count;
	  //Printer::Print("limit_rows: " + to_string(limit_rows));
	  //Printer::Print("batch_num_rows: " + to_string(batch_num_rows));
	  //Printer::Print("current duckdb_row_idx: " + to_string(duckdb_row_idx));

	  auto first_min =  gstate.has_limit ? std::min(batch_num_rows, limit_rows) : batch_num_rows;

	  idx_t max_rows = std::min(
			 first_min,
			 static_cast<int64_t>(STANDARD_VECTOR_SIZE));

		D_ASSERT(record_batch->num_columns() == output.ColumnCount());

	  //Printer::Print("max_rows: " + to_string(max_rows));

	  //Printer::Print("Column count : " + to_string(output.ColumnCount()));

	  for (idx_t c = 0; c < output.ColumnCount(); c++) {

		std::shared_ptr<arrow::Array> column = record_batch->column(c);

		for(idx_t r = 0; r < max_rows; r++){
			//Printer::Print("Setting value for column: " + to_string(c) + " bq arrow row: " + to_string(r));

			arrow::Result<std::shared_ptr<arrow::Scalar> > result =
        	  column->GetScalar(r);
			if (!result.ok()) {
				std::cout << "Unable to parse scalar\n";
				throw result.status();
			}

			std::shared_ptr<arrow::Scalar> scalar = result.ValueOrDie();

			auto v = BigQueryUtils::ValueFromArrowScalar(scalar);
			output.SetValue(c, gstate.current_offset + r, v);
		}
	  }
	gstate.current_offset = gstate.current_offset + max_rows;
	gstate.global_row_count = gstate.global_row_count + max_rows;
	duckdb_row_idx = duckdb_row_idx + max_rows;

    }
  }

	if (duckdb_row_idx == 0) {
		// done
		return;
	}

	output.SetCardinality(duckdb_row_idx);
	//Printer::Print("SetCardinality with r: " + to_string(duckdb_row_idx));

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
	projection_pushdown = true;
	filter_pushdown = true;
}

} // namespace duckdb
