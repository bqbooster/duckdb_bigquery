#include "duckdb.hpp"

#include "duckdb/main/database_manager.hpp"
//#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "bigquery_scanner.hpp"
//#include "bigquery_connection.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {

struct BigQueryExecuteBindData : public TableFunctionData {
	explicit BigQueryExecuteBindData(BigQueryCatalog &bigquery_catalog, string query_p)
	    : bigquery_catalog(bigquery_catalog), query(std::move(query_p)) {
	}
	bool finished = false;
	BigQueryCatalog &bigquery_catalog;
	string query;
};

static duckdb::unique_ptr<FunctionData> BigQueryExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {

	 return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	 names.emplace_back("Success");

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in bigquery_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "bigquery") {
		throw BinderException("Attached database \"%s\" does not refer to a BigQuery database", db_name);
	}
	auto &bigquery_catalog = catalog.Cast<BigQueryCatalog>();
	return make_uniq<BigQueryExecuteBindData>(bigquery_catalog, input.inputs[1].GetValue<string>());
}

static void BigQueryExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<BigQueryExecuteBindData>();
	if (data.finished) {
		return;
	}
	auto &transaction = Transaction::Get(context, data.bigquery_catalog).Cast<BigQueryTransaction>();
	if (transaction.GetAccessMode() == AccessMode::READ_ONLY) {
		throw PermissionException("bigquery_execute cannot be run in a read-only connection");
	}
	//transaction.GetConnection().Execute(data.query);
	data.finished = true;
}

BigQueryExecuteFunction::BigQueryExecuteFunction()
    : TableFunction("bigquery_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, BigQueryExecuteFunc, BigQueryExecuteBind) {
}

} // namespace duckdb
