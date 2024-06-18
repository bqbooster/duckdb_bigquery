#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "bigquery_query.hpp"
#include "bigquery_scanner.hpp"
#include "bigquery_result.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_table_set.hpp"
#include "bigquery_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BigQuery Query
//===--------------------------------------------------------------------===//

struct BigQueryQueryLocalState : public LocalTableFunctionState {};

struct BigQueryQueryGlobalState : public GlobalTableFunctionState {
	explicit BigQueryQueryGlobalState(unique_ptr<BigQueryResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<BigQueryResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct BigQueryQueryBindData : public FunctionData {
	BigQueryQueryBindData(Catalog &catalog, unique_ptr<BigQueryResult> result_p, string query_p)
	    : catalog(catalog), result(std::move(result_p)), query(std::move(query_p)) {
	}

	Catalog &catalog;
	unique_ptr<BigQueryResult> result;
	string query;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("BigQueryQueryBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

static void BigQueryQuery(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Printer::Print("BigQueryQuery");
	auto &gstate = data.global_state->Cast<BigQueryQueryGlobalState>();
	idx_t r = 0;
	if (r == 0) {
		// done
		return;
	}
	output.SetCardinality(r);
}

static unique_ptr<FunctionData> BigQueryQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("First Parameter to bigquery_query cannot be NULL");
	}
	else if (input.inputs[1].IsNull()) {
		throw BinderException("Second Parameter to bigquery_query cannot be NULL");
	}

	//auto &bind_data = input.bind_data->Cast<BigQueryBindData>();
	//auto &catalog = bind_data.table.catalog.Cast<BigQueryCatalog>();

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
	// auto &transaction = BigQueryTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();
	// auto result = transaction.GetConnection().Query(sql, &context);
	// for (auto &field : result->Fields()) {
	// 	names.push_back(field.name);
	// 	return_types.push_back(field.type);
	// }

	Printer::Print("db_name" + db_name + "\n");
	Printer::Print("sql" + sql + "\n");

	//return make_uniq<BigQueryQueryBindData>(catalog, std::move(result), std::move(sql));
	return make_uniq<BigQueryQueryBindData>(catalog, nullptr, std::move(sql));
}

static unique_ptr<GlobalTableFunctionState> BigQueryQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BigQueryQueryBindData>();
	Printer::Print("BigQueryQueryInitGlobalState");
	// unique_ptr<BigQueryResult> bigquery_result;
	// if (bind_data.result) {
	// 	bigquery_result = std::move(bind_data.result);
	// } else {
	// 	auto &transaction = BigQueryTransaction::Get(context, bind_data.catalog);
	// 	bigquery_result = transaction.GetConnection().Query(bind_data.query, &context);
	// }
	// auto column_count = bigquery_result->ColumnCount();


	// // generate the varchar chunk
	// vector<LogicalType> varchar_types;
	// for (idx_t c = 0; c < column_count; c++) {
	// 	varchar_types.push_back(LogicalType::VARCHAR);
	// }
	// result->varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	// auto result = make_uniq<BigQueryQueryGlobalState>(std::move(bigquery_result));
	//return std::move(result);
	return nullptr;
}

static unique_ptr<LocalTableFunctionState> BigQueryInitLocalState(
	ExecutionContext &context,
	TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state) {
		Printer::Print("BigQueryInitLocalState");
	return make_uniq<BigQueryQueryLocalState>();
}

static void BigQueryQuerySerialize(Serializer &serializer,
								  const optional_ptr<FunctionData> bind_data_p,
                                  const TableFunction &function) {
	throw NotImplementedException("BigQueryQuerySerialize");
}

static unique_ptr<FunctionData> BigQueryQueryDeserialize(Deserializer &deserializer,
														TableFunction &function) {
	throw NotImplementedException("BigQueryQueryDeserialize");
}

BigQueryQueryFunction::BigQueryQueryFunction()
    : TableFunction("bigquery_query", //table function name
					{LogicalType::VARCHAR, LogicalType::VARCHAR}, // arguments: database name, query
					BigQueryQuery,
					BigQueryQueryBind,
                    BigQueryQueryInitGlobalState,
					BigQueryInitLocalState) {
	serialize = BigQueryQuerySerialize;
	deserialize = BigQueryQueryDeserialize;
}

} // namespace duckdb
