#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "bigquery_scanner.hpp"
#include "bigquery_result.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_table_set.hpp"
#include "bigquery_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct BigQueryGlobalState;

struct BigQueryLocalState : public LocalTableFunctionState {};

struct BigQueryGlobalState : public GlobalTableFunctionState {
	explicit BigQueryGlobalState(unique_ptr<BigQueryResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<BigQueryResult> result;
	DataChunk varchar_chunk;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> BigQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("BigQueryBind");
}

static unique_ptr<GlobalTableFunctionState> BigQueryInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BigQueryBindData>();
	// generate the SELECT statement
	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += BigQueryUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += BigQueryUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += BigQueryUtils::WriteIdentifier(bind_data.table.name);
	string filter_string = BigQueryFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}
	if (!bind_data.limit.empty()) {
		select += bind_data.limit;
	}
	// run the query
	auto &transaction = BigQueryTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();
	auto query_result = con.Query(select);
	auto result = make_uniq<BigQueryGlobalState>(std::move(query_result));

	// generate the varchar chunk
	vector<LogicalType> varchar_types;
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		varchar_types.push_back(LogicalType::VARCHAR);
	}
	result->varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> BigQueryInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<BigQueryLocalState>();
}

void CastBoolFromBigQuery(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto str_data = input_data[r].GetData();
		auto str_size = input_data[r].GetSize();
		if (str_size != 1) {
			throw BinderException("Failed to cast BigQuery boolean - expected 1 byte "
			                      "element but got element of size %s",
			                      str_size);
		}
		auto bool_char = *str_data;
		result_data[r] = bool_char == '\1' || bool_char == '1';
	}
}

static void BigQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<BigQueryGlobalState>();
	idx_t r;
	gstate.varchar_chunk.Reset();
	for (r = 0; r < STANDARD_VECTOR_SIZE; r++) {
		if (!gstate.result->Next()) {
			// exhausted result
			break;
		}
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			auto &vec = gstate.varchar_chunk.data[c];
			if (gstate.result->IsNull(c)) {
				FlatVector::SetNull(vec, r, true);
			} else {
				auto string_data = FlatVector::GetData<string_t>(vec);
				string_data[r] = StringVector::AddStringOrBlob(vec, gstate.result->GetStringT(c));
			}
		}
	}
	if (r == 0) {
		// done
		return;
	}
	D_ASSERT(output.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		switch (output.data[c].GetType().id()) {
		case LogicalTypeId::BLOB:
			// blobs are sent over the wire as-is
			output.data[c].Reinterpret(gstate.varchar_chunk.data[c]);
			break;
		case LogicalTypeId::BOOLEAN:
			// booleans can be sent either as numbers ('0' or '1') or as bits ('\0' or
			// '\1')
			CastBoolFromBigQuery(context, gstate.varchar_chunk.data[c], output.data[c], r);
			break;
		default: {
			string error;
			VectorOperations::TryCast(context, gstate.varchar_chunk.data[c], output.data[c], r, &error);
			break;
		}
		}
	}
	output.SetCardinality(r);
}

static string BigQueryScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<BigQueryBindData>();
	return bind_data.table.name;
}

static void BigQueryScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("BigQueryScanSerialize");
}

static unique_ptr<FunctionData> BigQueryScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("BigQueryScanDeserialize");
}

BigQueryScanFunction::BigQueryScanFunction()
    : TableFunction("bigquery_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, BigQueryScan,
                    BigQueryBind, BigQueryInitGlobalState, BigQueryInitLocalState) {
	to_string = BigQueryScanToString;
	serialize = BigQueryScanSerialize;
	deserialize = BigQueryScanDeserialize;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// BigQuery Query
//===--------------------------------------------------------------------===//
struct BigQueryQueryBindData : public FunctionData {
	BigQueryQueryBindData(Catalog &catalog, unique_ptr<BigQueryResult> result_p, string query_p)
	    : catalog(catalog), result(std::move(result_p)), query(std::move(query_p)) {
	}

	Catalog &catalog;
	unique_ptr<BigQueryResult> result;
	string query;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("BigQueryBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};

static unique_ptr<FunctionData> BigQueryQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to bigquery_query cannot be NULL");
	}

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
	auto &transaction = BigQueryTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();
	auto result = transaction.GetConnection().Query(sql, &context);
	for (auto &field : result->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.type);
	}
	return make_uniq<BigQueryQueryBindData>(catalog, std::move(result), std::move(sql));
}

static unique_ptr<GlobalTableFunctionState> BigQueryQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BigQueryQueryBindData>();
	unique_ptr<BigQueryResult> bigquery_result;
	if (bind_data.result) {
		bigquery_result = std::move(bind_data.result);
	} else {
		auto &transaction = BigQueryTransaction::Get(context, bind_data.catalog);
		bigquery_result = transaction.GetConnection().Query(bind_data.query, &context);
	}
	auto column_count = bigquery_result->ColumnCount();

	auto result = make_uniq<BigQueryGlobalState>(std::move(bigquery_result));

	// generate the varchar chunk
	vector<LogicalType> varchar_types;
	for (idx_t c = 0; c < column_count; c++) {
		varchar_types.push_back(LogicalType::VARCHAR);
	}
	result->varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	return std::move(result);
}

BigQueryQueryFunction::BigQueryQueryFunction()
    : TableFunction("bigquery_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, BigQueryScan, BigQueryQueryBind,
                    BigQueryQueryInitGlobalState, BigQueryInitLocalState) {
	serialize = BigQueryScanSerialize;
	deserialize = BigQueryScanDeserialize;
}

} // namespace duckdb
