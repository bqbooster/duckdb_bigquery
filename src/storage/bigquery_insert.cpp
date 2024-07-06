#include "storage/bigquery_insert.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "bigquery_connection.hpp"
#include "bigquery_scanner.hpp"

namespace duckdb {

BigQueryInsert::BigQueryInsert(LogicalOperator &op, TableCatalogEntry &table,
                         physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

BigQueryInsert::BigQueryInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class BigQueryInsertGlobalState : public GlobalSinkState {
public:
	explicit BigQueryInsertGlobalState(ClientContext &context, BigQueryTableEntry &table,
	                                const vector<LogicalType> &varchar_types)
	    : table(table), insert_count(0) {
		varchar_chunk.Initialize(context, varchar_types);
	}

	BigQueryTableEntry &table;
	DataChunk varchar_chunk;
	idx_t insert_count;
	string base_insert_query;
	string insert_values;
};

vector<string> GetInsertColumns(const BigQueryInsert &insert, BigQueryTableEntry &entry) {
	vector<string> column_names;
	auto &columns = entry.GetColumns();
	idx_t column_count;
	if (!insert.column_index_map.empty()) {
		column_count = 0;
		vector<PhysicalIndex> column_indexes;
		column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto column_index = PhysicalIndex(c);
			auto mapped_index = insert.column_index_map[column_index];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// column not specified
				continue;
			}
			column_indexes[mapped_index] = column_index;
			column_count++;
		}
		for (idx_t c = 0; c < column_count; c++) {
			auto &col = columns.GetColumn(column_indexes[c]);
			column_names.push_back(col.GetName());
		}
	}
	return column_names;
}

string GetBaseInsertQuery(const BigQueryTableEntry &table, const vector<string> &column_names) {
	string query;
	query += "INSERT INTO ";
	query += BigQueryUtils::WriteIdentifier(table.schema.name);
	query += ".";
	query += BigQueryUtils::WriteIdentifier(table.name);
	query += " ";
	if (!column_names.empty()) {
		query += "(";
		for (idx_t c = 0; c < column_names.size(); c++) {
			if (c > 0) {
				query += ", ";
			}
			query += column_names[c];
		}
		query += ")";
	}
	query += " VALUES ";
	return query;
}

unique_ptr<GlobalSinkState> BigQueryInsert::GetGlobalSinkState(ClientContext &context) const {
	BigQueryTableEntry *insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<BigQueryTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<BigQueryTableEntry>();
	}
	auto insert_columns = GetInsertColumns(*this, *insert_table);
	vector<LogicalType> insert_types;
	idx_t insert_column_count =
	    insert_columns.empty() ? insert_table->GetColumns().LogicalColumnCount() : insert_columns.size();
	for (idx_t c = 0; c < insert_column_count; c++) {
		insert_types.push_back(LogicalType::VARCHAR);
	}
	auto result = make_uniq<BigQueryInsertGlobalState>(context, *insert_table, insert_types);
	result->base_insert_query = GetBaseInsertQuery(*insert_table, insert_columns);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static void BigQueryCastBlob(const Vector &input, Vector &result, idx_t count) {
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t r = 0; r < count; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto blob_data = const_data_ptr_cast(input_data[r].GetData());
		auto blob_size = input_data[r].GetSize();
		string result_blob = "0x";
		for (idx_t b = 0; b < blob_size; b++) {
			auto blob_entry = blob_data[b];
			auto byte_a = blob_entry >> 4;
			auto byte_b = blob_entry & 0x0F;
			result_blob += string(1, HEX_TABLE[byte_a]);
			result_blob += string(1, HEX_TABLE[byte_b]);
		}
		result_data[r] = StringVector::AddString(result, result_blob);
	}
}

SinkResultType BigQueryInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	static constexpr const idx_t INSERT_FLUSH_SIZE = 8000;

	auto &gstate = input.global_state.Cast<BigQueryInsertGlobalState>();
	auto &transaction = BigQueryTransaction::Get(context.client, gstate.table.catalog);
	//auto &con = transaction.GetConnection();
	// cast to varchar
	D_ASSERT(chunk.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	chunk.Flatten();
	gstate.varchar_chunk.Reset();
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BLOB:
			BigQueryCastBlob(chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		case LogicalTypeId::TIMESTAMP_TZ: {
			Vector timestamp_vector(LogicalType::TIMESTAMP);
			timestamp_vector.Reinterpret(chunk.data[c]);
			VectorOperations::Cast(context.client, timestamp_vector, gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
		default:
			VectorOperations::Cast(context.client, chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
	}
	gstate.varchar_chunk.SetCardinality(chunk.size());
	// for each column type check if we need to add quotes or not
	vector<bool> add_quotes;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		bool add_quotes_for_type;
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BLOB:
			add_quotes_for_type = false;
			break;
		default:
			add_quotes_for_type = true;
			break;
		}
		add_quotes.push_back(add_quotes_for_type);
	}

	// generate INSERT INTO statements
	for (idx_t r = 0; r < chunk.size(); r++) {
		if (!gstate.insert_values.empty()) {
			gstate.insert_values += ", ";
		}
		gstate.insert_values += "(";
		for (idx_t c = 0; c < gstate.varchar_chunk.ColumnCount(); c++) {
			if (c > 0) {
				gstate.insert_values += ", ";
			}
			if (FlatVector::IsNull(gstate.varchar_chunk.data[c], r)) {
				gstate.insert_values += "NULL";
			} else {
				auto data = FlatVector::GetData<string_t>(gstate.varchar_chunk.data[c]);
				if (add_quotes[c]) {
					gstate.insert_values += BigQueryUtils::WriteLiteral(data[r].GetString());
				} else {
					gstate.insert_values += data[r].GetString();
				}
			}
		}
		gstate.insert_values += ")";
		if (gstate.insert_values.size() >= INSERT_FLUSH_SIZE) {
			// perform the actual insert
			// con.Query(gstate.base_insert_query + gstate.insert_values);
			// reset the to-be-inserted values
			gstate.insert_values = string();
		}
	}
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType BigQueryInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BigQueryInsertGlobalState>();
	if (!gstate.insert_values.empty()) {
		// perform the final insert
		auto &transaction = BigQueryTransaction::Get(context, gstate.table.catalog);
		//auto &con = transaction.GetConnection();
		//con.Query(gstate.base_insert_query + gstate.insert_values);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType BigQueryInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<BigQueryInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string BigQueryInsert::GetName() const {
	return table ? "BIGQUERY_INSERT" : "BIGQUERY_CREATE_TABLE_AS";
}

string BigQueryInsert::ParamsToString() const {
	return table ? table->name : info->Base().table;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> AddCastToBigQueryTypes(ClientContext &context, unique_ptr<PhysicalOperator> plan) {
	// check if we need to cast anything
	// bool require_cast = false;
	// auto &child_types = plan->GetTypes();
	// for (auto &type : child_types) {
	// 	auto bigquery_type = BigQueryUtils::ToBigQueryType(type);
	// 	if (bigquery_type != type) {
	// 		require_cast = true;
	// 		break;
	// 	}
	// }
	// if (require_cast) {
	// 	vector<LogicalType> bigquery_types;
	// 	vector<unique_ptr<Expression>> select_list;
	// 	for (idx_t i = 0; i < child_types.size(); i++) {
	// 		auto &type = child_types[i];
	// 		unique_ptr<Expression> expr;
	// 		expr = make_uniq<BoundReferenceExpression>(type, i);

	// 		auto bigquery_type = BigQueryUtils::ToBigQueryType(type);
	// 		if (bigquery_type != type) {
	// 			// add a cast
	// 			expr = BoundCastExpression::AddCastToType(context, std::move(expr), bigquery_type);
	// 		}
	// 		bigquery_types.push_back(std::move(bigquery_type));
	// 		select_list.push_back(std::move(expr));
	// 	}
	// 	// we need to cast: add casts
	// 	auto proj =
	// 	    make_uniq<PhysicalProjection>(std::move(bigquery_types), std::move(select_list), plan->estimated_cardinality);
	// 	proj->children.push_back(std::move(plan));
	// 	plan = std::move(proj);
	// }

	//return plan;
	return nullptr;
}

unique_ptr<PhysicalOperator> BigQueryCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into BigQuery table");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into BigQuery table");
	}
	plan = AddCastToBigQueryTypes(context, std::move(plan));

	auto insert = make_uniq<BigQueryInsert>(op, op.table, op.column_index_map);
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

unique_ptr<PhysicalOperator> BigQueryCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                             unique_ptr<PhysicalOperator> plan) {
	plan = AddCastToBigQueryTypes(context, std::move(plan));

	auto insert = make_uniq<BigQueryInsert>(op, op.schema, std::move(op.info));
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

} // namespace duckdb
