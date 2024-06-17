#include "storage/bigquery_execute_query.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "bigquery_connection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

BigQueryExecuteQuery::BigQueryExecuteQuery(LogicalOperator &op, string op_name_p, TableCatalogEntry &table, string query_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), op_name(std::move(op_name_p)), table(table),
      query(std::move(query_p)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class BigQueryExecuteQueryGlobalState : public GlobalSinkState {
public:
	explicit BigQueryExecuteQueryGlobalState() : affected_rows(0) {
	}

	idx_t affected_rows;
};

unique_ptr<GlobalSinkState> BigQueryExecuteQuery::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BigQueryExecuteQueryGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType BigQueryExecuteQuery::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return SinkResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType BigQueryExecuteQuery::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	// auto &gstate = input.global_state.Cast<BigQueryExecuteQueryGlobalState>();
	// auto &transaction = BigQueryTransaction::Get(context, table.catalog);
	// auto &connection = transaction.GetConnection();
	// auto result = connection.Query(query);
	// gstate.affected_rows = result->AffectedRows();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType BigQueryExecuteQuery::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<BigQueryExecuteQueryGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string BigQueryExecuteQuery::GetName() const {
	return op_name;
}

string BigQueryExecuteQuery::ParamsToString() const {
	return table.name;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
string ExtractFilters(PhysicalOperator &child, const string &statement) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	if (child.type == PhysicalOperatorType::FILTER) {
		auto &filter = child.Cast<PhysicalFilter>();
		auto result = ExtractFilters(*child.children[0], statement);
		auto filter_str = filter.expression->ToString();
		if (result.empty()) {
			return filter_str;
		} else {
			return result + " AND " + filter_str;
		}
	} else if (child.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = child.Cast<PhysicalTableScan>();
		if (!table_scan.table_filters) {
			return string();
		}
		throw NotImplementedException("Pushed down table filters not supported currently");
	} else {
		throw NotImplementedException("Unsupported operator type %s in %s statement - only simple deletes "
		                              "(e.g. %s "
		                              "FROM tbl WHERE x=y) are supported in the BigQuery connector",
		                              PhysicalOperatorToString(child.type), statement, statement);
	}
}

string ConstructDeleteStatement(LogicalDelete &op, PhysicalOperator &child) {
	string result = "DELETE FROM ";
	result += BigQueryUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += BigQueryUtils::WriteIdentifier(op.table.name);
	auto filters = ExtractFilters(child, "DELETE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

unique_ptr<PhysicalOperator> BigQueryCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a BigQuery table");
	}

	auto result = make_uniq<BigQueryExecuteQuery>(op, "DELETE", op.table, ConstructDeleteStatement(op, *plan));
	result->children.push_back(std::move(plan));
	return std::move(result);
}

string ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child) {
	// FIXME - all of this is pretty gnarly, we should provide a hook earlier on
	// in the planning process to convert this into a SQL statement
	string result = "UPDATE";
	result += BigQueryUtils::WriteIdentifier(op.table.schema.name);
	result += ".";
	result += BigQueryUtils::WriteIdentifier(op.table.name);
	result += " SET ";
	if (child.type != PhysicalOperatorType::PROJECTION) {
		throw NotImplementedException("BigQuery Update not supported - Expected the "
		                              "child of an update to be a projection");
	}
	auto &proj = child.Cast<PhysicalProjection>();
	for (idx_t c = 0; c < op.columns.size(); c++) {
		if (c > 0) {
			result += ", ";
		}
		auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
		result += BigQueryUtils::WriteIdentifier(col.GetName());
		result += " = ";
		if (op.expressions[c]->type == ExpressionType::VALUE_DEFAULT) {
			result += "DEFAULT";
			continue;
		}
		if (op.expressions[c]->type != ExpressionType::BOUND_REF) {
			throw NotImplementedException("BigQuery Update not supported - Expected a bound reference expression");
		}
		auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
		result += proj.select_list[ref.index]->ToString();
	}
	result += " ";
	auto filters = ExtractFilters(*child.children[0], "UPDATE");
	if (!filters.empty()) {
		result += " WHERE " + filters;
	}
	return result;
}

unique_ptr<PhysicalOperator> BigQueryCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                      unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a BigQuery table");
	}
	auto result = make_uniq<BigQueryExecuteQuery>(op, "UPDATE", op.table, ConstructUpdateStatement(op, *plan));
	result->children.push_back(std::move(plan));
	return std::move(result);
}

} // namespace duckdb
