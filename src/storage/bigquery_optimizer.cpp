#include "storage/bigquery_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "bigquery_scanner.hpp"

namespace duckdb {

static bool IsBigQueryScan(const string &function_name) {
	return function_name == "bigquery_scan";
}

void OptimizeBigQueryScan(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}
		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = child.get().Cast<LogicalGet>();
		if (!IsBigQueryScan(get.function.name)) {
			return;
		}
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			return;
		}
		switch (limit.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset offset
			return;
		}
		auto &bind_data = get.bind_data->Cast<BigQueryScanBindData>();
		// if (limit.limit_val.Type() != LimitNodeType::UNSET) {
		// 	bind_data.limit += " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		// }
		// if (limit.offset_val.Type() != LimitNodeType::UNSET) {
		// 	bind_data.limit += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		// }
		// remove the limit
		op = std::move(op->children[0]);
		return;
	}
	// recurse into children
	for (auto &child : op->children) {
		OptimizeBigQueryScan(child);
	}
}

void BigQueryOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan){
	// TODO - Implement this function as it was written for mysql
	// OptimizeBigQueryScan(plan);
}

} // namespace duckdb
