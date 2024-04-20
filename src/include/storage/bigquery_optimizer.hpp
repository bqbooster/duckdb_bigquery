//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"

namespace duckdb {
class BigQueryOptimizer {
public:
	static void Optimize(ClientContext &context, OptimizerExtensionInfo *info, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
