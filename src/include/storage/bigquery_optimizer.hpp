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
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
