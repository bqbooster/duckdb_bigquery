//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "bigquery_utils.hpp"
#include "bigquery_connection.hpp"

namespace duckdb {
class BigQueryTableEntry;

class BigQueryQueryFunction : public TableFunction {
public:
	BigQueryQueryFunction();
};

} // namespace duckdb
