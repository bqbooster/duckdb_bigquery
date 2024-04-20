//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class BigQueryStorageExtension : public StorageExtension {
public:
	BigQueryStorageExtension();
};

} // namespace duckdb
