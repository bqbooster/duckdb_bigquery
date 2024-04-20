//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_index_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_index_entry.hpp"

namespace duckdb {
class BigQuerySchemaEntry;

class BigQueryIndexSet : public BigQueryInSchemaSet {
public:
	BigQueryIndexSet(BigQuerySchemaEntry &schema);

	void DropEntry(ClientContext &context, DropInfo &info) override;

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
