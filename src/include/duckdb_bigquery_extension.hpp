#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

using namespace duckdb;

class DuckdbBigqueryExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override {
		return "duckdb_bigquery";
	}
};

extern "C" {
DUCKDB_EXTENSION_API void duckdb_bigquery_init(duckdb::DatabaseInstance &db);
DUCKDB_EXTENSION_API const char *duckdb_bigquery_version();
DUCKDB_EXTENSION_API void duckdb_bigquery_storage_init(DBConfig &config);
}
