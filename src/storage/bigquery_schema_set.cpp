#include "storage/bigquery_schema_set.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

// static bool BigQuerySchemaIsInternal(const string &name) {
// 	if (name == "information_schema") {
// 		return true;
// 	}
// 	return false;
// }

BigQuerySchemaSet::BigQuerySchemaSet(Catalog &catalog) : BigQueryCatalogSet(catalog) {
}

void BigQuerySchemaSet::LoadEntries(ClientContext &context) {
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
// 	auto query = R"(
// SELECT schema_name
// FROM information_schema.schemata;
// )";

	//auto &transaction = BigQueryTransaction::Get(context, catalog);
	//auto result = transaction.Query(query);
	//while (result->Next()) {
	// CreateSchemaInfo info;
	// info.schema = result->GetString(0);
	// info.internal = BigQuerySchemaIsInternal(info.schema);
	// auto schema = make_uniq<BigQuerySchemaEntry>(catalog, info);
	// CreateEntry(std::move(schema));
	// }
}

optional_ptr<CatalogEntry> BigQuerySchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
	// auto &transaction = BigQueryTransaction::Get(context, catalog);

	// string create_sql = "CREATE SCHEMA " + BigQueryUtils::WriteIdentifier(info.schema);
	// transaction.Query(create_sql);
	// auto schema_entry = make_uniq<BigQuerySchemaEntry>(catalog, info);
	// return CreateEntry(std::move(schema_entry));
	return nullptr;
}

} // namespace duckdb
