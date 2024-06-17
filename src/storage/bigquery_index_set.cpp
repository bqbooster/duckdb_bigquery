#include "storage/bigquery_index_set.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/bigquery_index_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

BigQueryIndexSet::BigQueryIndexSet(BigQuerySchemaEntry &schema) : BigQueryInSchemaSet(schema) {
}

void BigQueryIndexSet::DropEntry(ClientContext &context, DropInfo &info) {
	// auto entry = GetEntry(context, info.name);
	// if (!entry) {
	// 	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
	// 		return;
	// 	}
	// 	throw CatalogException("Failed to DROP INDEX \"%s\": entry not found", info.name);
	// }
	// auto &bigquery_index = entry->Cast<BigQueryIndexEntry>();
	// string drop_query = "DROP INDEX ";
	// drop_query += BigQueryUtils::WriteIdentifier(info.name);
	// drop_query += " ON ";
	// drop_query += BigQueryUtils::WriteIdentifier(bigquery_index.table_name);
	// auto &transaction = BigQueryTransaction::Get(context, catalog);
	// transaction.Query(drop_query);

	EraseEntryInternal(info.name);
}

void BigQueryIndexSet::LoadEntries(ClientContext &context) {
// 	auto query = StringUtil::Replace(R"(
// SELECT DISTINCT TABLE_NAME, INDEX_NAME
// FROM INFORMATION_SCHEMA.STATISTICS
// WHERE TABLE_SCHEMA = 'bigqueryscanner';
// )", "${SCHEMA_NAME}", BigQueryUtils::WriteLiteral(schema.name));

// 	auto &transaction = BigQueryTransaction::Get(context, catalog);
// 	auto result = transaction.Query(query);
// 	while (result->Next()) {
// 		auto table_name = result->GetString(0);
// 		auto index_name = result->GetString(1);
// 		CreateIndexInfo info;
// 		info.schema = schema.name;
// 		info.table = table_name;
// 		info.index_name = index_name;
// 		auto index_entry = make_uniq<BigQueryIndexEntry>(catalog, schema, info, table_name);
// 		CreateEntry(std::move(index_entry));
// 	}
}

} // namespace duckdb
