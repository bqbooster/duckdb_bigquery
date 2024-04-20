#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "bigquery_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

BigQueryCatalog::BigQueryCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode), schemas(*this) {
	default_schema = BigQueryUtils::ParseConnectionParameters(path).db;
	// try to connect
	auto connection = BigQueryConnection::Open(path);
}

BigQueryCatalog::~BigQueryCatalog() = default;

void BigQueryCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> BigQueryCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropEntry(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void BigQueryCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void BigQueryCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<BigQuerySchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> BigQueryCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                         OnEntryNotFound if_not_found,
                                                         QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                            "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found, error_context);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool BigQueryCatalog::InMemory() {
	return false;
}

string BigQueryCatalog::GetDBPath() {
	return path;
}

DatabaseSize BigQueryCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	auto &postgres_transaction = BigQueryTransaction::Get(context, *this);
	auto query = StringUtil::Replace(R"(
SELECT SUM(data_length + index_length)
FROM information_schema.tables
WHERE table_schema = ${SCHEMA_NAME};
)",
	                                 "${SCHEMA_NAME}", BigQueryUtils::WriteLiteral(default_schema));
	auto result = postgres_transaction.Query(query);
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	if (!result->Next()) {
		throw InternalException("BigQueryCatalog::GetDatabaseSize - No row returned!?");
	}
	size.bytes = result->IsNull(0) ? 0 : result->GetInt64(0);
	return size;
}

void BigQueryCatalog::ClearCache() {
	schemas.ClearEntries();
}

} // namespace duckdb
