#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "bigquery_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

BigQueryCatalog::BigQueryCatalog(AttachedDatabase &db_p, const string &storage_project, const string &execution_project, AccessMode access_mode)
    : Catalog(db_p), path(storage_project), access_mode(access_mode), schemas(*this) {
	this->storage_project = storage_project;
	this->execution_project = execution_project;
	// try to connect
	//auto connection = BigQueryConnection::Open(path);
}

BigQueryCatalog::~BigQueryCatalog() = default;

void BigQueryCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> BigQueryCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
	// if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
	// 	DropInfo try_drop;
	// 	try_drop.type = CatalogType::SCHEMA_ENTRY;
	// 	try_drop.name = info.schema;
	// 	try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
	// 	try_drop.cascade = false;
	// 	schemas.DropEntry(transaction.GetContext(), try_drop);
	// }
	// return schemas.CreateSchema(transaction.GetContext(), info);
	return nullptr;
}

void BigQueryCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
	//return schemas.DropEntry(context, info);
}

void BigQueryCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	Printer::Print("BigQueryCatalog::ScanSchemas");
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<BigQuerySchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> BigQueryCatalog::GetSchema(CatalogTransaction transaction,
															const string &schema_name,
                                                         	OnEntryNotFound if_not_found,
                                                         	QueryErrorContext error_context) {
	Printer::Print("BigQueryCatalog::GetSchema " + schema_name);
	// if (schema_name == DEFAULT_SCHEMA) {
	// 	if (default_schema.empty()) {
	// 		throw InvalidInputException("Attempting to fetch the default schema - but no database was "
	// 		                            "provided in the connection string");
	// 	}
	// 	return GetSchema(transaction, default_schema, if_not_found, error_context);
	// }
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	// print entry
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	} else if(!entry) {
		Printer::Print("BigQueryCatalog::GetSchema not found creating for " + schema_name);
		auto schema_info = make_uniq<CreateSchemaInfo>();
		schema_info->catalog = this->storage_project;
		schema_info->schema = schema_name;
		auto schema_entry = make_uniq<BigQuerySchemaEntry>(*this, *schema_info);
 		schemas.CreateEntry(std::move(schema_entry));
		return GetSchema(transaction, schema_name, if_not_found, error_context);
	}
	Printer::Print("BigQueryCatalog::GetSchema found");
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());

	return nullptr;
}

bool BigQueryCatalog::InMemory() {
	return false;
}

string BigQueryCatalog::GetDBPath() {
	return path;
}

DatabaseSize BigQueryCatalog::GetDatabaseSize(ClientContext &context) {
// TODO implement this, we don't have access to the catalog with current BQ C++ API
// 	if (default_schema.empty()) {
// 		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
// 		                            "in the connection string");
// 	}
// 	auto &postgres_transaction = BigQueryTransaction::Get(context, *this);
// 	auto query = StringUtil::Replace(R"(
// SELECT SUM(data_length + index_length)
// FROM information_schema.tables
// WHERE table_schema = ${SCHEMA_NAME};
// )", "${SCHEMA_NAME}", BigQueryUtils::WriteLiteral(default_schema));
// 	auto result = postgres_transaction.Query(query);
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	// if (!result->Next()) {
	// 	throw InternalException("BigQueryCatalog::GetDatabaseSize - No row returned!?");
	// }
	// size.bytes = result->IsNull(0) ? 0 : result->GetInt64(0);
	size.bytes = 0;
	return size;
}

void BigQueryCatalog::ClearCache() {
	schemas.ClearEntries();
}

} // namespace duckdb
