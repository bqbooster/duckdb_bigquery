#include "storage/bigquery_catalog_set.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/bigquery_schema_entry.hpp"

namespace duckdb {

BigQueryCatalogSet::BigQueryCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> BigQueryCatalogSet::GetEntry(ClientContext &context, const string &name) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	Printer::Print("BigQueryCatalogSet::GetEntry find " + name);

	for (auto &entry : entries) {
		Printer::Print("BigQueryCatalogSet::GetEntry HAS " + entry.first);
	}

	auto entry = entries.find(name);
	if (entry == entries.end()) {
		Printer::Print("BigQueryCatalogSet::GetEntry not found");
		return nullptr;
	} else {
		Printer::Print("BigQueryCatalogSet::GetEntry found!!!");
	}
	return entry->second.get();
}

void BigQueryCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	// TODO implement this, we don't have access to the catalog with current BQ C++ API
	// string drop_query = "DROP ";
	// drop_query += CatalogTypeToString(info.type) + " ";
	// if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
	// 	drop_query += " IF EXISTS ";
	// }
	// drop_query += BigQueryUtils::WriteIdentifier(info.name);
	// if (info.type != CatalogType::SCHEMA_ENTRY) {
	// 	if (info.cascade) {
	// 		drop_query += " CASCADE";
	// 	}
	// }
	// auto &transaction = BigQueryTransaction::Get(context, catalog);
	// transaction.Query(drop_query);

	// erase the entry from the catalog set
	EraseEntryInternal(info.name);
}

void BigQueryCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void BigQueryCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> BigQueryCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	Printer::Print("BigQueryCatalogSet::CreateEntry");
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("BigQueryCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	// print all values in entries
	for (auto &entry : entries) {
		Printer::Print("BigQueryCatalogSet::CreateEntry HAS " + entry.first);
	}
	return result;
}

void BigQueryCatalogSet::ClearEntries() {
	entries.clear();
	is_loaded = false;
}

BigQueryInSchemaSet::BigQueryInSchemaSet(BigQuerySchemaEntry &schema) : BigQueryCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> BigQueryInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return BigQueryCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
