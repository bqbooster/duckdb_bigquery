#include "duckdb.hpp"

#include "bigquery_storage.hpp"
#include "storage/bigquery_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/bigquery_transaction_manager.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this
	// use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

string SecretValueOrEmpty(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	return input_val.ToString();
}

static unique_ptr<Catalog> BigQueryAttach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db,
                                       const string &name, AttachInfo &info, AccessMode access_mode) {
	Printer::Print("BigQueryAttach");
	string database = info.path;
	string execution_project = database;
	// check if we have a secret provided
	string secret_name;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "execution_project") {
			execution_project = entry.second.ToString();
		} else if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for BigQuery attach: %s", entry.first);
		}
	}

	// if no secret is specified we default to the unnamed bigquery secret, if it
	// exists
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed bigquery secret if none is
		// provided
		secret_name = "__default_bigquery";
	}

	auto secret_entry = GetSecret(context, secret_name);
	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		execution_project = SecretValueOrEmpty(kv_secret, "execution_project");
		database = SecretValueOrEmpty(kv_secret, "database");
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}

	Printer::Print("execution_project: " + execution_project + "\n");
	Printer::Print("database: " + database + "\n");

	return make_uniq<BigQueryCatalog>(db, database, execution_project, access_mode);
}

static unique_ptr<TransactionManager> BigQueryCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &bigquery_catalog = catalog.Cast<BigQueryCatalog>();
	return make_uniq<BigQueryTransactionManager>(db, bigquery_catalog);
}

BigQueryStorageExtension::BigQueryStorageExtension() {
	attach = BigQueryAttach;
	create_transaction_manager = BigQueryCreateTransactionManager;
}

} // namespace duckdb
