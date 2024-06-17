#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "bigquery_result.hpp"

namespace duckdb {

BigQueryTransaction::BigQueryTransaction(BigQueryCatalog &bigquery_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(bigquery_catalog.access_mode) {
	//connection = BigQueryConnection::Open(bigquery_catalog.path);
}

BigQueryTransaction::~BigQueryTransaction() = default;

void BigQueryTransaction::Start() {
	transaction_state = BigQueryTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void BigQueryTransaction::Commit() {
	if (transaction_state == BigQueryTransactionState::TRANSACTION_STARTED) {
		transaction_state = BigQueryTransactionState::TRANSACTION_FINISHED;
		//connection.Execute("COMMIT");
	}
}
void BigQueryTransaction::Rollback() {
	if (transaction_state == BigQueryTransactionState::TRANSACTION_STARTED) {
		transaction_state = BigQueryTransactionState::TRANSACTION_FINISHED;
		//connection.Execute("ROLLBACK");
	}
}

// BigQueryConnection &BigQueryTransaction::GetConnection() {
// 	if (transaction_state == BigQueryTransactionState::TRANSACTION_NOT_YET_STARTED) {
// 		transaction_state = BigQueryTransactionState::TRANSACTION_STARTED;
// 		string query = "START TRANSACTION";
// 		if (access_mode == AccessMode::READ_ONLY) {
// 			query += " READ ONLY";
// 		}
// 		connection.Execute(query);
// 	}
// 	return connection;
// }

unique_ptr<BigQueryResult> BigQueryTransaction::Query(const string &query) {
	if (transaction_state == BigQueryTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = BigQueryTransactionState::TRANSACTION_STARTED;
		string transaction_start = "START TRANSACTION";
		if (access_mode == AccessMode::READ_ONLY) {
			transaction_start += " READ ONLY";
		}
		//connection.Query(transaction_start);
		//return connection.Query(query);
		return nullptr;
	}
	//return connection.Query(query);
	return nullptr;
}

BigQueryTransaction &BigQueryTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<BigQueryTransaction>();
}

} // namespace duckdb
