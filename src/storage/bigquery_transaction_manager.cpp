#include "storage/bigquery_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

BigQueryTransactionManager::BigQueryTransactionManager(AttachedDatabase &db_p, BigQueryCatalog &bigquery_catalog)
    : TransactionManager(db_p), bigquery_catalog(bigquery_catalog) {
}

Transaction &BigQueryTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<BigQueryTransaction>(bigquery_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData BigQueryTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &bigquery_transaction = transaction.Cast<BigQueryTransaction>();
	bigquery_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void BigQueryTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &bigquery_transaction = transaction.Cast<BigQueryTransaction>();
	bigquery_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void BigQueryTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = BigQueryTransaction::Get(context, db.GetCatalog());
	//auto &db = transaction.GetConnection();
	//db.Execute("CHECKPOINT");
}

} // namespace duckdb
