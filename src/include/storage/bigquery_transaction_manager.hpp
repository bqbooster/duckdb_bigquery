//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {

class BigQueryTransactionManager : public TransactionManager {
public:
	BigQueryTransactionManager(AttachedDatabase &db_p, BigQueryCatalog &bigquery_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	BigQueryCatalog &bigquery_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<BigQueryTransaction>> transactions;
};

} // namespace duckdb
