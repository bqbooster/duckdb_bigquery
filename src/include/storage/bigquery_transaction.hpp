//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/bigquery_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "bigquery_connection.hpp"

namespace duckdb {
class BigQueryCatalog;
class BigQuerySchemaEntry;
class BigQueryTableEntry;

enum class BigQueryTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class BigQueryTransaction : public Transaction {
public:
	BigQueryTransaction(BigQueryCatalog &bigquery_catalog, TransactionManager &manager, ClientContext &context);
	~BigQueryTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	// BigQueryConnection &GetConnection();
	unique_ptr<BigQueryResult> Query(const string &query);
	static BigQueryTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	//BigQueryConnection connection;
	BigQueryTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
