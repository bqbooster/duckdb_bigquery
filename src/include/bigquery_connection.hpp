//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "bigquery_utils.hpp"
#include "bigquery_result.hpp"

namespace duckdb {
// class BigQueryBinaryWriter;
// class BigQueryTextWriter;
// struct BigQueryBinaryReader;
// class BigQuerySchemaEntry;
// class BigQueryTableEntry;
// class BigQueryStatement;
// class BigQueryResult;
// struct IndexInfo;

// struct OwnedBigQueryConnection {
// 	explicit OwnedBigQueryConnection(BIGQUERY *conn = nullptr) : connection(conn) {
// 	}
// 	~OwnedBigQueryConnection() {
// 		if (!connection) {
// 			return;
// 		}
// 		bigquery_close(connection);
// 		connection = nullptr;
// 	}

// 	BIGQUERY *connection;
// };

// class BigQueryConnection {
// public:
// 	explicit BigQueryConnection(shared_ptr<OwnedBigQueryConnection> connection = nullptr);
// 	~BigQueryConnection();
// 	// disable copy constructors
// 	BigQueryConnection(const BigQueryConnection &other) = delete;
// 	BigQueryConnection &operator=(const BigQueryConnection &) = delete;
// 	//! enable move constructors
// 	BigQueryConnection(BigQueryConnection &&other) noexcept;
// 	BigQueryConnection &operator=(BigQueryConnection &&) noexcept;

// public:
// 	static BigQueryConnection Open(const string &connection_string);
// 	void Execute(const string &query);
// 	unique_ptr<BigQueryResult> Query(const string &query, optional_ptr<ClientContext> context = nullptr);

// 	vector<IndexInfo> GetIndexInfo(const string &table_name);

// 	bool IsOpen();
// 	void Close();

// 	shared_ptr<OwnedBigQueryConnection> GetConnection() {
// 		return connection;
// 	}
// 	string GetDSN() {
// 		return dsn;
// 	}

// 	BIGQUERY *GetConn() {
// 		if (!connection || !connection->connection) {
// 			throw InternalException("BigQueryConnection::GetConn - no connection available");
// 		}
// 		return connection->connection;
// 	}

// 	static void DebugSetPrintQueries(bool print);
// 	static bool DebugPrintQueries();

// private:
// 	BIGQUERY_RES *BigQueryExecute(const string &query);

// 	string dsn;
// 	shared_ptr<OwnedBigQueryConnection> connection;
// };

} // namespace duckdb
