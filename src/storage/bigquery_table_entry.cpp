#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "bigquery_scanner.hpp"

namespace duckdb {

static bool TableIsInternal(const SchemaCatalogEntry &schema, const string &name) {
	if (schema.name != "bigquery") {
		return false;
	}
	// this is a list of all system tables
	// https://dev.bigquery.com/doc/refman/8.0/en/system-schema.html#system-schema-data-dictionary-tables
	unordered_set<string> system_tables {"audit_log_filter",
	                                     "audit_log_user",
	                                     "columns_priv",
	                                     "component",
	                                     "db",
	                                     "default_roles",
	                                     "engine_cost",
	                                     "event",
	                                     "events",
	                                     "firewall_group_allowlist",
	                                     "firewall_groups",
	                                     "firewall_memebership",
	                                     "firewall_users",
	                                     "firewall_whitelist",
	                                     "func",
	                                     "general_log",
	                                     "global_grants",
	                                     "gtid_executed",
	                                     "help_category",
	                                     "help_keyword",
	                                     "help_relation",
	                                     "help_topic",
	                                     "innodb_index_stats",
	                                     "innodb_table_stats",
	                                     "innodb_dynamic_metadata",
	                                     "ndb_binlog_index",
	                                     "password_history",
	                                     "parameters",
	                                     "plugin",
	                                     "procs_priv",
	                                     "proxies_priv",
	                                     "replication_asynchronous_connection_failover",
	                                     "replication_asynchronous_connection_failover_managed",
	                                     "replication_group_configuration_version",
	                                     "replication_group_member_actions",
	                                     "role_edges",
	                                     "routines",
	                                     "server_cost",
	                                     "servers",
	                                     "slave_master_info",
	                                     "slave_relay_log_info",
	                                     "slave_worker_info",
	                                     "slow_log",
	                                     "tables_priv",
	                                     "time_zone",
	                                     "time_zone_leap_second",
	                                     "time_zone_name",
	                                     "time_zone_transition",
	                                     "time_zone_transition_type",
	                                     "user"};
	return system_tables.find(name) != system_tables.end();
}

BigQueryTableEntry::BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = TableIsInternal(schema, name);
}

BigQueryTableEntry::BigQueryTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BigQueryTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = TableIsInternal(schema, name);
}

unique_ptr<BaseStatistics> BigQueryTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void BigQueryTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                                   ClientContext &context) {
}

TableFunction BigQueryTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<BigQueryBindData>(*this);
	for (auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
		result->names.push_back(col.GetName());
	}

	bind_data = std::move(result);

	auto function = BigQueryScanFunction();
	Value filter_pushdown;
	if (context.TryGetCurrentSetting("bigquery_experimental_filter_pushdown", filter_pushdown)) {
		function.filter_pushdown = BooleanValue::Get(filter_pushdown);
	}
	return function;
}

TableStorageInfo BigQueryTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<BigQueryTransaction>();
	auto &db = transaction.GetConnection();
	TableStorageInfo result;
	result.cardinality = 0;
	result.index_info = db.GetIndexInfo(name);
	return result;
}

} // namespace duckdb
