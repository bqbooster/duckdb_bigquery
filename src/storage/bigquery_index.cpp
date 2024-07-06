#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_index.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

BigQueryCreateIndex::BigQueryCreateIndex(unique_ptr<CreateIndexInfo> info, TableCatalogEntry &table)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), info(std::move(info)), table(table) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType BigQueryCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &catalog = table.catalog;
	if (info->catalog == INVALID_CATALOG && info->schema == catalog.GetName()) {
		info->schema = DEFAULT_SCHEMA;
	}
	auto &schema = catalog.GetSchema(context.client, info->schema);
	schema.CreateIndex(context.client, *info, table);

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class LogicalBigQueryCreateIndex : public LogicalExtensionOperator {
public:
	LogicalBigQueryCreateIndex(unique_ptr<CreateIndexInfo> info_p, TableCatalogEntry &table)
	    : info(std::move(info_p)), table(table) {
	}

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

	unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override {
		return make_uniq<BigQueryCreateIndex>(std::move(info), table);
	}

	void Serialize(Serializer &serializer) const override {
		throw InternalException("Cannot serialize BigQuery Create index");
	}

	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

unique_ptr<LogicalOperator> BigQueryCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	return make_uniq<LogicalBigQueryCreateIndex>(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info)),
	                                          table);
}

} // namespace duckdb
