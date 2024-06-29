#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_catalog.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
//#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

BigQuerySchemaEntry::BigQuerySchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) //, indexes(*this)
	{}

BigQueryTransaction &GetBigQueryTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<BigQueryTransaction>();
}

void BigQuerySchemaEntry::TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name) {
	DropInfo info;
	info.type = catalog_type;
	info.name = name;
	info.cascade = false;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	DropEntry(context, info);
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}
	return tables.CreateTable(transaction.GetContext(), info);
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("BigQuery databases do not support creating functions");
}

// void BigQueryUnqualifyColumnRef(ParsedExpression &expr) {
// 	if (expr.type == ExpressionType::COLUMN_REF) {
// 		auto &colref = expr.Cast<ColumnRefExpression>();
// 		auto name = std::move(colref.column_names.back());
// 		colref.column_names = {std::move(name)};
// 		return;
// 	}
// 	ParsedExpressionIterator::EnumerateChildren(expr, BigQueryUnqualifyColumnRef);
// }

// string GetBigQueryCreateIndex(CreateIndexInfo &info, TableCatalogEntry &tbl) {
// 	string sql;
// 	sql = "CREATE";
// 	if (info.constraint_type == IndexConstraintType::UNIQUE) {
// 		sql += " UNIQUE";
// 	}
// 	sql += " INDEX ";
// 	sql += BigQueryUtils::WriteIdentifier(info.index_name);
// 	sql += " ON ";
// 	sql += BigQueryUtils::WriteIdentifier(tbl.name);
// 	sql += "(";
// 	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
// 		if (i > 0) {
// 			sql += ", ";
// 		}
// 		BigQueryUnqualifyColumnRef(*info.parsed_expressions[i]);
// 		if (info.parsed_expressions[i]->type == ExpressionType::COLUMN_REF) {
// 			// index on column
// 			sql += info.parsed_expressions[i]->ToString();
// 		} else {
// 			// index on expression
// 			// expressions need to be wrapped in brackets
// 			sql += "(" + info.parsed_expressions[i]->ToString() + ")";
// 		}
// 	}
// 	sql += ")";
// 	return sql;
// }

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	//auto &bigquery_transaction = BigQueryTransaction::Get(context, table.catalog);
	//bigquery_transaction.Query(GetBigQueryCreateIndex(info, table));
	return nullptr;
}

string GetBigQueryCreateView(CreateViewInfo &info) {
	string sql;
	sql = "CREATE VIEW ";
	sql += BigQueryUtils::WriteIdentifier(info.view_name);
	sql += " ";
	if (!info.aliases.empty()) {
		sql += "(";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto &alias = info.aliases[i];
			sql += BigQueryUtils::WriteIdentifier(alias);
		}
		sql += ") ";
	}
	sql += "AS ";
	sql += info.query->ToString();
	return sql;
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in BigQuery that originated from an "
		                      "empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (current_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return current_entry;
			}
			// CREATE OR REPLACE - drop any existing entries first (if any)
			TryDropEntry(transaction.GetContext(), CatalogType::VIEW_ENTRY, info.view_name);
		}
	}
	auto &bigquery_transaction = GetBigQueryTransaction(transaction);
	bigquery_transaction.Query(GetBigQueryCreateView(info));
	return tables.RefreshTable(transaction.GetContext(), info.view_name);
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("BigQuery databases do not support creating types");
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("BigQuery databases do not support creating sequences");
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	throw BinderException("BigQuery databases do not support creating table functions");
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	throw BinderException("BigQuery databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	throw BinderException("BigQuery databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	throw BinderException("BigQuery databases do not support creating collations");
}

void BigQuerySchemaEntry:: Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &alter = info.Cast<AlterTableInfo>();
	tables.AlterTable(transaction.GetContext(), alter);
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	//case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void BigQuerySchemaEntry::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	Printer::Print("BigQuerySchemaEntry::Scan");
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
}
void BigQuerySchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void BigQuerySchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	GetCatalogSet(info.type).DropEntry(context, info);
}

optional_ptr<CatalogEntry> BigQuerySchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                      const string &name) {
	if (!CatalogTypeIsSupported(type)) {
		return nullptr;
	}
	auto entry = GetCatalogSet(type).GetEntry(transaction.GetContext(), name);
	if(!entry && type != CatalogType::INDEX_ENTRY) {
		Printer::Print("BigQuerySchemaEntry::GetEntry not found creating");
		// cast catalog to BigQueryCatalog
		auto bq_catalog = dynamic_cast<BigQueryCatalog*>(&this->catalog);

		auto info = this->GetInfo();
		auto table_entry = BigQueryUtils::BigQueryCreateBigQueryTableEntry(
			catalog,
			this,
			bq_catalog->execution_project,
			bq_catalog->storage_project,
			this->name,
			name);
		Printer::Print("BigQuerySchemaEntry::GetEntry creating table entry");
		// print the columns from create_info inside table_info
		auto &columns = table_entry->GetColumns();
		for (auto &col : columns.GetColumnNames()) {
			Printer::Print("BigQuerySchemaEntry::GetEntry column: " + col);
		}
		return GetCatalogSet(type).CreateEntry(std::move(table_entry));
	}
	return GetCatalogSet(type).GetEntry(transaction.GetContext(), name);
}

BigQueryCatalogSet &BigQuerySchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	// case CatalogType::INDEX_ENTRY:
	// 	return indexes;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
