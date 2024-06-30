#include "storage/bigquery_schema_entry.hpp"
#include "bigquery_utils.hpp"
#include "bigquery_result.hpp"

#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include <google/cloud/credentials.h>
#include <google/cloud/status_or.h>
#include <google/cloud/storage/oauth2/google_credentials.h>
#include <nlohmann/json.hpp>
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <iostream>
#include <fstream>
#include <sstream>

#include <arrow/api.h>
#include <arrow/array/data.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;
namespace gcs = google::cloud::storage;
namespace gcpoauth2 = google::cloud::storage::oauth2;
using json = nlohmann::json;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;

namespace duckdb {

std::string GetAccessToken() {
	auto credentials = gcpoauth2::GoogleDefaultCredentials();
	if (!credentials) {
		throw std::runtime_error("Failed to create credentials: " + credentials.status().message());
	}
	auto token = credentials.value()->AuthorizationHeader();
	if (!token) {
		throw std::runtime_error("Failed to obtain access token: " + token.status().message());
	}
	// Remove the "Authorization: Bearer " prefix
	return token->substr(21);
}

unique_ptr<BigQueryTableEntry> BigQueryUtils::BigQueryCreateBigQueryTableEntry(
	Catalog &catalog,
	BigQuerySchemaEntry * schema_entry,
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table) {
	Printer::Print("BigQueryReadTableEntry for execution_project: " + execution_project + " storage_project: " + storage_project + " dataset: " + dataset + " table: " + table);
	auto column_list = BigQueryUtils::BigQueryReadColumnListForTable(
		execution_project, storage_project, dataset, table);
	if (column_list.size() == 0) {
		return nullptr;
	}
	// print size of columns
	auto column_list_size = column_list.size();
	Printer::Print("column_list_size size: " + to_string(column_list_size));

	Printer::Print("column_list done");
	auto table_info = make_uniq<BigQueryTableInfo>(dataset, table);
	auto &create_info = table_info->create_info;
	auto &columns = create_info->columns;
	for (auto &col : column_list) {
		ColumnDefinition column(std::move(col.name), std::move(col.type));
		columns.AddColumn(std::move(column));
	}
	// print size of columns
	auto column_size = columns.GetColumnNames().size();
	Printer::Print("columns size: " + to_string(column_size));
	auto table_entry = make_uniq<BigQueryTableEntry>(catalog, *schema_entry, *table_info);
	return table_entry;
}

	vector<BQField> BigQueryUtils::BigQueryReadColumnListForTable(
    const std::string &execution_project,
    const std::string &storage_project,
    const std::string &dataset,
    const std::string &table) {
    Printer::Print("BigQueryReadColumnListForTable for execution_project: " + execution_project + " storage_project: " + storage_project + " dataset: " + dataset + " table: " + table);

    std::string access_token = GetAccessToken();

    Printer::Print("Access token: " + access_token);

    // Create HTTP client
    http_client client(U("https://bigquery.googleapis.com"));

    // Create request URI
    uri_builder builder(U("/bigquery/v2/projects/"));
    builder.append_path(storage_project);
    builder.append_path(U("datasets"));
    builder.append_path(dataset);
    builder.append_path(U("tables"));
    builder.append_path(table);

    Printer::Print("Requesting: " + builder.to_string());

    // Create and send request
    http_request request(methods::GET);
    request.headers().add(U("Authorization"), U("Bearer ") + utility::conversions::to_string_t(access_token));
    request.set_request_uri(builder.to_uri());

    pplx::task<vector<BQField>> requestTask = client.request(request)
        .then([](http_response response) -> pplx::task<vector<BQField>> {
        if (response.status_code() == status_codes::OK) {
            return response.extract_json()
            .then([](web::json::value const& v) -> vector<BQField> {
                std::string str = v.serialize();
                Printer::Print("received JSON: " + str);

                // Parse the JSON string
                json j = json::parse(str);

                // Extract the fields
                vector<BQField> column_list;
                for (const auto& field : j["schema"]["fields"]) {
					auto field_name = field["name"].get<std::string>();
                    auto field_type = BigQueryUtils::TypeToLogicalType(field["type"]);
					//add BQField to column_list
					column_list.push_back(BQField(field_name, field_type));
                }

                for (const auto& field : column_list) {
                    Printer::Print("field: " + field.name + " type: " + field.type.ToString());
                }

                return column_list;
            });
        }
        return pplx::task_from_result(vector<BQField>());
    });

    // Wait for all the outstanding I/O to complete and handle any exceptions
    try {
        auto column_list = requestTask.get();
        return column_list;
    }
    catch (const std::exception &e) {
        Printer::Print("Error: " + std::string(e.what()));
        return vector<BQField>();
    }
    return vector<BQField>();
}

unique_ptr<BigQueryResult> BigQueryUtils::BigQueryReadTable(
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table,
	const vector<string> &column_names) {
	Printer::Print("BigQueryReadTable: " + execution_project + " " + storage_project + "." + dataset + "." + table);
	std::string const project_name = "projects/" + execution_project;
  	// table_name should be in the format:
  	// "projects/<project-table-resides-in>/datasets/<dataset-table_resides-in>/tables/<table
  	// name>" The project values in project_name and table_name do not have to be
  	// identical.
  	std::string const table_name = "projects/" + storage_project + "/datasets/" + dataset + "/tables/" + table;

	constexpr int max_streams = 1;
	// Create the ReadSession.
	auto client = std::move(bigquery_storage::BigQueryReadClient(
		bigquery_storage::MakeBigQueryReadConnection()));
	bigquery_storage_read::ReadSession read_session;
	read_session.set_data_format(
		google::cloud::bigquery::storage::v1::DataFormat::ARROW);
	read_session.set_table(table_name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		read_session.mutable_read_options()->add_selected_fields(column_names[c]);
	}
	Printer::Print("column_names size: " + to_string(column_names.size()));
	auto session =
		client.CreateReadSession(project_name, read_session, max_streams);
	if(session.ok()) {
		auto session_value = std::move(session.value());
			// Get schema.
	std::shared_ptr<arrow::Schema> schema =
		BigQueryUtils::GetArrowSchema(session_value.arrow_schema());

		return make_uniq<BigQueryResult>(
			execution_project,
			storage_project,
			dataset,
			table,
			std::move(schema),
			make_uniq<bigquery_storage_read::ReadSession>(std::move(session_value)),
			make_uniq<bigquery_storage::BigQueryReadClient>(std::move(client))
			);
	}
	else {
		Printer::Print("Error creating ReadSession: " + session.status().message());
		return unique_ptr<BigQueryResult>();
	}
}

LogicalType BigQueryUtils::TypeToLogicalType(const std::string &bq_type) {
    if (bq_type == "INTEGER") {
        return LogicalType::BIGINT;
    } else if (bq_type == "FLOAT") {
        return LogicalType::DOUBLE;
    } else if (bq_type == "DATE") {
        return LogicalType::DATE;
    } else if (bq_type == "TIME") {
        // we need to convert time to VARCHAR because TIME in BigQuery is more like an
        // interval and can store ranges between -838:00:00 to 838:00:00
        return LogicalType::VARCHAR;
    } else if (bq_type == "TIMESTAMP") {
        // in BigQuery, "timestamp" columns are timezone aware while "datetime" columns
        // are not
        return LogicalType::TIMESTAMP_TZ;
    } else if (bq_type == "YEAR") {
        return LogicalType::INTEGER;
    } else if (bq_type == "DATETIME") {
        return LogicalType::TIMESTAMP;
    } else if (bq_type == "NUMERIC" || bq_type == "BIGNUMERIC") {
        // BigQuery NUMERIC and BIGNUMERIC types can have a precision up to 38 and a scale up to 9
        // Assume a default precision and scale for this example; these could be parameterized if needed
        return LogicalType::DECIMAL(38, 9);
    } else if (bq_type == "JSON") {
        // FIXME
        return LogicalType::VARCHAR;
    } else if (bq_type == "BYTES") {
        return LogicalType::BLOB;
    } else if (bq_type == "STRING") {
        return LogicalType::VARCHAR;
    } else if(bq_type == "BOOLEAN") {
		return LogicalType::BOOLEAN;
	}
	Printer::Print("Unknown type: " + bq_type);
    // fallback for unknown types
    return LogicalType::VARCHAR;
}

string BigQueryUtils::EscapeQuotes(const string &text, char quote) {
	string result;
	for (auto c : text) {
		if (c == quote) {
			result += "\\";
			result += quote;
		} else if (c == '\\') {
			result += "\\\\";
		} else {
			result += c;
		}
	}
	return result;
}

std::shared_ptr<arrow::Schema> BigQueryUtils::GetArrowSchema(
    ::google::cloud::bigquery::storage::v1::ArrowSchema const& schema_in) {
  std::shared_ptr<arrow::Buffer> buffer =
      std::make_shared<arrow::Buffer>(schema_in.serialized_schema());
  arrow::io::BufferReader buffer_reader(buffer);
  arrow::ipc::DictionaryMemo dictionary_memo;
  auto result = arrow::ipc::ReadSchema(&buffer_reader, &dictionary_memo);
  if (!result.ok()) {
	Printer::Print("Unable to parse schema: " + result.status().message());
    throw result.status();
  }
  std::shared_ptr<arrow::Schema> schema = result.ValueOrDie();
  //Printer::Print("Schema: " + schema->ToString());
  return schema;
}

string BigQueryUtils::WriteQuoted(const string &text, char quote) {
	// 1. Escapes all occurences of 'quote' by escaping them with a backslash
	// 2. Adds quotes around the string
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string BigQueryUtils::WriteIdentifier(const string &identifier) {
	return BigQueryUtils::WriteQuoted(identifier, '`');
}

string BigQueryUtils::WriteLiteral(const string &identifier) {
	return BigQueryUtils::WriteQuoted(identifier, '\'');
}

} // namespace duckdb
