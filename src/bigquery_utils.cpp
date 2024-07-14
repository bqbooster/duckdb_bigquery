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
#include <string>

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

	class BQColumnRequest {
	private:
		json schema;
	public:
		BQColumnRequest(const json& schema): schema(schema) {}
		vector<BQField> ParseColumnFields() {
			return ParseColumnFields(this->schema);
		}
		vector<BQField> ParseColumnFields(const json& schema) {
			//Printer::Print("Parsing schema 1");
			vector<BQField> column_list;
			for (const auto& field : schema["fields"]) {
				auto field_name = field["name"].get<std::string>();
				auto field_type_str = field["type"].get<std::string>();
				std::vector<BQField> subfields;
				//Printer::Print("Parsing schema " + field_name + " " + field_type_str);

				if (field_type_str == "RECORD") {
					//Printer::Print("Parsing record");
					subfields = ParseColumnFields(field);
				}

				auto field_type = TypeToLogicalType(field_type_str, subfields);
				column_list.push_back(BQField(field_name, field_type));
			}
			return column_list;
		}

		LogicalType TypeToLogicalType(const std::string &bq_type, std::vector<BQField> subfields) {
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
			} else if(bq_type == "RECORD") {
				// transform the subfields into a list of LogicalType using a recursive call
				child_list_t<LogicalType> subfield_types;
				for (auto &subfield : subfields) {
					subfield_types.emplace_back(subfield.name, subfield.type);
				}
				auto duckdb_type = LogicalType::STRUCT(subfield_types);
				//Printer::Print("Struct type: " + duckdb_type.ToString());
				return duckdb_type;
				//return LogicalType::STRUCT();
			}
			Printer::Print("Unknown type: " + bq_type);
			// fallback for unknown types
			return LogicalType::VARCHAR;
		}
};

std::string GetAccessToken(const string &service_account_json) {
	google::cloud::StatusOr<std::shared_ptr<google::cloud::storage::oauth2::Credentials>> credentials;

	if(service_account_json.empty()) {
		credentials = gcpoauth2::GoogleDefaultCredentials();
	} else {
		//Printer::Print("GetAccessToken service_account_json: " + service_account_json);
		credentials = gcpoauth2::CreateServiceAccountCredentialsFromJsonContents(service_account_json);
	}
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
	const string &table,
	const string &service_account_json
	) {
	//Printer::Print("BigQueryReadTableEntry for execution_project: " + execution_project + " storage_project: " + storage_project + " dataset: " + dataset + " table: " + table);
	auto column_list = BigQueryUtils::BigQueryReadColumnListForTable(
		execution_project, storage_project, dataset, table, service_account_json);
	if (column_list.size() == 0) {
		return nullptr;
	}
	// print size of columns
	//auto column_list_size = column_list.size();
	//Printer::Print("column_list_size size: " + to_string(column_list_size));

	//Printer::Print("column_list done");
	auto table_info = make_uniq<BigQueryTableInfo>(dataset, table);
	auto &create_info = table_info->create_info;
	auto &columns = create_info->columns;
	for (auto &col : column_list) {
		ColumnDefinition column(std::move(col.name), std::move(col.type));
		columns.AddColumn(std::move(column));
	}
	// print size of columns
	//auto column_size = columns.GetColumnNames().size();
	//Printer::Print("columns size: " + to_string(column_size));
	auto table_entry = make_uniq<BigQueryTableEntry>(catalog, *schema_entry, *table_info);
	return table_entry;
}

	vector<BQField> BigQueryUtils::BigQueryReadColumnListForTable(
    const std::string &execution_project,
    const std::string &storage_project,
    const std::string &dataset,
    const std::string &table,
	const string &service_account_json) {
    //Printer::Print("BigQueryReadColumnListForTable for execution_project: " + execution_project + " storage_project: " + storage_project + " dataset: " + dataset + " table: " + table);

    std::string access_token = GetAccessToken(service_account_json);

    //Printer::Print("Access token: " + access_token);

    // Create HTTP client
    http_client client(U("https://bigquery.googleapis.com"));

    // Create request URI
    uri_builder builder(U("/bigquery/v2/projects/"));
    builder.append_path(storage_project);
    builder.append_path(U("datasets"));
    builder.append_path(dataset);
    builder.append_path(U("tables"));
    builder.append_path(table);

    //Printer::Print("Requesting: " + builder.to_string());

    // Create and send request
    http_request request(methods::GET);
    request.headers().add(U("Authorization"), U("Bearer ") + utility::conversions::to_string_t(access_token));
    request.set_request_uri(builder.to_uri());

    pplx::task<vector<BQField>> requestTask = client.request(request)
        .then([](http_response response) -> pplx::task<vector<BQField>> {
        if (response.status_code() == status_codes::OK) {
            return response.extract_json()
            .then([](web::json::value const& v) -> vector<BQField> {
				auto bqFields = BigQueryUtils::ParseColumnJSONResponse(v);
				// print bq fields
				for (auto &field : bqFields) {
					//Printer::Print("Field: " + field.name + " " + field.type.ToString());
				}
				return bqFields;
            });
        } else {
			//Printer::Print("Error: " + response.to_string());
			throw std::runtime_error("Failed to get column list for provided table, it's likely either an authentication issue or the table does not exist");
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

vector<BQField> BigQueryUtils::ParseColumnJSONResponse(web::json::value const& v){

	std::string str = v.serialize();
	//Printer::Print("received JSON: " + str);
	// Parse the JSON string
	json j = json::parse(str);
	auto schema = j["schema"];
	//return vector<BQField>();
	auto bcr = new BQColumnRequest(schema);
	return bcr->ParseColumnFields();
}

Value BigQueryUtils::ValueFromArrowScalar(std::shared_ptr<arrow::Scalar> scalar) {
	switch (scalar->type->id()) {
		case arrow::Type::INT64: {
			auto v = ((arrow::Int64Scalar *)scalar.get())->value;
			return Value(v);
			}
		case arrow::Type::STRING: {
			auto v = std::dynamic_pointer_cast<arrow::StringScalar>(scalar)->view();
			return Value(string(v));
			}
		case arrow::Type::DOUBLE: {
			return Value::DOUBLE(((arrow::DoubleScalar *)scalar.get())->value);
			}
		case arrow::Type::BOOL: {
			return Value::BOOLEAN(((arrow::BooleanScalar *)scalar.get())->value);
			}
		case arrow::Type::TIMESTAMP: {
			int64_t v = ((arrow::TimestampScalar *)scalar.get())->value;
			return Value::TIMESTAMP(timestamp_t(v));
			}
		case arrow::Type::DATE32: {
			int32_t v = ((arrow::Date32Scalar *)scalar.get())->value;
			return Value::DATE(date_t(v));
			}
		case arrow::Type::TIME32: {
			int32_t v = ((arrow::Time32Scalar *)scalar.get())->value;
			return Value::TIME(dtime_t(v));
			}
		case arrow::Type::TIME64: {
			int64_t v = ((arrow::Time64Scalar *)scalar.get())->value;
			return Value::TIME(dtime_t(v));
			}
		// case arrow::Type::DECIMAL: {
		// 	auto decimal_scalar = std::static_pointer_cast<arrow::Decimal128Scalar>(scalar);
		// 	// Extract the value, precision, and scale from the scalar
		// 	auto v = decimal_scalar->value;
		// 	int32_t precision = decimal_scalar->type->precision();
		// 	int32_t scale = decimal_scalar->type->scale();
		// 	return Value::DECIMAL(v, width, scale);
		// 	}
		case arrow::Type::BINARY: {
			arrow::BaseBinaryScalar::ValueType v = ((arrow::BinaryScalar *)scalar.get())->value;
			std::string data(reinterpret_cast<const char*>(v->data()), v->size());
			return Value::BLOB(data);
			}
		case arrow::Type::FIXED_SIZE_BINARY: {
			arrow::BaseBinaryScalar::ValueType v = ((arrow::FixedSizeBinaryScalar *)scalar.get())->value;
			std::string data(reinterpret_cast<const char*>(v->data()), v->size());
			return Value::BLOB(data);
			}
		case arrow::Type::FLOAT: {
			return Value(((arrow::FloatScalar *)scalar.get())->value);
			}
		case arrow::Type::UINT8: {
			return Value(((arrow::UInt8Scalar *)scalar.get())->value);
			}
		case arrow::Type::UINT16: {
			return Value(((arrow::UInt16Scalar *)scalar.get())->value);
			}
		case arrow::Type::UINT32: {
			return Value::UINTEGER(((arrow::UInt32Scalar *)scalar.get())->value);
			}
		case arrow::Type::UINT64: {
			return Value::UHUGEINT(((arrow::UInt64Scalar *)scalar.get())->value);
			}
		case arrow::Type::INT8: {
			return Value(((arrow::Int8Scalar *)scalar.get())->value);
			}
		case arrow::Type::INT16: {
			return Value(((arrow::Int16Scalar *)scalar.get())->value);
			}
		case arrow::Type::INT32: {
			return Value(((arrow::Int32Scalar *)scalar.get())->value);
			}
		case arrow::Type::HALF_FLOAT: {
			return Value(((arrow::HalfFloatScalar *)scalar.get())->value);
			}
		case arrow::Type::DURATION: {
			return Value(((arrow::DurationScalar *)scalar.get())->value);
			}
		case arrow::Type::LIST: {
			// Extract the list value from the ListScalar
			auto list_scalar = std::static_pointer_cast<arrow::ListScalar>(scalar);
			std::shared_ptr<arrow::Array> list_array = list_scalar->value;

			// Convert each element in the list to a Value
			std::vector<Value> values;
			for (int64_t i = 0; i < list_array->length(); i++) {
				// Get the element scalar
				auto element_scalar_result = list_array->GetScalar(i);
				if (!element_scalar_result.ok()) {
					throw std::runtime_error("Failed to get scalar from array element");
				}
				auto element_scalar = element_scalar_result.ValueOrDie();

				// Convert the element scalar to a Value (recursive call, assuming convertToValue function exists)
				values.push_back(ValueFromArrowScalar(element_scalar));
			}
			return Value::LIST(std::move(values));
			}
		case arrow::Type::STRUCT: {
			 // Extract the struct value from the StructScalar
			 //Printer::Print("Struct scalar");
			auto struct_scalar = std::static_pointer_cast<arrow::StructScalar>(scalar);
			const auto& struct_values = struct_scalar->value;
			const auto& field_names = struct_scalar->type->fields();
			//Printer::Print("Struct scalar values: " + to_string(struct_values.size()));
			//Printer::Print("Struct scalar names: " + to_string(field_names.size()));
			// Convert each field in the struct to a Value
			child_list_t<Value> values;
			for (size_t i = 0; i < struct_values.size(); i++) {
				// Convert the field scalar to a Value (recursive call, assuming convertToValue function exists)
				auto field_value = ValueFromArrowScalar(struct_values[i]);
				// Collect the field name and its corresponding Value
				values.emplace_back(field_names[i]->name(), std::move(field_value));
			}

			// Create and return a Value using the STRUCT function
			return Value::STRUCT(std::move(values));
			}
		case arrow::Type::MAP: {
			// Extract the map value from the MapScalar
			auto map_scalar = std::static_pointer_cast<arrow::MapScalar>(scalar);
			auto map_array = std::static_pointer_cast<arrow::StructArray>(map_scalar->value);

			// Extract keys and values arrays
			auto key_array = std::static_pointer_cast<arrow::StringArray>(map_array->field(0));
			auto value_array = std::static_pointer_cast<arrow::StringArray>(map_array->field(1));

			// Check if the key and value arrays have the same length
			if (key_array->length() != value_array->length()) {
				throw std::runtime_error("Key and value arrays have different lengths");
			}

			// Convert each key-value pair to a Value
			unordered_map<string, string> kv_pairs;
			for (int64_t i = 0; i < key_array->length(); i++) {
				// Get the key and value as strings
				std::string key = key_array->GetString(i);
				std::string value = value_array->GetString(i);

				// Insert the key-value pair into the unordered_map
				kv_pairs.emplace(std::move(key), std::move(value));
			}

			// Create and return a Value using the MAP function
			return Value::MAP(kv_pairs);
			}
		case arrow::Type::DICTIONARY: {
			// Extract the dictionary value from the DictionaryScalar
			auto dict_scalar = std::static_pointer_cast<arrow::DictionaryScalar>(scalar);
			const auto& dict_value = dict_scalar->value;
			const auto& index_scalar = dict_value.index;
			const auto& dictionary_array = dict_value.dictionary;

			// Ensure that index_scalar is valid and retrieve its value
			if (!index_scalar->is_valid) {
				throw std::runtime_error("Invalid index in DictionaryScalar");
			}

			// Convert the index scalar to an integer value
			int64_t index_value = 0;
			switch (index_scalar->type->id()) {
				case arrow::Type::INT8:
					index_value = std::static_pointer_cast<arrow::Int8Scalar>(index_scalar)->value;
					break;
				case arrow::Type::INT16:
					index_value = std::static_pointer_cast<arrow::Int16Scalar>(index_scalar)->value;
					break;
				case arrow::Type::INT32:
					index_value = std::static_pointer_cast<arrow::Int32Scalar>(index_scalar)->value;
					break;
				case arrow::Type::INT64:
					index_value = std::static_pointer_cast<arrow::Int64Scalar>(index_scalar)->value;
					break;
				default:
					throw std::runtime_error("Unsupported index type in DictionaryScalar");
			}

			// Retrieve the value from the dictionary array at the specified index
			auto value_scalar_result = dictionary_array->GetScalar(index_value);
			if (!value_scalar_result.ok()) {
				throw std::runtime_error("Failed to get scalar from dictionary array");
			}
			auto value_scalar = value_scalar_result.ValueOrDie();

			// Convert the retrieved value scalar to a Value
			Value result_value = ValueFromArrowScalar(value_scalar);

			// Return the resulting Value
			return result_value;

			}
		case arrow::Type::NA: {
			return Value();
			}
		default:
			throw NotImplementedException("Unsupported type for BigQuery conversion");
	}
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
