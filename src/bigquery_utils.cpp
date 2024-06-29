#include "bigquery_utils.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"
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

namespace duckdb {

	static optional_ptr<BigQueryResult> BigQueryReadTable(
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table,
	const vector<string> &column_names) {
		Printer::Print("BigQueryReadTable");
	 std::string const project_name = "projects/" + execution_project;
  	// table_name should be in the format:
  	// "projects/<project-table-resides-in>/datasets/<dataset-table_resides-in>/tables/<table
  	// name>" The project values in project_name and table_name do not have to be
  	// identical.
  	std::string const table_name = "projects/" + storage_project + "/datasets/" + dataset + "/tables/" + table;

	constexpr int max_streams = 1;
	// Create the ReadSession.
	auto client = bigquery_storage::BigQueryReadClient(
		bigquery_storage::MakeBigQueryReadConnection());
	bigquery_storage_read::ReadSession read_session;
	read_session.set_data_format(
		google::cloud::bigquery::storage::v1::DataFormat::ARROW);
	read_session.set_table(table_name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		read_session.mutable_read_options()->add_selected_fields(column_names[c]);
	}
	auto session =
		client.CreateReadSession(project_name, read_session, max_streams);
	if(session.ok()) {
		bigquery_storage_read::ReadSession &session_value = session.value();
		BigQueryResult bq_result = BigQueryResult(session_value, client);
		return optional_ptr<BigQueryResult>(bq_result);
	}
	else {
		return optional_ptr<BigQueryResult>();
	}
}

namespace gcs = google::cloud::storage;
namespace gcpoauth2 = google::cloud::storage::oauth2;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;
using json = nlohmann::json;

struct Field {
    std::string name;
    std::string type;
};

std::vector<Field> extractFields(const std::string& jsonString, duckdb::unique_ptr<duckdb::ColumnList> column_list) {
    // Parse the JSON string
    json j = json::parse(jsonString);

    // Extract the fields
    std::vector<Field> fields;
    for (const auto& field : j["schema"]["fields"]) {
        fields.push_back({field["name"], field["type"]});
    }

    return fields;
}

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

static optional_ptr<ColumnList> BigQueryReadColumnListForTable(
	const string &execution_project,
	const string &storage_project,
	const string &dataset,
	const string &table) {
	Printer::Print("BigQueryReadColumnListForTable");
	auto column_list = make_uniq<ColumnList>();
	// hope for a miracle so that we can Google C++ API to get the column list with reimplementing everything
	//TODO: implement this

	try {
        std::string access_token = GetAccessToken();

        // Create HTTP client
        http_client client(U("https://bigquery.googleapis.com"));

        // Create request URI
        uri_builder builder(U("/bigquery/v2/projects/"));
        builder.append_path(storage_project);
        builder.append_path(U("datasets"));
        builder.append_path(dataset);
        builder.append_path(U("tables"));
        builder.append_path(table);

        // Create and send request
        http_request request(methods::GET);
        request.headers().add(U("Authorization"), U("Bearer ") + utility::conversions::to_string_t(access_token));
        request.set_request_uri(builder.to_uri());

        pplx::task<void> requestTask = client.request(request)
            .then([](http_response response) {
                if (response.status_code() == status_codes::OK) {
                    return response.extract_json();
                }
                return pplx::task_from_result(web::json::value());
            })
            .then([&column_list](pplx::task<web::json::value> previousTask) {
                try {
                    web::json::value const& v = previousTask.get();
                    std::string str = v.serialize();
                    std::cout << "v : " << str << std::endl;

					// Parse the JSON string
				    json j = json::parse(str);
					// Extract the fields

					// Extract the fields
					for (const auto& field : j["schema"]["fields"]) {
						auto field_type = BigQueryUtils::TypeToLogicalType(field["type"]);
						column_list->AddColumn(ColumnDefinition(
							field["name"],
							 field_type));
					}

                    for (const auto& field : column_list->GetColumnNames()) {
                        std::cout << field << std::endl;
                    }
                }
                catch (http_exception const& e) {
                    std::wcout << e.what() << std::endl;
                }
            });

        // Wait for all the outstanding I/O to complete and handle any exceptions
        try {
            requestTask.wait();
        }
        catch (const std::exception &e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return column_list;
    }
	return column_list;
	}

// static bool ParseValue(const string &dsn, idx_t &pos, string &result) {
// 	// skip leading spaces
// 	while (pos < dsn.size() && StringUtil::CharacterIsSpace(dsn[pos])) {
// 		pos++;
// 	}
// 	if (pos >= dsn.size()) {
// 		return false;
// 	}
// 	// check if we are parsing a quoted value or not
// 	if (dsn[pos] == '"') {
// 		pos++;
// 		// scan until we find another quote
// 		bool found_quote = false;
// 		for (; pos < dsn.size(); pos++) {
// 			if (dsn[pos] == '"') {
// 				found_quote = true;
// 				pos++;
// 				break;
// 			}
// 			if (dsn[pos] == '\\') {
// 				// backslash escapes the backslash or double-quote
// 				if (pos + 1 >= dsn.size()) {
// 					throw InvalidInputException("Invalid dsn \"%s\" - backslash at end of dsn", dsn);
// 				}
// 				if (dsn[pos + 1] != '\\' && dsn[pos + 1] != '"') {
// 					throw InvalidInputException("Invalid dsn \"%s\" - backslash can only escape \\ or \"", dsn);
// 				}
// 				result += dsn[pos + 1];
// 				pos++;
// 			} else {
// 				result += dsn[pos];
// 			}
// 		}
// 		if (!found_quote) {
// 			throw InvalidInputException("Invalid dsn \"%s\" - unterminated quote", dsn);
// 		}
// 	} else {
// 		// unquoted value, continue until space, equality sign or end of string
// 		for (; pos < dsn.size(); pos++) {
// 			if (dsn[pos] == '=') {
// 				break;
// 			}
// 			if (StringUtil::CharacterIsSpace(dsn[pos])) {
// 				break;
// 			}
// 			result += dsn[pos];
// 		}
// 	}
// 	return true;
// }

// bool ReadOptionFromEnv(const char *env, string &result) {
// 	auto res = std::getenv(env);
// 	if (!res) {
// 		return false;
// 	}
// 	result = res;
// 	return true;
// }

// uint32_t ParsePort(const string &value) {
// 	constexpr const static int PORT_MIN = 0;
// 	constexpr const static int PORT_MAX = 65353;
// 	int port_val = std::stoi(value);
// 	if (port_val < PORT_MIN || port_val > PORT_MAX) {
// 		throw InvalidInputException("Invalid port %d - port must be between %d and %d", port_val, PORT_MIN, PORT_MAX);
// 	}
// 	return uint32_t(port_val);
// }

// BigQueryConnectionParameters BigQueryUtils::ParseConnectionParameters(const string &dsn) {
// 	BigQueryConnectionParameters result;

// 	unordered_set<string> set_options;
// 	// parse options
// 	idx_t pos = 0;
// 	while (pos < dsn.size()) {
// 		string key;
// 		string value;
// 		if (!ParseValue(dsn, pos, key)) {
// 			break;
// 		}
// 		if (pos >= dsn.size() || dsn[pos] != '=') {
// 			throw InvalidInputException("Invalid dsn \"%s\" - expected key=value pairs separated by spaces", dsn);
// 		}
// 		pos++;
// 		if (!ParseValue(dsn, pos, value)) {
// 			throw InvalidInputException("Invalid dsn \"%s\" - expected key=value pairs separated by spaces", dsn);
// 		}
// 		key = StringUtil::Lower(key);
// 		if (key == "host") {
// 			set_options.insert("host");
// 			result.host = value;
// 		} else if (key == "user") {
// 			set_options.insert("user");
// 			result.user = value;
// 		} else if (key == "passwd" || key == "password") {
// 			set_options.insert("password");
// 			result.passwd = value;
// 		} else if (key == "db" || key == "database") {
// 			set_options.insert("database");
// 			result.db = value;
// 		} else if (key == "port") {
// 			set_options.insert("port");
// 			result.port = ParsePort(value);
// 		} else if (key == "socket" || key == "unix_socket") {
// 			set_options.insert("socket");
// 			result.unix_socket = value;
// 		} else {
// 			throw InvalidInputException("Unrecognized configuration parameter \"%s\" "
// 			                            "- expected options are host, "
// 			                            "user, passwd, db, port, socket",
// 			                            key);
// 		}
// 	}
// 	// read options that are not set from environment variables
// 	if (set_options.find("host") == set_options.end()) {
// 		ReadOptionFromEnv("BIGQUERY_HOST", result.host);
// 	}
// 	if (set_options.find("password") == set_options.end()) {
// 		ReadOptionFromEnv("BIGQUERY_PWD", result.passwd);
// 	}
// 	if (set_options.find("user") == set_options.end()) {
// 		ReadOptionFromEnv("BIGQUERY_USER", result.user);
// 	}
// 	if (set_options.find("database") == set_options.end()) {
// 		ReadOptionFromEnv("BIGQUERY_DATABASE", result.db);
// 	}
// 	if (set_options.find("socket") == set_options.end()) {
// 		ReadOptionFromEnv("BIGQUERY_UNIX_PORT", result.unix_socket);
// 	}
// 	if (set_options.find("port") == set_options.end()) {
// 		string port_number;
// 		if (ReadOptionFromEnv("BIGQUERY_TCP_PORT", port_number)) {
// 			result.port = ParsePort(port_number);
// 		}
// 	}
// 	return result;
// }

/*
BIGQUERY *BigQueryUtils::Connect(const string &dsn) {
	BIGQUERY *bigquery = bigquery_init(NULL);
	if (!bigquery) {
		throw IOException("Failure in bigquery_init");
	}
	BIGQUERY *result;
	auto config = ParseConnectionParameters(dsn);
	const char *host = config.host.empty() ? nullptr : config.host.c_str();
	const char *user = config.user.empty() ? nullptr : config.user.c_str();
	const char *passwd = config.passwd.empty() ? nullptr : config.passwd.c_str();
	const char *db = config.db.empty() ? nullptr : config.db.c_str();
	const char *unix_socket = config.unix_socket.empty() ? nullptr : config.unix_socket.c_str();
	result = bigquery_real_connect(bigquery, host, user, passwd, db, config.port, unix_socket, config.client_flag);
	if (!result) {
		if (config.host.empty() || config.host == "localhost") {
			// retry
			result =
			    bigquery_real_connect(bigquery, "127.0.0.1", user, passwd, db, config.port, unix_socket, config.client_flag);
			if (result) {
				return result;
			}
		}
		throw IOException("Failed to connect to BigQuery database with parameters \"%s\": %s", dsn, bigquery_error(bigquery));
	}
	D_ASSERT(bigquery == result);
	return result;
}
*/

// string BigQueryUtils::TypeToString(const LogicalType &input) {
// 	switch (input.id()) {
// 	case LogicalType::VARCHAR:
// 		return "TEXT";
// 	case LogicalType::UTINYINT:
// 		return "TINYINT UNSIGNED";
// 	case LogicalType::USMALLINT:
// 		return "SMALLINT UNSIGNED";
// 	case LogicalType::UINTEGER:
// 		return "INTEGER UNSIGNED";
// 	case LogicalType::UBIGINT:
// 		return "BIGINT UNSIGNED";
// 	case LogicalType::TIMESTAMP:
// 		return "DATETIME";
// 	case LogicalType::TIMESTAMP_TZ:
// 		return "TIMESTAMP";
// 	default:
// 		return input.ToString();
// 	}
// }

LogicalType BigQueryUtils::TypeToLogicalType(const std::string &bq_type) {
    if (bq_type == "INTEGER") {
        return LogicalType::BIGINT;
    } else if (bq_type == "FLOAT64") {
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
    }
	std::cout << "Unknown type: " << bq_type << std::endl;
    // fallback for unknown types
    return LogicalType::VARCHAR;
}


/*
LogicalType BigQueryUtils::FieldToLogicalType(ClientContext &context, BIGQUERY_FIELD *field) {
	BigQueryTypeData type_data;
	switch (field->type) {
	case BIGQUERY_TYPE_TINY:
		type_data.type_name = "tinyint";
		break;
	case BIGQUERY_TYPE_SHORT:
		type_data.type_name = "smallint";
		break;
	case BIGQUERY_TYPE_INT24:
		type_data.type_name = "mediumint";
		break;
	case BIGQUERY_TYPE_LONG:
		type_data.type_name = "int";
		break;
	case BIGQUERY_TYPE_LONGLONG:
		type_data.type_name = "bigint";
		break;
	case BIGQUERY_TYPE_FLOAT:
		type_data.type_name = "float";
		break;
	case BIGQUERY_TYPE_DOUBLE:
		type_data.type_name = "double";
		break;
	case BIGQUERY_TYPE_DECIMAL:
	case BIGQUERY_TYPE_NEWDECIMAL:
		type_data.precision = int64_t(field->max_length) - 2; // -2 for minus sign and dot
		type_data.scale = field->decimals;
		type_data.type_name = "decimal";
		break;
	case BIGQUERY_TYPE_TIMESTAMP:
		type_data.type_name = "timestamp";
		break;
	case BIGQUERY_TYPE_DATE:
		type_data.type_name = "date";
		break;
	case BIGQUERY_TYPE_TIME:
		type_data.type_name = "time";
		break;
	case BIGQUERY_TYPE_DATETIME:
		type_data.type_name = "datetime";
		break;
	case BIGQUERY_TYPE_YEAR:
		type_data.type_name = "year";
		break;
	case BIGQUERY_TYPE_BIT:
		type_data.type_name = "bit";
		break;
	case BIGQUERY_TYPE_GEOMETRY:
		type_data.type_name = "geometry";
		break;
	case BIGQUERY_TYPE_NULL:
		type_data.type_name = "null";
		break;
	case BIGQUERY_TYPE_SET:
		type_data.type_name = "set";
		break;
	case BIGQUERY_TYPE_ENUM:
		type_data.type_name = "enum";
		break;
	case BIGQUERY_TYPE_BLOB:
	case BIGQUERY_TYPE_STRING:
	case BIGQUERY_TYPE_VAR_STRING:
		if (field->flags & BINARY_FLAG) {
			type_data.type_name = "blob";
		} else {
			type_data.type_name = "varchar";
		}
		break;
	default:
		type_data.type_name = "__unknown_type";
		break;
	}
	type_data.column_type = type_data.type_name;
	if (field->max_length != 0) {
		type_data.column_type += "(" + std::to_string(field->max_length) + ")";
	}
	if (field->flags & UNSIGNED_FLAG && field->flags & NUM_FLAG) {
		type_data.column_type += " unsigned";
	}
	return BigQueryUtils::TypeToLogicalType(context, type_data);
}
*/

// LogicalType BigQueryUtils::ToBigQueryType(const LogicalType &input) {
// 	switch (input.id()) {
// 	case LogicalTypeId::BOOLEAN:
// 	case LogicalTypeId::SMALLINT:
// 	case LogicalTypeId::INTEGER:
// 	case LogicalTypeId::BIGINT:
// 	case LogicalTypeId::TINYINT:
// 	case LogicalTypeId::UTINYINT:
// 	case LogicalTypeId::USMALLINT:
// 	case LogicalTypeId::UINTEGER:
// 	case LogicalTypeId::UBIGINT:
// 	case LogicalTypeId::FLOAT:
// 	case LogicalTypeId::DOUBLE:
// 	case LogicalTypeId::BLOB:
// 	case LogicalTypeId::DATE:
// 	case LogicalTypeId::DECIMAL:
// 	case LogicalTypeId::TIMESTAMP:
// 	case LogicalTypeId::TIMESTAMP_TZ:
// 	case LogicalTypeId::VARCHAR:
// 		return input;
// 	case LogicalTypeId::LIST:
// 		throw NotImplementedException("BigQuery does not support arrays - unsupported type \"%s\"", input.ToString());
// 	case LogicalTypeId::STRUCT:
// 	case LogicalTypeId::MAP:
// 	case LogicalTypeId::UNION:
// 		throw NotImplementedException("BigQuery does not support composite types - unsupported type \"%s\"",
// 		                              input.ToString());
// 	case LogicalTypeId::TIMESTAMP_SEC:
// 	case LogicalTypeId::TIMESTAMP_MS:
// 	case LogicalTypeId::TIMESTAMP_NS:
// 		return LogicalType::TIMESTAMP;
// 	case LogicalTypeId::HUGEINT:
// 		return LogicalType::DOUBLE;
// 	default:
// 		return LogicalType::VARCHAR;
// 	}
// }

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
