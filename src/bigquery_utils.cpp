#include "bigquery_utils.hpp"
#include "storage/bigquery_schema_entry.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {

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

// LogicalType BigQueryUtils::TypeToLogicalType(ClientContext &context, const BigQueryTypeData &type_info) {
// 	if (type_info.type_name == "tinyint") {
// 		if (type_info.column_type == "tinyint(1)") {
// 			Value tinyint_as_boolean;
// 			if (context.TryGetCurrentSetting("bigquery_tinyint1_as_boolean", tinyint_as_boolean)) {
// 				if (BooleanValue::Get(tinyint_as_boolean)) {
// 					return LogicalType::BOOLEAN;
// 				}
// 			}
// 		}
// 		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
// 			return LogicalType::UTINYINT;
// 		} else {
// 			return LogicalType::TINYINT;
// 		}
// 	} else if (type_info.type_name == "smallint") {
// 		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
// 			return LogicalType::USMALLINT;
// 		} else {
// 			return LogicalType::SMALLINT;
// 		}
// 	} else if (type_info.type_name == "mediumint" || type_info.type_name == "int") {
// 		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
// 			return LogicalType::UINTEGER;
// 		} else {
// 			return LogicalType::INTEGER;
// 		}
// 	} else if (type_info.type_name == "bigint") {
// 		if (StringUtil::Contains(type_info.column_type, "unsigned")) {
// 			return LogicalType::UBIGINT;
// 		} else {
// 			return LogicalType::BIGINT;
// 		}
// 	} else if (type_info.type_name == "float") {
// 		return LogicalType::FLOAT;
// 	} else if (type_info.type_name == "double") {
// 		return LogicalType::DOUBLE;
// 	} else if (type_info.type_name == "date") {
// 		return LogicalType::DATE;
// 	} else if (type_info.type_name == "time") {
// 		// we need to convert time to VARCHAR because TIME in BigQuery is more like an
// 		// interval and can store ranges between -838:00:00 to 838:00:00
// 		return LogicalType::VARCHAR;
// 	} else if (type_info.type_name == "timestamp") {
// 		// in BigQuery, "timestamp" columns are timezone aware while "datetime" columns
// 		// are not
// 		return LogicalType::TIMESTAMP_TZ;
// 	} else if (type_info.type_name == "year") {
// 		return LogicalType::INTEGER;
// 	} else if (type_info.type_name == "datetime") {
// 		return LogicalType::TIMESTAMP;
// 	} else if (type_info.type_name == "decimal") {
// 		if (type_info.precision > 0 && type_info.precision <= 38) {
// 			return LogicalType::DECIMAL(type_info.precision, type_info.scale);
// 		}
// 		return LogicalType::DOUBLE;
// 	} else if (type_info.type_name == "json") {
// 		// FIXME
// 		return LogicalType::VARCHAR;
// 	} else if (type_info.type_name == "enum") {
// 		// FIXME: we can actually retrieve the enum values from the column_type
// 		return LogicalType::VARCHAR;
// 	} else if (type_info.type_name == "set") {
// 		// FIXME: set is essentially a list of enum
// 		return LogicalType::VARCHAR;
// 	} else if (type_info.type_name == "bit") {
// 		if (type_info.column_type == "bit(1)") {
// 			Value bit_as_boolean;
// 			if (context.TryGetCurrentSetting("bigquery_bit1_as_boolean", bit_as_boolean)) {
// 				if (BooleanValue::Get(bit_as_boolean)) {
// 					return LogicalType::BOOLEAN;
// 				}
// 			}
// 		}
// 		return LogicalType::BLOB;
// 	} else if (type_info.type_name == "blob" || type_info.type_name == "binary" || type_info.type_name == "varbinary" ||
// 	           type_info.type_name == "geometry" || type_info.type_name == "point" ||
// 	           type_info.type_name == "linestring" || type_info.type_name == "polygon" ||
// 	           type_info.type_name == "multipoint" || type_info.type_name == "multilinestring" ||
// 	           type_info.type_name == "multipolygon" || type_info.type_name == "geomcollection") {
// 		return LogicalType::BLOB;
// 	} else if (type_info.type_name == "varchar" || type_info.type_name == "mediumtext" ||
// 	           type_info.type_name == "longtext" || type_info.type_name == "text" || type_info.type_name == "enum" ||
// 	           type_info.type_name == "char") {
// 		return LogicalType::VARCHAR;
// 	}
// 	// fallback for unknown types
// 	return LogicalType::VARCHAR;
// }

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
