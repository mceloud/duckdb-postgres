#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_connection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

static bool debug_postgres_print_queries = false;

OwnedPostgresConnection::OwnedPostgresConnection(PGconn *conn) : connection(conn) {
}

OwnedPostgresConnection::~OwnedPostgresConnection() {
	if (!connection) {
		return;
	}
	PQfinish(connection);
	connection = nullptr;
}

PostgresConnection::PostgresConnection(shared_ptr<OwnedPostgresConnection> connection_p)
    : connection(std::move(connection_p)) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &connection_string) {
	PostgresConnection result;
	result.connection = make_shared_ptr<OwnedPostgresConnection>(PostgresUtils::PGConnect(connection_string));
	result.dsn = connection_string;
	return result;
}

static bool ResultHasError(PGresult *result) {
	if (!result) {
		return true;
	}
	switch (PQresultStatus(result)) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
		return false;
	default:
		return true;
	}
}

PGresult *PostgresConnection::PQExecute(const string &query) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("DEBUG: About to execute query: " + query + "\n");
	}
	auto result = PQexec(GetConn(), query.c_str());
	if (PostgresConnection::DebugPrintQueries()) {
		auto status = result ? PQresultStatus(result) : PGRES_FATAL_ERROR;
		Printer::Print("DEBUG: Query execution completed with status: " + std::to_string(status) + "\n");
		if (result && ResultHasError(result)) {
			Printer::Print("DEBUG: Query execution error: " + string(PQresultErrorMessage(result)) + "\n");
		}
	}
	return result;
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(const string &query, optional_ptr<string> error_message) {
	lock_guard<mutex> guard(connection->connection_lock);
	auto result = PQExecute(query.c_str());
	if (ResultHasError(result)) {
		if (error_message) {
			*error_message = StringUtil::Format("Failed to execute query \"" + query +
			                                    "\": " + string(PQresultErrorMessage(result)));
		}
		PQclear(result);
		return nullptr;
	}
	
	// Handle unexpected PGRES_COPY_OUT results
	if (PQresultStatus(result) == PGRES_COPY_OUT) {
		if (PostgresConnection::DebugPrintQueries()) {
			Printer::Print("DEBUG: TryQuery - Unexpected PGRES_COPY_OUT result for query: " + query + "\n");
		}
		
		// We need to consume the copy data even though we're not expecting it
		char *out_buffer;
		int len;
		idx_t copy_data_chunks = 0;
		while ((len = PQgetCopyData(GetConn(), &out_buffer, 0)) >= 0) {
			copy_data_chunks++;
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: TryQuery - Consumed unexpected copy data chunk " + std::to_string(copy_data_chunks) + 
				               " (size: " + std::to_string(len) + " bytes)\n");
			}
			// Free the buffer data (we're just consuming it)
			if (out_buffer) {
				PQfreemem(out_buffer);
			}
		}
		if (len == -2) {
			// Error occurred during copy
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: TryQuery - Error during unexpected copy data consumption\n");
			}
			PQclear(result);
			if (error_message) {
				*error_message = "Error during unexpected COPY OUT data consumption: " + string(PQerrorMessage(GetConn()));
			}
			return nullptr;
		}
		if (PostgresConnection::DebugPrintQueries()) {
			Printer::Print("DEBUG: TryQuery - Unexpected copy data ended after " + std::to_string(copy_data_chunks) + 
			               " chunks, consuming remaining results\n");
		}
		// len == -1 means end of copy data, now consume all remaining results
		idx_t remaining_results = 0;
		while (true) {
			auto copy_result = PQgetResult(GetConn());
			if (!copy_result) {
				break;
			}
			remaining_results++;
			PostgresResult pg_copy_result(copy_result);
			auto copy_status = PQresultStatus(copy_result);
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: TryQuery - Consumed remaining result " + std::to_string(remaining_results) + 
				               " with status: " + std::to_string(copy_status) + "\n");
			}
			if (copy_status != PGRES_COMMAND_OK) {
				auto error_msg = string(PQresultErrorMessage(copy_result));
				if (PostgresConnection::DebugPrintQueries()) {
					Printer::Print("DEBUG: TryQuery - Unexpected result status during copy completion: " + error_msg + "\n");
				}
				PQclear(result);
				if (error_message) {
					*error_message = "Failed to complete unexpected COPY operation: " + error_msg;
				}
				return nullptr;
			}
		}
		if (PostgresConnection::DebugPrintQueries()) {
			Printer::Print("DEBUG: TryQuery - Unexpected COPY OUT operation completed successfully, consumed " + 
			               std::to_string(remaining_results) + " remaining results\n");
		}
	}
	
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	string error_msg;
	auto result = TryQuery(query, &error_msg);
	if (!result) {
		throw std::runtime_error(error_msg);
	}
	return result;
}

void PostgresConnection::Execute(const string &query) {
	Query(query);
}

vector<unique_ptr<PostgresResult>> PostgresConnection::ExecuteQueries(const string &queries) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(queries + "\n");
	}
	auto res = PQsendQuery(GetConn(), queries.c_str());
	if (res == 0) {
		throw std::runtime_error("Failed to execute query \"" + queries + "\": " + string(PQerrorMessage(GetConn())));
	}
	vector<unique_ptr<PostgresResult>> results;
	while (true) {
		auto res = PQgetResult(GetConn());
		if (!res) {
			break;
		}
		auto result = make_uniq<PostgresResult>(res);
		if (ResultHasError(res)) {
			throw std::runtime_error("Failed to execute query \"" + queries +
			                         "\": " + string(PQresultErrorMessage(res)));
		}
		auto status = PQresultStatus(res);
		if (status == PGRES_COPY_OUT) {
			// Handle COPY OUT: consume all copy data and wait for CommandComplete
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: ExecuteQueries - Handling PGRES_COPY_OUT, consuming copy data\n");
			}
			char *out_buffer;
			int len;
			idx_t copy_data_chunks = 0;
			while ((len = PQgetCopyData(GetConn(), &out_buffer, 0)) >= 0) {
				copy_data_chunks++;
				if (PostgresConnection::DebugPrintQueries()) {
					Printer::Print("DEBUG: ExecuteQueries - Consumed copy data chunk " + std::to_string(copy_data_chunks) + 
					               " (size: " + std::to_string(len) + " bytes)\n");
				}
				// Free the buffer data (we're just consuming it)
				if (out_buffer) {
					PQfreemem(out_buffer);
				}
			}
			if (len == -2) {
				// Error occurred during copy
				if (PostgresConnection::DebugPrintQueries()) {
					Printer::Print("DEBUG: ExecuteQueries - Error during copy data consumption\n");
				}
				throw std::runtime_error("Error during COPY OUT data consumption: " + string(PQerrorMessage(GetConn())));
			}
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: ExecuteQueries - Copy data ended after " + std::to_string(copy_data_chunks) + 
				               " chunks, consuming remaining results\n");
			}
			// len == -1 means end of copy data, now consume all remaining results
			idx_t remaining_results = 0;
			while (true) {
				auto copy_result = PQgetResult(GetConn());
				if (!copy_result) {
					break;
				}
				remaining_results++;
				PostgresResult pg_copy_result(copy_result);
				auto copy_status = PQresultStatus(copy_result);
				if (PostgresConnection::DebugPrintQueries()) {
					Printer::Print("DEBUG: ExecuteQueries - Consumed remaining result " + std::to_string(remaining_results) + 
					               " with status: " + std::to_string(copy_status) + "\n");
				}
				if (copy_status != PGRES_COMMAND_OK) {
					auto error_msg = string(PQresultErrorMessage(copy_result));
					if (PostgresConnection::DebugPrintQueries()) {
						Printer::Print("DEBUG: ExecuteQueries - Unexpected result status during copy completion: " + error_msg + "\n");
					}
					throw std::runtime_error("Failed to complete COPY operation: " + error_msg);
				}
			}
			if (PostgresConnection::DebugPrintQueries()) {
				Printer::Print("DEBUG: ExecuteQueries - COPY OUT operation completed successfully, consumed " + 
				               std::to_string(remaining_results) + " remaining results\n");
			}
			continue;
		}
		if (status != PGRES_TUPLES_OK) {
			continue;
		}
		results.push_back(std::move(result));
	}
	return results;
}

PostgresVersion PostgresConnection::GetPostgresVersion() {
	auto result = TryQuery("SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')");
	if (!result) {
		PostgresVersion version;
		version.type_v = PostgresInstanceType::UNKNOWN;
		return version;
	}
	auto version = PostgresUtils::ExtractPostgresVersion(result->GetString(0, 0));
	if (result->GetInt64(0, 1) > 0) {
		version.type_v = PostgresInstanceType::AURORA;
	}
	return version;
}

bool PostgresConnection::IsOpen() {
	return connection.get();
}

void PostgresConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void PostgresConnection::DebugSetPrintQueries(bool print) {
	debug_postgres_print_queries = print;
}

bool PostgresConnection::DebugPrintQueries() {
	return debug_postgres_print_queries;
}

} // namespace duckdb
