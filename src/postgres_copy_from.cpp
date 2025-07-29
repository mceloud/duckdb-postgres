#include "postgres_connection.hpp"
#include "postgres_binary_reader.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyFrom(const string &query, ExecStatusType expected_result) {
	// Add debug logging to see exactly what command is being executed
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("DEBUG: Executing COPY command: " + query + "\n");
	}
	
	PostgresResult pg_res(PQExecute(query.c_str()));
	auto result = pg_res.res;
	auto actual_status = result ? PQresultStatus(result) : PGRES_FATAL_ERROR;
	
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print("DEBUG: COPY command result status: " + std::to_string(actual_status) + 
		               " (expected: " + std::to_string(expected_result) + ")\n");
		if (result) {
			Printer::Print("DEBUG: Number of tuples returned: " + std::to_string(PQntuples(result)) + "\n");
			Printer::Print("DEBUG: Number of fields: " + std::to_string(PQnfields(result)) + "\n");
			Printer::Print("DEBUG: Command status: " + string(PQcmdStatus(result)) + "\n");
		}
	}
	
	if (!result || actual_status != expected_result) {
		// Provide more detailed error information
		string error_details = result ? string(PQresultErrorMessage(result)) : "No result returned from PostgreSQL";
		string status_info = "Status: " + std::to_string(actual_status) + " (expected: " + std::to_string(expected_result) + ")";
		
		// Additional debugging for the TUPLES_OK case
		if (actual_status == PGRES_TUPLES_OK) {
			status_info += " - PostgreSQL treated COPY as regular SELECT query. Check: 1) COPY privileges 2) PostgreSQL version 3) Syntax";
		}
		
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + error_details + " | " + status_info);
	}
}

} // namespace duckdb
