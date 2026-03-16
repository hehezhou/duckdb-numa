#include "duckdb.hpp"

#include "duckdb/common/numa_config.hpp"

#include "ittnotify.h"

#include <fstream>
#include <iostream>
#include <filesystem>
#include <sys/time.h>

#include <unistd.h>

using namespace duckdb;

double RunQuery(Connection &con, std::string query) {
	auto query_start = GetNow();
	auto result = con.Query(query);
	auto query_end = GetNow();
	return query_end - query_start;
}

std::string Query(std::string);

int main(int argc, char *argv[]) {
	std::string query_number = argv[1];
	swap_bitmask = swap_bitmask_start = atoi(argv[2]);

	std::string db_file = "/PATH/TO/imdb.db";
	DuckDB db(db_file);
	Connection con(db);
	con.Query("SET threads TO 48;");
	con.Query("set enable_profiling='json';");
	// con.Query("set disabled_optimizers = \"join_filter_pushdown	\";");

    con.Query("set profiling_output = './prof.json'");
	for (int i = 0; i < 15; i++) {
		RunQuery(con, Query(query_number));
	}
}

std::string Query(std::string number) {
	std::ifstream sqlfile("/PATH/TO/job/" + number + ".sql");
	return std::string((std::istreambuf_iterator<char>(sqlfile)), std::istreambuf_iterator<char>());
}