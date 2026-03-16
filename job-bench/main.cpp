#include "duckdb.hpp"

#include "duckdb/common/numa_config.hpp"

#include "ittnotify.h"

#include <fstream>
#include <iostream>
#include <sys/time.h>

using namespace duckdb;

double RunQuery(Connection &con, std::string query) {
	auto query_start = GetNow();
	auto result = con.Query(query);
	auto query_end = GetNow();
	return query_end - query_start;
}

std::string Query(std::string);

double run(Connection &con, const std::string &query_number) {
    double total_usage = 0;
	for (int i = -10; i <= 10; i++) {
		auto time_usage = RunQuery(con, Query(query_number));
		if (i > 0) {
            total_usage += time_usage;
		}
	}
    return total_usage / 10;
}

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	std::string query_number = argv[2];
	swap_bitmask = swap_bitmask_start = atoi(argv[3]);

	std::string db_file = "/PATH/TO/imdb.db";
	DuckDB db(db_file);
	Connection con(db);
	con.Query("SET threads TO " + thread + ";");
	//con.Query("set disabled_optimizers = \"join_filter_pushdown	\";");

    while (std::cin >> split_probe_rest_start) {
        std::cout << run(con, query_number) << std::endl;
    }
}

std::string Query(std::string number) {
	std::ifstream sqlfile("/PATH/TO/job/" + number + ".sql");
	return std::string((std::istreambuf_iterator<char>(sqlfile)), std::istreambuf_iterator<char>());
}
