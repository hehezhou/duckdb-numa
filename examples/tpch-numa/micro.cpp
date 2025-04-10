#include "duckdb.hpp"
#include "ittnotify.h"

#include <fstream>
#include <iostream>
#include <sys/time.h>

using namespace duckdb;

int print_tag = 0;
int numa_tag = 0;
int parallel_build_tag = 0;
int split_probe_tag = 0;
int debug_tag = 0;
double query_start;
double prepare_payloads_end;
double init_pointer_table_end;
double build_end;
double first_probe_end;
double combine_end;
double probe_end;
double total_time;

int probe_type = 0;
std::atomic<double> first_probe_time;
std::atomic<double> second_probe_time;
std::atomic<double> first_probe_calc_time;
std::atomic<double> second_probe_calc_time;
std::atomic<double> pipeline_breaker_time;

int split_probe_rest;

std::atomic<int> current_build_id;

double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

double RunQuery(Connection &con, std::string query) {
	auto query_start = GetNow();
	split_probe_rest = split_probe_tag;
	con.Query(query);
	auto query_end = GetNow();
	return query_end - query_start;
}

std::string Query(int, int, int, float, float, int);

int main(int argc, char *argv[]) {
	std::string thread = argv[1];
	split_probe_tag = atoi(argv[2]);
	debug_tag = 1;

	int build_size = atoi(argv[3]);
	int build_size_2 = atoi(argv[4]);
	int probe_size = atoi(argv[5]);
	float sel1 = atof(argv[6]);
	float sel2 = atof(argv[7]);
	int payload_size = atoi(argv[8]);

	// need to change
	std::string db_file = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_numa_uncom.db";
	DuckDB db(db_file);
	Connection con(db);
	con.Query("SET threads TO " + thread + ";");
	con.Query("SET disabled_optimizers = 'join_order,build_side_probe_side';");

	for (int i = 0; i <= 5; i++) {
		if (i == 1) {
			__itt_resume();
		}
		auto time_usage = RunQuery(con, Query(build_size, build_size_2, probe_size, sel1, sel2, payload_size));
		if (i == 1) {
			__itt_pause();
		}
		if (i != 0) {
			std::cout << time_usage << "\n";
		}
	}
	abort();
}

std::string Query(int build_size, int build_size_2, int probe_size, float sel1, float sel2, int payload_size) {
	std::string build_side_table = "build_" + std::to_string(build_size);
	std::string build_side_table_2 = "build2_" + std::to_string(build_size_2);
	std::string probe_side_table = "probe_" + std::to_string(build_size) + "_" + std::to_string(build_size_2) + "_" +
	                               std::to_string(probe_size) + "_" + std::to_string(int(sel1 * 10000)) + "_" +
	                               std::to_string(int(sel2 * 10000)) + "_" + std::to_string(payload_size);
	std::string query = "Select payload_0 from " + probe_side_table + "," + build_side_table + "," +
	                    build_side_table_2 + " where " + build_side_table + ".build_key = " + probe_side_table +
	                    ".probe_key and " + build_side_table_2 + ".build_key = " + probe_side_table + ".probe_key;";

	// std::cout << query << std::endl;
	return query;
}