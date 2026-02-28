#pragma once

#include "duckdb/common/typedefs.hpp"

#include "sys/time.h"

namespace duckdb {
class Pipeline;
}
extern int split_probe_rest;
extern int split_probe_rest_start;
extern double numa_test_start;
extern int swap_bitmask_start;
extern int swap_bitmask;

[[maybe_unused]] static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

extern std::vector<std::pair<duckdb::Pipeline*, duckdb::Pipeline*>> equal_dependency_pairs;
extern std::vector<duckdb::Pipeline*> all_pipelines;

const int thread_count = 96;
extern double numa_test_start;

static constexpr const duckdb::idx_t STEAL_CHUNKS = 5;
static constexpr const duckdb::idx_t LOCAL_AT_LEAST = 15;
