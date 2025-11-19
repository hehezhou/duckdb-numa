#pragma once

#include "duckdb/common/typedefs.hpp"

#include "sys/time.h"

extern int current_join_id;
extern idx_t split_probe_bitmask;
extern double numa_test_start;
extern int next_numa_id;

static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static void InitParams() {
	current_join_id = 0;
	next_numa_id = 0;
    numa_test_start = GetNow();
}