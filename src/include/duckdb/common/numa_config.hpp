#pragma once

#include "duckdb/common/typedefs.hpp"

#include "sys/time.h"

extern int split_probe_rest;
extern int split_probe_rest_start;
extern double numa_test_start;

static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static void InitParams() {
    split_probe_rest = split_probe_rest_start;
    numa_test_start = GetNow();
}