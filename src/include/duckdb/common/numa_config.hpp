#pragma once

#include "duckdb/common/typedefs.hpp"

#include "sys/time.h"

static int split_probe_rest;
static int split_probe_rest_start;
static double numa_test_start;
extern int swap_bitmask_start;
extern int swap_bitmask;

static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
static void InitParams() {
    split_probe_rest = split_probe_rest_start;
    swap_bitmask = swap_bitmask_start;
}

const int thread_count = 96;
#define ONE_SOCKET_ENABLED
extern int socket0_cpus;