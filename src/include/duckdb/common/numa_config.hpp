#pragma once

#include "duckdb/common/typedefs.hpp"

#include "sys/time.h"

static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static int split_probe_rest;
static int split_probe_rest_start;
const int thread_count = 96;
static double numa_test_start;

static constexpr const duckdb::idx_t STEAL_CHUNKS = 5;
static constexpr const duckdb::idx_t LOCAL_AT_LEAST = 10;

static void InitParams() {
    split_probe_rest = split_probe_rest_start;
}

#define ONE_SOCKET_ENABLED
extern int socket0_cpus;