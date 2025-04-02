#pragma once

#include "sys/time.h"

static double GetNow() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

extern int split_probe_rest;
extern int split_probe_rest_start;
const int thread_count = 96;
extern double numa_test_start;

static void InitParams() {
    split_probe_rest = split_probe_rest_start;
}
