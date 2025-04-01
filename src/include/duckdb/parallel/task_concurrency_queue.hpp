#pragma once

#include "duckdb/main/database.hpp"
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

#include <queue>

namespace duckdb {

class Task;
class TaskNUMA;
class TaskScheduler;

typedef duckdb_moodycamel::ConcurrentQueue<shared_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

enum DequeueResult {
	NO_TASK,
	TASK_NORMAL,
	TASK_NUMA_LOCAL,
	TASK_NUMA_STEAL,
};

struct ConcurrentQueue {
	concurrent_queue_t q;
	std::queue<TaskNUMA*> q_numa[2];
	lightweight_semaphore_t semaphore[2];
	std::atomic<TaskNUMA*> current_task_numa[2];
	std::atomic<idx_t> wait_steal[2];
	std::atomic<bool> stealable[2];

	mutex latch;

	ConcurrentQueue() {
		current_task_numa[0] = current_task_numa[1] = nullptr;
		wait_steal[0] = wait_steal[1] = 0;
		stealable[0] = stealable[1] = false;
	}

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueNUMA(ProducerToken &token, TaskNUMA* task);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	DequeueResult Dequeue(shared_ptr<Task> &task, TaskNUMA* &task_numa, idx_t cpu_id);
	//! Must be called when holding latch.
	bool TryFill(idx_t numa_id);
	void SignAll(idx_t n);
	void AddSteal(idx_t numa_id, idx_t count);
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue);

	duckdb_moodycamel::ProducerToken queue_token;
};

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

}