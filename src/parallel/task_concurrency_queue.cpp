#include "duckdb/parallel/task_concurrency_queue.hpp"

#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/task_numa.hpp"

// NUMATODO: hardcode some thread count

namespace duckdb {

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	if (q.enqueue(token.token->queue_token, std::move(task))) {
		semaphore[0].signal();
		semaphore[1].signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

void ConcurrentQueue::EnqueueNUMA(ProducerToken &token, TaskNUMA* task) {
    lock_guard<mutex> queue_lock(latch);
    auto numa_id = task->numa_id;
    q_numa[numa_id].push(task);
    TryFill(numa_id);
}

bool ConcurrentQueue::TryFill(idx_t numa_id) {
    if (current_task_numa[numa_id] == nullptr && !q_numa[numa_id].empty()) {
        auto task = q_numa[numa_id].front();
        q_numa[numa_id].pop();
        auto [count_0, count_1] = task->Register(this);
        current_task_numa[numa_id].store(task, std::memory_order::memory_order_relaxed);
        semaphore[0].signal(count_0);
        semaphore[1].signal(count_1);
        return true;
    }
    return false;
}

DequeueResult ConcurrentQueue::Dequeue(shared_ptr<Task> &task, TaskNUMA* &task_numa, idx_t cpu_id) {
    auto numa_id = cpu_id % 2;
    semaphore[numa_id].wait();
    if (q.try_dequeue(task)) {
        return TASK_NORMAL;
    }
    task_numa = current_task_numa[numa_id].load(std::memory_order::memory_order_relaxed);
    if (task_numa != nullptr && task_numa->TryFetch()) {
        return TASK_NUMA_LOCAL;
    }
    return NO_TASK;
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.try_dequeue_from_producer(token.token->queue_token, task);
}

void ConcurrentQueue::SignAll(idx_t n) {
    semaphore[0].signal(static_cast<size_t>((n + 1) / 2));
    semaphore[1].signal(static_cast<size_t>((n + 1) / 2));
}

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {}

ProducerToken::~ProducerToken() {}

QueueProducerToken::QueueProducerToken(ConcurrentQueue &queue)
    : queue_token(queue.q) {}

}