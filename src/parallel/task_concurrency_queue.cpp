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
        task->Register(this);
        if (task->is_final_task) {
            if (!stealable[numa_id].exchange(task->is_final_task)) {
                semaphore[numa_id].signal(wait_steal[numa_id ^ 1].exchange(0));
            }
        }
        return true;
    }
    return false;
}

DequeueResult ConcurrentQueue::Dequeue(shared_ptr<Task> &task, TaskNUMA* &task_numa, idx_t cpu_id) {
    auto numa_id = cpu_id % 2;
    semaphore[numa_id].wait();
    if (q.try_dequeue(task)) {
        return DequeueResult::TASK_NORMAL;
    }
    if (cpu_id == 94 || cpu_id == 92) {
        return DequeueResult::NO_TASK;
    }
    task_numa = current_task_numa[numa_id].load();
    if (task_numa != nullptr && task_numa->TryLocal()) {
        return DequeueResult::TASK_NUMA_LOCAL;
    }
    task_numa = current_task_numa[numa_id ^ 1].load();
    if (stealable[numa_id].load() && task_numa != nullptr && task_numa->TrySteal()) {
        return DequeueResult::TASK_NUMA_STEAL;
    }
    return DequeueResult::NO_TASK;
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	return q.try_dequeue_from_producer(token.token->queue_token, task);
}

void ConcurrentQueue::SignAll(idx_t n) {
    semaphore[0].signal(static_cast<size_t>((n + 1) / 2));
    semaphore[1].signal(static_cast<size_t>((n + 1) / 2));
}

void ConcurrentQueue::AddSteal(idx_t numa_id, idx_t count) {
    if (!stealable[numa_id ^ 1].load()) {
        wait_steal[numa_id].fetch_add(count);
    }
    if (stealable[numa_id ^ 1].load()) {
        semaphore[numa_id ^ 1].signal(count);
    }
}

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {}

ProducerToken::~ProducerToken() {}

QueueProducerToken::QueueProducerToken(ConcurrentQueue &queue)
    : queue_token(queue.q) {}

}