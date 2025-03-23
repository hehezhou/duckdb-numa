#include "duckdb/parallel/task_numa.hpp"
#include "duckdb/parallel/task_concurrency_queue.hpp"

namespace duckdb {

std::tuple<idx_t, idx_t> TaskNUMA::Register(ConcurrentQueue *queue) {
    schedule_queue = queue;
    if (rest_inputs != 0) {
        if (numa_id == 0) {
            return {48, 0};
        } else if (numa_id == 1) {
            return {0, 48};
        }
        abort();
    } else {
        return {0, 0};
    }
}

void TaskNUMA::AddInput(idx_t count) {
    rest_inputs.fetch_add(count, std::memory_order::memory_order_release);
    auto before = total_inputs.fetch_add(count, std::memory_order::memory_order_release);
    auto after = before + count;
    if (after / 48 != before / 48) {
        schedule_queue->semaphore[numa_id].signal(48);
        if (stealable) {
            schedule_queue->semaphore[numa_id ^ 1].signal(after / 48 - before / 48);
        }
    }
}

void TaskNUMA::FinishInput() {
    schedule_queue->semaphore[numa_id].signal(total_inputs % 48);
    if (stealable) {
        schedule_queue->semaphore[numa_id ^ 1].signal();
    }
}

void TaskNUMA::Finish() {
    bool expected = false;
    if (!finished.compare_exchange_strong(expected, true, std::memory_order::memory_order_release)) {
        return;
    }
    schedule_queue->current_task_numa[numa_id].store(nullptr, std::memory_order::memory_order_release);
    schedule_queue->TryFill(numa_id);
}

bool TaskNUMA::TryFetch() {
    auto current = rest_inputs.load(std::memory_order::memory_order_relaxed);
    do {
        if (current < LOCAL_COUNT) {
            return false;
        }
    } while (rest_inputs.compare_exchange_strong(current, current - LOCAL_COUNT, std::memory_order::memory_order_relaxed));
    return true;
}

bool TaskNUMA::TrySteal() {
    if (!stealable) {
        return false;
    }
    auto current = rest_inputs.load(std::memory_order::memory_order_relaxed);
    do {
        if (current < STEAL_COUNT) {
            return false;
        }
    } while (rest_inputs.compare_exchange_strong(current, current - STEAL_COUNT, std::memory_order::memory_order_relaxed));
    return true;
}

}