#include "duckdb/parallel/task_numa.hpp"
#include "duckdb/parallel/task_concurrency_queue.hpp"

namespace duckdb {

void TaskNUMA::Register(ConcurrentQueue *queue) {
    queue->current_task_numa[numa_id].store(this, std::memory_order_release);
    schedule_queue.store(queue, std::memory_order_release);
    auto start_count = total_inputs.load(std::memory_order_acquire);
    queue->semaphore[numa_id].signal(std::min<idx_t>(start_count / LOCAL_COUNT + 1, 48));
    if (stealable) {
        queue->semaphore[numa_id].signal(std::min<idx_t>(start_count / STEAL_COUNT + 1, 48));
    }
}

void TaskNUMA::AddInput(idx_t count) {
    rest_inputs.fetch_add(count, std::memory_order_release);
    auto before = total_inputs.fetch_add(count, std::memory_order_release);
    auto after = before + count;
    if (after / STEAL_COUNT != before / STEAL_COUNT) {
        auto queue = schedule_queue.load(std::memory_order_acquire);
        if (queue != nullptr) {
            queue->semaphore[numa_id].signal(STEAL_COUNT / LOCAL_COUNT);
            if (stealable) {
                queue->semaphore[numa_id ^ 1].signal(after / STEAL_COUNT - before / STEAL_COUNT);
            }
        }
    }
}

void TaskNUMA::FinishInput() {
    input_finished.store(true, std::memory_order_release);

    ConcurrentQueue *queue = schedule_queue.load(std::memory_order_relaxed);

    if (queue != nullptr) {
        queue->semaphore[numa_id].signal(total_inputs % STEAL_COUNT);
        if (stealable) {
            queue->semaphore[numa_id ^ 1].signal();
        }
    }
}

void TaskNUMA::Finish() {
    bool expected = false;
    if (!finished.compare_exchange_strong(expected, true, std::memory_order_release)) {
        return;
    }
    ConcurrentQueue *queue = schedule_queue.load(std::memory_order_relaxed);
    while (queue == nullptr) {
        asm volatile("rep; nop" ::: "memory");
        queue = schedule_queue.load(std::memory_order_relaxed);
    }
    std::lock_guard queue_lock(queue->latch);
    queue->current_task_numa[numa_id].store(nullptr, std::memory_order_release);
    queue->TryFill(numa_id);
}

bool TaskNUMA::TryFetch() {
    auto current = rest_inputs.load(std::memory_order_relaxed);
    idx_t target;
    do {
        if (current == 0) {
            return false;
        }
        if (!input_finished.load(std::memory_order_release)) {
            if (current < LOCAL_COUNT) {
                return false;
            }
        }
        if (current >= LOCAL_COUNT) {
            target = current - LOCAL_COUNT;
        } else {
            target = 0;
        }
    } while (rest_inputs.compare_exchange_strong(current, current - LOCAL_COUNT, std::memory_order_relaxed));
    return true;
}

bool TaskNUMA::TrySteal() {
    if (!stealable) {
        return false;
    }
    auto current = rest_inputs.load(std::memory_order_relaxed);
    do {
        if (current < STEAL_COUNT) {
            return false;
        }
    } while (rest_inputs.compare_exchange_strong(current, current - STEAL_COUNT, std::memory_order_relaxed));
    return true;
}

}