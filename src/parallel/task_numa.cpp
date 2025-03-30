#include "duckdb/parallel/task_numa.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/task_concurrency_queue.hpp"

namespace duckdb {

TaskNUMA::TaskNUMA(Executor &executor, shared_ptr<Event> event, idx_t numa_id, bool is_final_task)
	: event(std::move(event)), numa_id(numa_id), is_final_task(is_final_task), executor(executor) {
    executor.RegisterTask();
}

TaskNUMA::~TaskNUMA() {}

void TaskNUMA::Register(ConcurrentQueue *queue) {
    queue->current_task_numa[numa_id].store(this, std::memory_order_release);
    schedule_queue.store(queue, std::memory_order_release);
    RegisterInternal();
}

void TaskNUMA::Finish() {
    bool expected = false;
    if (!finished.compare_exchange_strong(expected, true, std::memory_order_release)) {
        return;
    }
    event->FinishTask();
    ConcurrentQueue *queue = schedule_queue.load(std::memory_order_relaxed);
    while (queue == nullptr) {
        asm volatile("rep; nop" ::: "memory");
        queue = schedule_queue.load(std::memory_order_relaxed);
    }
    lock_guard<mutex> queue_lock(queue->latch);
    queue->current_task_numa[numa_id].store(nullptr, std::memory_order_release);
    queue->TryFill(numa_id);
    executor.UnregisterTask();
}

}