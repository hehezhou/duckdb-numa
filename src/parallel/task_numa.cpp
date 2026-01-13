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
    schedule_queue.store(queue);
    queue->current_task_numa[numa_id].store(this);
    RegisterInternal();
}

void TaskNUMA::Finish() {
    // Printer::PrintF("FINISH %f", GetNow() - numa_test_start);
    bool expected = false;
    if (!finished.compare_exchange_strong(expected, true)) {
        return;
    }
    auto queue = schedule_queue.load();

    unique_lock<mutex> queue_lock(queue->latch);
    queue->wait_steal[numa_id].store(0);
    queue->current_task_numa[numa_id].store(nullptr);
    queue->TryFill(numa_id);
    queue_lock.unlock();

    event->FinishTask();
    executor.UnregisterTask();
}

}