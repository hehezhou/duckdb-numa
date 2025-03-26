#pragma once

#include "duckdb/parallel/task.hpp"
#include "duckdb/common/numa_config.hpp"

#include <atomic>

namespace duckdb {

class ConcurrentQueue;
class Event;

enum class TaskNUMAExecutionMode : uint8_t { PROCESS_LOCAL, PROCESS_STEAL };

class TaskNUMA : public enable_shared_from_this<TaskNUMA> {
public:
	const idx_t numa_id;
	const bool is_final_task;

public:
	TaskNUMA(shared_ptr<Event> event, idx_t numa_id, bool is_final_task)
		: event(std::move(event)), numa_id(numa_id), is_final_task(is_final_task) {}

	virtual ~TaskNUMA();

public:
	virtual TaskExecutionResult Execute(TaskNUMAExecutionMode mode, idx_t cpu_id) = 0;

	void Register(ConcurrentQueue *queue);

	virtual void RegisterInternal() = 0;

	void Finish();

protected:
	std::atomic<ConcurrentQueue*> schedule_queue{nullptr};

private:
	shared_ptr<Event> event;
	std::atomic<bool> finished{false};
};

}