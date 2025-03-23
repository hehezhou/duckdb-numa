#pragma once

#include "duckdb/parallel/task.hpp"

#include <atomic>

// NUMATODO: hardcode numbers

namespace duckdb {

class ConcurrentQueue;

enum class TaskNUMAExecutionMode : uint8_t { PROCESS_LOCAL, PROCESS_STEAL };

class TaskNUMA : public enable_shared_from_this<TaskNUMA> {
static const idx_t STEAL_COUNT = 48;
static const idx_t LOCAL_COUNT = 1;
public:
    const idx_t numa_id;
	const bool stealable;
	const bool is_final_task;
	const bool dynamic_source;

public:
	TaskNUMA(idx_t numa_id, bool stealable, bool is_final_task, bool dynamic_source, bool breaker_source)
		: numa_id(numa_id), stealable(stealable), is_final_task(is_final_task), dynamic_source(dynamic_source) {
        if (!breaker_source) {
            rest_inputs = UINT64_MAX;
            input_finished = true;
        } else {
            rest_inputs = total_inputs = 0;
        }
	}

	virtual ~TaskNUMA() {}

public:
	virtual TaskExecutionResult Execute(TaskExecutionMode mode, idx_t cpu_id) = 0;

    void Register(ConcurrentQueue *queue);

	void AddInput(idx_t count);

    void FinishInput();

	void Finish();

    bool TryFetch();

    bool TrySteal();

private:
	std::atomic<ConcurrentQueue*> schedule_queue{nullptr};

	std::atomic<idx_t> rest_inputs;
	std::atomic<idx_t> total_inputs;
	std::atomic<bool> finished{false};
	std::atomic<bool> input_finished{false};
};

}