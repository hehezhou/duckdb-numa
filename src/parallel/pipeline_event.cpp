#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/common/printer.hpp"

#include "duckdb/common/numa_config.hpp"

namespace duckdb {

PipelineEvent::PipelineEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(std::move(pipeline_p)) {
}

void PipelineEvent::Schedule() {
	auto event = shared_from_this();
	auto &executor = pipeline->executor;
	try {
		pipeline->Schedule(event);
		D_ASSERT(total_tasks > 0);
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception in Finalize!"));
	} // LCOV_EXCL_STOP
}

void PipelineEvent::FinishEvent() {
	Printer::PrintF("task end %d %f", reinterpret_cast<const uint64_t>(this), GetNow() - numa_test_start);
}

void PipelineEvent::AddRuntimeDependency(PipelineEvent &event) {
	total_dependencies++;
	event.runtime_parents.push_back(weak_ptr<Event>(shared_from_this()));
}

void PipelineEvent::Start() {
	auto started = start.load();
	if (!started && start.compare_exchange_strong(started, true)) {
		for (auto &parent_entry : runtime_parents) {
			auto parent = parent_entry.lock();
			if (!parent) { // LCOV_EXCL_START
				continue;
			} // LCOV_EXCL_STOP
			// mark a dependency as completed for each of the parents
			parent->CompleteDependency();
		}
	}
}

} // namespace duckdb
