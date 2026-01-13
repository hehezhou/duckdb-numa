#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/task_concurrency_queue.hpp"

namespace duckdb {

static idx_t StealCount(idx_t chunks) {
	if (chunks >= thread_count / 2 * LOCAL_AT_LEAST + STEAL_CHUNKS) {
		return MinValue<idx_t>(thread_count / 2, ((chunks - thread_count / 2 * LOCAL_AT_LEAST) / STEAL_CHUNKS));
	}
	return 0;
}

PipelineTask::PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p) {
}

bool PipelineTask::TaskBlockedOnResult() const {
	// If this returns true, it means the pipeline this task belongs to has a cached chunk
	// that was the result of the Sink method returning BLOCKED
	return pipeline_executor->RemainingSinkChunk();
}

const PipelineExecutor &PipelineTask::GetPipelineExecutor() const {
	return *pipeline_executor;
}

TaskExecutionResult PipelineTask::ExecuteTask(TaskExecutionMode mode) {
	if (!pipeline_executor) {
		pipeline_executor = make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
	}

	auto &pipeline_event = dynamic_cast<PipelineEvent&>(*this->event);
	pipeline_event.Start();

	pipeline_executor->SetTaskForInterrupts(shared_from_this());

	if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
		auto res = pipeline_executor->Execute(PARTIAL_CHUNK_COUNT);

		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			return TaskExecutionResult::TASK_NOT_FINISHED;
		case PipelineExecuteResult::INTERRUPTED:
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	} else {
		auto res = pipeline_executor->Execute();
		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			throw InternalException("Execute without limit should not return NOT_FINISHED");
		case PipelineExecuteResult::INTERRUPTED:
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	}

	event->FinishTask();
	pipeline_executor.reset();
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineTaskNUMA::PipelineTaskNUMA(Pipeline &pipeline_p, shared_ptr<Event> event_p, idx_t numa_id, bool is_final_task,
								   PhysicalPipelineBreaker *breaker_source_p)
	: TaskNUMA(pipeline_p.executor, std::move(event_p), numa_id, is_final_task), pipeline(pipeline_p) {
	Printer::PrintF("breaker source %d %f", numa_id, GetNow() - numa_test_start);
	rest_chunk = 0;
	input_finished = false;
	for (auto &i : executors) {
		i.store(nullptr);
	}
	breaker_source_p->pipeline_task.store(this);
	auto buffered_status = breaker_source_p->buffered_chunks->fetch_or(PhysicalPipelineBreaker::SET_TASK_TAG);
	if (buffered_status & PhysicalPipelineBreaker::INPUT_FINISH_TAG) {
		buffered_status -= PhysicalPipelineBreaker::INPUT_FINISH_TAG;
		FinishInput();
	}
	AddChunks(buffered_status);
}

PipelineTaskNUMA::PipelineTaskNUMA(Pipeline &pipeline_p, shared_ptr<Event> event_p, idx_t numa_id, bool is_final_task,
								   idx_t source_chunks)
	: TaskNUMA(pipeline_p.executor, std::move(event_p), numa_id, is_final_task), pipeline(pipeline_p) {
	Printer::PrintF("normal source %d %d %f", numa_id, source_chunks, GetNow() - numa_test_start);
	rest_chunk = MaxValue<idx_t>(source_chunks, 48);
	input_finished = true;
	for (auto &i : executors) {
		i.store(nullptr);
	}
}

void PipelineTaskNUMA::RegisterInternal() {
	auto queue = schedule_queue.load();
	auto chunks = rest_chunk.load();
	queue->semaphore[numa_id].signal(MinValue<idx_t>(thread_count / 2, chunks));
	queue->AddSteal(numa_id, StealCount(chunks));
}

bool PipelineTaskNUMA::TryLocal() {
	auto chunks = rest_chunk.load(std::memory_order_relaxed);
	do {
		if (chunks == 0) {
			return false;
		}
	} while (!rest_chunk.compare_exchange_weak(chunks, chunks - 1, std::memory_order_relaxed));
	return true;
}

bool PipelineTaskNUMA::TrySteal() {
	auto chunks = rest_chunk.load(std::memory_order_relaxed);
	do {
		if (chunks < thread_count / 2 * LOCAL_AT_LEAST + STEAL_CHUNKS) {
			return false;
		}
	} while (!rest_chunk.compare_exchange_weak(chunks, chunks - STEAL_CHUNKS, std::memory_order_relaxed));
	return true;
}

TaskExecutionResult PipelineTaskNUMA::Execute(TaskNUMAExecutionMode mode, idx_t cpu_id) {
	PipelineExecutor *executor_ptr = executors[cpu_id].exchange(nullptr);
	if (executor_ptr == nullptr) {
		auto active_tasks_expect = active_tasks.load();
		do {
			if (active_tasks_expect & PREPARE_FINISH) {
				return TaskExecutionResult::TASK_FINISHED;
			}
		} while (!active_tasks.compare_exchange_weak(active_tasks_expect, active_tasks_expect + 1));
		executor_ptr = new PipelineExecutor(pipeline.GetClientContext(), pipeline);
	}

	bool finish_tag = false;
	switch (mode) {
	case TaskNUMAExecutionMode::PROCESS_LOCAL: {
		do {
			auto result = executor_ptr->Execute(1);
			if (result == PipelineExecuteResult::FINISHED) {
				delete executor_ptr;
				finish_tag = true;
				break;
			}
		} while (TryLocal() || input_finished);
		break;
	}
	case TaskNUMAExecutionMode::PROCESS_STEAL: {
		auto result = executor_ptr->Execute(STEAL_CHUNKS);
		if (result == PipelineExecuteResult::FINISHED) {
			delete executor_ptr;
			finish_tag = true;
		} else if (result == PipelineExecuteResult::NOT_FINISHED) {
			if (StealCount(rest_chunk) != 0) {
				schedule_queue.load()->AddSteal(numa_id, 1);
			}
		} else {
			throw InternalException("Disallowed in Research PipelineTaskNUMA::Execute");
		}
		break;
	}
	default:
		throw InternalException("PipelineTaskNUMA: wrong numa execute mode");
	}

	if (!finish_tag) {
		executors[cpu_id].store(executor_ptr);
		if (active_tasks.load() & PREPARE_FINISH) {
			executor_ptr = executors[cpu_id].exchange(nullptr);
		} else {
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
	}

	if (finish_tag) {
		active_tasks.fetch_or(PREPARE_FINISH);
	}

	if (!finish_tag) {
		FinishExecutor(executor_ptr);
	} else {
		auto rest_tasks = active_tasks.fetch_sub(1) - 1;
		if (rest_tasks == PREPARE_FINISH) {
			Finish();
		}
	}
	while (true) {
		auto finish_ptr_local = finish_ptr.fetch_add(1);
		if (finish_ptr_local >= thread_count) {
			break;
		}
		executor_ptr = executors[finish_ptr_local].exchange(nullptr);
		FinishExecutor(executor_ptr);
	}
	return TaskExecutionResult::TASK_FINISHED;
}

void PipelineTaskNUMA::FinishExecutor(PipelineExecutor *executor) {
	if (executor == nullptr) {
		return;
	}
	auto result = executor->Execute();
	if (result != PipelineExecuteResult::FINISHED) {
		throw InternalException("Disabled in project");
	}
	delete executor;
	auto rest_tasks = active_tasks.fetch_sub(1) - 1;
	if (rest_tasks == PREPARE_FINISH) {
		Finish();
	}
}

void PipelineTaskNUMA::AddChunks(idx_t num_chunks) {
	auto chunks = rest_chunk.fetch_add(num_chunks) + num_chunks;
	auto queue = schedule_queue.load();
	if (queue != nullptr) {
		queue->semaphore[numa_id].signal(num_chunks);
		queue->AddSteal(numa_id, StealCount(chunks) - StealCount(chunks - num_chunks));
	}
}

void PipelineTaskNUMA::FinishInput() {
	input_finished.store(true);
	AddChunks(1);
}

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), ready(false), initialized(false), source(nullptr), sink(nullptr), numa_id(0) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(double &current_percentage, idx_t &source_cardinality) {
	D_ASSERT(source);
	source_cardinality = MinValue<idx_t>(source->estimated_cardinality, 1ULL << 48ULL);
	if (!initialized) {
		current_percentage = 0;
		return true;
	}
	auto &client = executor.context;
	current_percentage = source->GetProgress(client, *source_state);
	current_percentage = sink->GetSinkProgress(client, *sink->sink_state, current_percentage);
	return current_percentage >= 0;
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelineTask>(*this, event));
	event->SetTasks(std::move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
	// check if the sink, source and all intermediate operators support parallelism
	if (!sink->ParallelSink()) {
		return false;
	}
	if (!source->ParallelSource()) {
		return false;
	}
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (!op.ParallelOperator()) {
			return false;
		}
	}
	if (sink->RequiresBatchIndex()) {
		if (!source->SupportsBatchIndex()) {
			throw InternalException(
			    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it");
		}
	}
	auto max_threads = source_state->MaxThreads();
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	auto active_threads = NumericCast<idx_t>(scheduler.NumberOfThreads());
	if (sink && sink->sink_state) {
		max_threads = sink->sink_state->MaxThreads(max_threads);
	}
	return LaunchScanTasks(event, max_threads);
}

bool Pipeline::IsOrderDependent() const {
	auto &config = DBConfig::GetConfig(executor.context);
	if (source) {
		auto source_order = source->SourceOrder();
		if (source_order == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
		if (source_order == OrderPreservationType::NO_ORDER) {
			return false;
		}
	}
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (op.OperatorOrder() == OrderPreservationType::NO_ORDER) {
			return false;
		}
		if (op.OperatorOrder() == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
	}
	if (!config.options.preserve_insertion_order) {
		return false;
	}
	if (sink && sink->SinkOrderDependent()) {
		return true;
	}
	return false;
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(sink);
	Reset();
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread

	// vector<shared_ptr<Task>> tasks;
	// for (idx_t i = 0; i < max_threads; i++) {
	// 	tasks.push_back(make_uniq<PipelineTask>(*this, event));
	// }
	// event->SetTasks(std::move(tasks));
	auto breaker_source = dynamic_cast<PhysicalPipelineBreaker*>(source.get());
	if (breaker_source) {
		event->SetTaskNUMA(new PipelineTaskNUMA(*this, event, numa_id, true, breaker_source));
	} else {
		bool is_final_task = false;
		if (dynamic_cast<PhysicalPipelineBreaker*>(sink.get()) != nullptr) {
			is_final_task = true;
		}
		event->SetTaskNUMA(new PipelineTaskNUMA(*this, event, numa_id, is_final_task, max_threads));
	}
	return true;
}

void Pipeline::ResetSink() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			sink->sink_state = sink->GetGlobalSinkState(GetClientContext());
		}
	}
}

void Pipeline::PrepareFinalize() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			throw InternalException("Sink of pipeline does not have sink state");
		}
		sink->PrepareFinalize(GetClientContext(), *sink->sink_state);
	}
}

void Pipeline::Reset() {
	ResetSink();
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		lock_guard<mutex> guard(op.lock);
		if (!op.op_state) {
			op.op_state = op.GetGlobalOperatorState(GetClientContext());
		}
	}
	ResetSource(false);
	// we no longer reset source here because this function is no longer guaranteed to be called by the main thread
	// source reset needs to be called by the main thread because resetting a source may call into clients like R
	initialized = true;
}

void Pipeline::ResetSource(bool force) {
	if (source && !source->IsSource()) {
		throw InternalException("Source of pipeline does not have IsSource set");
	}
	if (force || !source_state) {
		source_state = source->GetGlobalSourceState(GetClientContext());
	}
}

void Pipeline::Ready() {
	if (ready) {
		return;
	}
	ready = true;
	std::reverse(operators.begin(), operators.end());
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

void Pipeline::AddRuntimeDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	runtime_dependencies.push_back(weak_ptr<Pipeline>(pipeline));
}

string Pipeline::ToString() const {
	TextTreeRenderer renderer;
	return renderer.ToString(*this);
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

void Pipeline::PrintDependencies() const {
	for (auto &dep : dependencies) {
		shared_ptr<Pipeline>(dep)->Print();
	}
}

vector<reference<PhysicalOperator>> Pipeline::GetOperators() {
	vector<reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

vector<const_reference<PhysicalOperator>> Pipeline::GetOperators() const {
	vector<const_reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

void Pipeline::ClearSource() {
	source_state.reset();
	batch_indexes.clear();
}

idx_t Pipeline::RegisterNewBatchIndex() {
	lock_guard<mutex> l(batch_lock);
	idx_t minimum = batch_indexes.empty() ? base_batch_index : *batch_indexes.begin();
	batch_indexes.insert(minimum);
	return minimum;
}

idx_t Pipeline::UpdateBatchIndex(idx_t old_index, idx_t new_index) {
	lock_guard<mutex> l(batch_lock);
	if (new_index < *batch_indexes.begin()) {
		throw InternalException("Processing batch index %llu, but previous min batch index was %llu", new_index,
		                        *batch_indexes.begin());
	}
	auto entry = batch_indexes.find(old_index);
	if (entry == batch_indexes.end()) {
		throw InternalException("Batch index %llu was not found in set of active batch indexes", old_index);
	}
	batch_indexes.erase(entry);
	batch_indexes.insert(new_index);
	return *batch_indexes.begin();
}
//===--------------------------------------------------------------------===//
// Pipeline Build State
//===--------------------------------------------------------------------===//
void PipelineBuildState::SetPipelineSource(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.source = &op;
}

void PipelineBuildState::SetPipelineSink(Pipeline &pipeline, optional_ptr<PhysicalOperator> op,
                                         idx_t sink_pipeline_count) {
	pipeline.sink = op;
	// set the base batch index of this pipeline based on how many other pipelines have this node as their sink
	pipeline.base_batch_index = BATCH_INCREMENT * sink_pipeline_count;
}

void PipelineBuildState::AddPipelineOperator(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.operators.push_back(op);
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSource(Pipeline &pipeline) {
	return pipeline.source;
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSink(Pipeline &pipeline) {
	return pipeline.sink;
}

void PipelineBuildState::SetPipelineOperators(Pipeline &pipeline, vector<reference<PhysicalOperator>> operators) {
	pipeline.operators = std::move(operators);
}

shared_ptr<Pipeline> PipelineBuildState::CreateChildPipeline(Executor &executor, Pipeline &pipeline,
                                                             PhysicalOperator &op) {
	return executor.CreateChildPipeline(pipeline, op);
}

vector<reference<PhysicalOperator>> PipelineBuildState::GetPipelineOperators(Pipeline &pipeline) {
	return pipeline.operators;
}

} // namespace duckdb
