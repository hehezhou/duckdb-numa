#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"

#include "duckdb/common/numa_config.hpp"

int next_numa_id;

namespace duckdb {

class ChunkBuffer {
public:
	ChunkBuffer(Allocator &allocator, const vector<LogicalType> &types) : collection(allocator, types) {
		collection.InitializeAppend(append_state);
		segment = collection.GetSegments()[0].get();
		column_ids.reserve(types.size());
		for (idx_t i = 0; i < types.size(); i++) {
			column_ids.push_back(i);
		}
	}

	void Append(DataChunk &chunk) {
		collection.Append(append_state, chunk);
	}

	void Scan(ChunkMetaData &chunk_meta, DataChunk &chunk, ChunkManagementState &lstate) {
		chunk.Reset();
		lstate.handles.clear();
		segment->ReadChunkFromChunkMeta(chunk_meta, lstate, chunk, column_ids);
	}

	ChunkMetaData& FetchChunkMeta(idx_t chunk_index) {
		return segment->chunk_data[chunk_index];
	}

	idx_t ChunkCount() {
		return segment->ChunkCount();
	}

private:
	ColumnDataCollection collection;
	ColumnDataAppendState append_state;
	ColumnDataCollectionSegment *segment;
	vector<column_t> column_ids;
};

void BreakerConcurrentQueue::Enqueue(ChunkReference &&chunk_ref) {
	if (q.enqueue(std::move(chunk_ref))) {
		semaphore.signal();
	} else {
		throw InternalException("Could not enqueue datachunk!");
	}
}

bool BreakerConcurrentQueue::TryDequeue(ChunkReference &chunk_ref) {
	semaphore.wait();
	return q.try_dequeue(chunk_ref);
}

void BreakerConcurrentQueue::Finalize() {
	semaphore.signal(96);
}

PhysicalPipelineBreaker::PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> child_operator,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality),
	  chunk_queue(make_uniq<BreakerConcurrentQueue>()) {
	children.push_back(std::move(child_operator));
}

PhysicalPipelineBreaker::~PhysicalPipelineBreaker() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerSinkState : public LocalSinkState {
public:
	PipelineBreakerSinkState(Allocator &allocator, const vector<LogicalType> &types)
	: buffer(make_shared_ptr<ChunkBuffer>(allocator, types)), added_chunk(0) {}
public:
	shared_ptr<ChunkBuffer> buffer;
	idx_t added_chunk;
};

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PipelineBreakerSinkState>(Allocator::DefaultAllocator(), types);
}

SinkResultType PhysicalPipelineBreaker::Sink(ExecutionContext &context, DataChunk &chunk,
											 OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerSinkState>();
	lstate.buffer->Append(chunk);
	while (lstate.added_chunk + 1 < lstate.buffer->ChunkCount()) {
		ChunkReference chunk_ref{lstate.buffer, std::move(lstate.buffer->FetchChunkMeta(lstate.added_chunk))};
		chunk_queue->Enqueue(std::move(chunk_ref));
		lstate.added_chunk++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerSinkState>();
	while (lstate.added_chunk < lstate.buffer->ChunkCount()) {
		ChunkReference chunk_ref{lstate.buffer, std::move(lstate.buffer->FetchChunkMeta(lstate.added_chunk))};
		chunk_queue->Enqueue(std::move(chunk_ref));
		lstate.added_chunk++;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPipelineBreaker::Finalize(Pipeline &pipeline, Event &event,
                                                   ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	chunk_queue->Finalize();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSource : public GlobalSourceState {
public:
	idx_t MaxThreads() override {
		return 96;
	}
	//! Filter expression built from dynamic_filters (join filter pushdown), applied when reading chunks
	unique_ptr<Expression> filter_expression;
};

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	auto gstate = make_uniq<PipelineBreakerGlobalSource>();
	if (dynamic_filters && dynamic_filters->HasFilters()) {
		auto table_filters = dynamic_filters->GetMergedFilters();
		vector<unique_ptr<Expression>> exprs;
		for (auto &entry : table_filters->filters) {
			// entry.first is the output position (set by optimizer when pushing to breaker)
			auto col_idx = entry.first;
			auto &filter = entry.second;
			auto col_expr = make_uniq<BoundReferenceExpression>(types[col_idx], col_idx);
			exprs.push_back(filter->ToExpression(*col_expr));
		}
		if (exprs.size() == 1) {
			gstate->filter_expression = std::move(exprs[0]);
		} else {
			auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &e : exprs) {
				conjunction->children.push_back(std::move(e));
			}
			gstate->filter_expression = std::move(conjunction);
		}
	}
	return std::move(gstate);
}

class PipelineBreakerLocalSource : public LocalSourceState {
public:
	PipelineBreakerLocalSource(ExecutionContext &context, optional_ptr<Expression> filter_expression) {
		scan_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		if (filter_expression) {
			filter_executor = make_uniq<ExpressionExecutor>(context.client, *filter_expression);
			sel.Initialize(STANDARD_VECTOR_SIZE);
		}
	}

public:
	ChunkManagementState scan_state;
	ChunkReference chunk_ref;
	unique_ptr<ExpressionExecutor> filter_executor;
	SelectionVector sel;
};

unique_ptr<LocalSourceState> PhysicalPipelineBreaker::GetLocalSourceState(ExecutionContext &context,
																		  GlobalSourceState &gstate) const {
	auto &g = gstate.Cast<PipelineBreakerGlobalSource>();
	return make_uniq<PipelineBreakerLocalSource>(context, g.filter_expression.get());
}

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSource>();
	auto &chunk_ref = lstate.chunk_ref;
	while (chunk_queue->TryDequeue(chunk_ref)) {
		chunk_ref.buffer->Scan(chunk_ref.chunk_meta, chunk, lstate.scan_state);
		if (lstate.filter_executor) {
			idx_t result_count = lstate.filter_executor->SelectExpression(chunk, lstate.sel);
			if (result_count == 0) {
				// all rows filtered out - try next chunk
				continue;
			}
			if (result_count < chunk.size()) {
				chunk.Slice(chunk, lstate.sel, result_count);
			}
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

// Build
void PhysicalPipelineBreaker::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();

	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	auto &child_meta_pipeline = meta_pipeline.CreateConcurrentChildMetaPipeline(current, *this);
	child_meta_pipeline.GetBasePipeline()->numa_id = ++next_numa_id;
	child_meta_pipeline.Build(*children[0]);
}

}  // namespace duckdb