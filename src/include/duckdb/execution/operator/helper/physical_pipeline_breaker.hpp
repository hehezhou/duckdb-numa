//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "duckdb/execution/physical_operator.hpp"

#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include "duckdb/common/types/column/column_data_collection_segment.hpp"

namespace duckdb {

class ChunkBuffer;
class PipelineTaskNUMA;

struct BreakerChunkReference {
	shared_ptr<ChunkBuffer> buffer;
	ChunkMetaData chunk_meta;
};

typedef duckdb_moodycamel::ConcurrentQueue<BreakerChunkReference> concurrent_chunk_queue_t;

struct ConcurrentChunkQueue {
public:
	void Enqueue(BreakerChunkReference &&chunk_ref);
	bool TryDequeue(BreakerChunkReference &chunk_ref);

private:
	concurrent_chunk_queue_t q;
};

//! PhysicalPipelineBreaker represents a physical operator that is used to break up pipelines
class PhysicalPipelineBreaker : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PIPELINE_BREAKER;
	static constexpr const idx_t SET_TASK_TAG = static_cast<idx_t>(1) << 63;
	static constexpr const idx_t INPUT_FINISH_TAG = static_cast<idx_t>(1) << 62;

public:
	PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> join, idx_t estimated_cardinality);

	~PhysicalPipelineBreaker() override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	atomic<PipelineTaskNUMA*> pipeline_task;

	unique_ptr<atomic<idx_t>> buffered_chunks;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	unique_ptr<ConcurrentChunkQueue> chunk_queue;
};
}  // namespace duckdb