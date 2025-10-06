//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_partitioner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "lightweightsemaphore.h"
#include <mutex>
#include <iostream>

namespace duckdb {

class ChunkBuffer;
class ChunkReference;
class ConcurrentQueue;

//! PhysicalPartitioner represents a physical operator that is used to break up pipelines
class PhysicalPartitioner : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PARTITIONER;
	int numa_number;
	unique_ptr<Expression> partition_key;
	mutable vector<int> total_items;
	mutable std::mutex total_items_lock;

public:
	PhysicalPartitioner(vector<LogicalType> types, unique_ptr<PhysicalOperator> source, idx_t estimated_cardinality,
	                    unique_ptr<Expression> partition_key);

	~PhysicalPartitioner() override;

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

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	vector<unique_ptr<ConcurrentQueue>> chunk_queue;
};
} // namespace duckdb