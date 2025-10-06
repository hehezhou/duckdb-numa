#include "duckdb/execution/operator/helper/physical_partitioner.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalPartitioner::PhysicalPartitioner(vector<LogicalType> types, unique_ptr<PhysicalOperator> child_operator,
                                         idx_t estimated_cardinality, unique_ptr<Expression> partition_key)
    : PhysicalOperator(PhysicalOperatorType::PARTITIONER, std::move(types), estimated_cardinality), numa_number(2),
      partition_key(std::move(partition_key)) {
	for (int i = 0; i < numa_number; i++) {
		chunk_queue.push_back(make_uniq<ConcurrentQueue>());
		total_items.push_back(0);
	}
	children.push_back(std::move(child_operator));
}

PhysicalPartitioner::~PhysicalPartitioner() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PartitionerSinkState : public LocalSinkState {
public:
	PartitionerSinkState(Allocator &allocator, const vector<LogicalType> &types)
	    :numa_number(2) {
			for (int i = 0; i < numa_number; i++) {
				numa_buffers.push_back(make_shared_ptr<ChunkBuffer>(allocator, types));
				added_chunks.push_back(0);
			}
	}

public:
	vector<shared_ptr<ChunkBuffer>> numa_buffers;
	vector<idx_t> added_chunks;
	int numa_number;
};

unique_ptr<LocalSinkState> PhysicalPartitioner::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PartitionerSinkState>(Allocator::DefaultAllocator(), types);
}

SinkResultType PhysicalPartitioner::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
{ 	auto &lstate = input.local_state.Cast<PartitionerSinkState>();

	// Ensure partition_key is a BoundReferenceExpression
	auto &bound_ref = partition_key->Cast<BoundReferenceExpression>();
	idx_t column_idx = bound_ref.index;

	// Extract the column to partition on
	auto &partition_column = chunk.data[column_idx];

	// use partitionedColumnData to partition chunks by column_idx into two chunks
	// if (partition_column.GetType().id() != LogicalTypeId::INTEGER) {
	// 	throw InvalidTypeException(partition_column.GetType(), "Partition key must be of INTEGER type");
	// }
	// we copy the chunk about x = numa_count times, and attach different selection vectors to each
	for (int i = 0; i < numa_number; i++) {
		// auto partitioned_column_data = partition_column.GetData();
		
		// auto partitioned_column_data = FlatVector::GetData<int64_t>(partition_column);
		UnifiedVectorFormat format;
		partition_column.ToUnifiedFormat(chunk.size(), format);
		SelectionVector sel;
		sel.Initialize(chunk.size());
		idx_t count = 0;

		// Partition the data based on the NUMA node
		for (idx_t j = 0; j < chunk.size(); j++) {
			idx_t child_idx = format.sel->get_index(j);
			auto data_ptr = (const int32_t *)format.data;
			int32_t value = data_ptr[child_idx];
			if (value % numa_number == i) {
				// std::cout<<"numa_id: "<<i<<" value: "<<partitioned_column_data[j]<<std::endl;
				sel.set_index(count, j);
				count++;
			}
		}

		// Create a new chunk for the partition
		DataChunk partitioned_chunk;
		partitioned_chunk.Initialize(Allocator::DefaultAllocator(), types);
		partitioned_chunk.SetCardinality(count);

		// Copy the data to the new chunk
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			partitioned_chunk.data[col_idx].Reference(chunk.data[col_idx]);
		}
		// set the selection vector
		partitioned_chunk.Slice(sel, count);


		// Enqueue the partitioned chunk
		lstate.numa_buffers[i]->Append(partitioned_chunk);
		while(lstate.numa_buffers[i]->ChunkCount()> lstate.added_chunks[i]+1) {
			ChunkReference tmp_chunk{lstate.numa_buffers[i], lstate.numa_buffers[i]->FetchChunkMeta(lstate.added_chunks[i])};
			// std::cout<<tmp_chunk.chunk_meta.count<<std::endl;
			total_items_lock.lock();
			total_items[i] += tmp_chunk.chunk_meta.count;
			total_items_lock.unlock();

			chunk_queue[i]->Enqueue(std::move(tmp_chunk));
			lstate.added_chunks[i]++;
		}

		// ChunkReference tmp_chunk{lstate.numa_buffers[i], lstate.numa_buffers[i]->FetchChunkMeta(lstate.numa_buffers[i]->ChunkCount() - 1)};
		// chunk_queue[i]->Enqueue(std::move(tmp_chunk));
		
	}

	return SinkResultType::NEED_MORE_INPUT;
}


SinkCombineResultType PhysicalPartitioner::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PartitionerSinkState>();
	for(int i = 0; i < lstate.numa_number; i++) {
		while (lstate.added_chunks[i] < lstate.numa_buffers[i]->ChunkCount()) {
			ChunkReference chunk_ref{lstate.numa_buffers[i], std::move(lstate.numa_buffers[i]->FetchChunkMeta(lstate.added_chunks[i]))};
			chunk_queue[i]->Enqueue(std::move(chunk_ref));
			lstate.added_chunks[i]++;
			total_items_lock.lock();
			// total_items[i].fetch_add(chunk_ref.chunk_meta.count);
			total_items[i] += chunk_ref.chunk_meta.count;
			total_items_lock.unlock();
			std::cout<<"Combine "<<chunk_ref.chunk_meta.count<<std::endl;
		}
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPartitioner::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	for(int i = 0; i < numa_number; i++) {
		chunk_queue[i]->Finalize();
		std::cout<<"numa_id: "<<i<<" total items: "<<total_items[i]<<std::endl;
		total_items[i] = 0;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PartitionerGlobalSource : public GlobalSourceState {
	idx_t MaxThreads() override {
		return 96;
	}
};

unique_ptr<GlobalSourceState> PhysicalPartitioner::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PartitionerGlobalSource>();
}

class PartitionerLocalSource : public LocalSourceState {
public:
	PartitionerLocalSource() {
		scan_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	}

public:
	ChunkManagementState scan_state;
};

unique_ptr<LocalSourceState> PhysicalPartitioner::GetLocalSourceState(ExecutionContext &context,
                                                                      GlobalSourceState &gstate) const {
	return make_uniq<PartitionerLocalSource>();
}

SourceResultType PhysicalPartitioner::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PartitionerLocalSource>();
	auto numa_id = input.numa_id;
	// std::cout<<"numa_id in GetData: "<<numa_id<<std::endl;
	ChunkReference chunk_ref;
	if (chunk_queue[numa_id]->TryDequeue(chunk_ref)) {
		chunk_ref.buffer->Scan(chunk_ref.chunk_meta, chunk, lstate.scan_state);
		total_items_lock.lock();
		total_items[numa_id] += chunk_ref.chunk_meta.count;
		// std::cout<<"numa_id: "<<numa_id <<" get data "<<total_items[numa_id]<<std::endl;
		total_items_lock.unlock();
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

// Build
void PhysicalPartitioner::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();

	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);

	// we create a new pipeline starting from the child
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.GetBasePipeline()->numa_id = 0;
	child_meta_pipeline.Build(*children[0]);
}

} // namespace duckdb