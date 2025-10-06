#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"
#include "duckdb/execution/operator/helper/physical_partitioner.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/numa_config.hpp"

// NUMACONSTANT
int split_probe_rest;
int split_probe_rest_start = 4;

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : CachingPhysicalOperator(type, op.types, estimated_cardinality), join_type(join_type) {
}

bool PhysicalJoin::EmptyResultIfRHSIsEmpty() const {
	// empty RHS with INNER, RIGHT or SEMI join means empty result set
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalJoin::BuildJoinPipelines(Pipeline &current, MetaPipeline &meta_pipeline, PhysicalOperator &op,
                                      bool build_rhs) {
	op.op_state.reset();
	op.sink_state.reset();

	// 'current' is the probe pipeline: add this operator
	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, op);

	// save the last added pipeline to set up dependencies later (in case we need to add a child pipeline)
	vector<shared_ptr<Pipeline>> pipelines_so_far;
	meta_pipeline.GetPipelines(pipelines_so_far, false);
	auto &last_pipeline = *pipelines_so_far.back();

	auto &hash_join = op.Cast<PhysicalHashJoin>();
	auto &cond = hash_join.conditions[0];

	// === Build Side Partition ===
	if (build_rhs) {
		// 在build side插入partitioner
		// 构造partitioner，假设op.children[1]为build input

		auto *build_child = op.children[1].get();
		vector<LogicalType> build_types = build_child->types;
		idx_t build_card = build_child->estimated_cardinality;
		// 这里partition_key需要你根据实际情况传入
		unique_ptr<Expression> build_partition_key= cond.right->Copy();
		auto build_partitioner = make_uniq<PhysicalPartitioner>(build_types, std::move(op.children[1]), build_card, std::move(build_partition_key));
		// 用partitioner替换build input
		op.children[1] = std::move(build_partitioner);
		// 构建child pipeline
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, op, MetaPipelineType::JOIN_BUILD);
		child_meta_pipeline.Build(*op.children[1]);
	}

	// === Probe Side Partition === (TODO: if child is join and same join key, skip partitoner)
	{
		vector<LogicalType> probe_types = op.children[0]->types;
		idx_t probe_card = op.children[0]->estimated_cardinality;
		unique_ptr<Expression> probe_partition_key = cond.left->Copy();
		auto probe_partitioner = make_uniq<PhysicalPartitioner>(probe_types, std::move(op.children[0]), probe_card, std::move(probe_partition_key));
		op.children[0] = std::move(probe_partitioner);
	}

	// NUMATODO: config
	if (--split_probe_rest == 0) {
		auto breaker_types = op.children[0]->types;
		auto breaker_estimated_cardinality = op.children[0]->estimated_cardinality;
		auto breaker = make_uniq<PhysicalPipelineBreaker>(breaker_types, std::move(op.children[0]), breaker_estimated_cardinality);
		op.children[0] = std::move(breaker);
	}

	// continue building the current pipeline on the LHS (probe side)
	op.children[0]->BuildPipelines(current, meta_pipeline);

	switch (op.type) {
	case PhysicalOperatorType::POSITIONAL_JOIN:
		// Positional joins are always outer
		meta_pipeline.CreateChildPipeline(current, op, last_pipeline);
		return;
	case PhysicalOperatorType::CROSS_PRODUCT:
		return;
	default:
		break;
	}

	// Join can become a source operator if it's RIGHT/OUTER, or if the hash join goes out-of-core
	if (op.Cast<PhysicalJoin>().IsSource()) {
		meta_pipeline.CreateChildPipeline(current, op, last_pipeline);
	}
}

void PhysicalJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const_reference<PhysicalOperator>> PhysicalJoin::GetSources() const {
	auto result = children[0]->GetSources();
	if (IsSource()) {
		result.push_back(*this);
	}
	return result;
}

} // namespace duckdb
