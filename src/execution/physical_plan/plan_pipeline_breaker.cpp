#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pipeline_breaker.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPipelineBreaker &op) {
	D_ASSERT(op.children.size() == 1);

	auto child = CreatePlan(*op.children[0]);
	auto breaker_types = child->types;
	auto breaker_estimated_cardinality = child->estimated_cardinality;
	return make_uniq<PhysicalPipelineBreaker>(std::move(breaker_types), std::move(child),
	                                          breaker_estimated_cardinality);
}

} // namespace duckdb
