#include "duckdb/optimizer/probe_side_pipeline_breaker_optimizer.hpp"

#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_pipeline_breaker.hpp"

#include "duckdb/common/numa_config.hpp"

namespace duckdb {

ProbeSidePipelineBreakerOptimizer::ProbeSidePipelineBreakerOptimizer() {
	probe_breaker_bitmask = split_probe_rest_start;
}

void ProbeSidePipelineBreakerOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		VisitOperator(*op.children[1]);
		if ((probe_breaker_bitmask >>= 1) & 1) {
			op.children[0] = make_uniq<LogicalPipelineBreaker>(std::move(op.children[0]));
		}
		VisitOperator(*op.children[0]);
		break;
	}
	default:
		VisitOperatorChildren(op);
		break;
	}
}

} // namespace duckdb
