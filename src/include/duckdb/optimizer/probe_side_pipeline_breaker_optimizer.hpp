//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/probe_side_pipeline_breaker_optimizer.hpp
//
//! Inserts pipeline breakers on the probe side of joins for NUMA optimizations.
//! Uses a bitmask (from split_probe_rest_start) to selectively add breakers
//! to specific joins in the plan.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class ProbeSidePipelineBreakerOptimizer : public LogicalOperatorVisitor {
public:
	ProbeSidePipelineBreakerOptimizer();

	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override {};

private:
	// Bitmask consumed as we traverse joins (matches BuildJoinPipelines order)
	int probe_breaker_bitmask;
};

} // namespace duckdb
