#include "duckdb/planner/operator/logical_pipeline_breaker.hpp"

namespace duckdb {

LogicalPipelineBreaker::LogicalPipelineBreaker(unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PIPELINE_BREAKER) {
	children.push_back(std::move(child));
}

void LogicalPipelineBreaker::ResolveTypes() {
	types = children[0]->types;
}

vector<ColumnBinding> LogicalPipelineBreaker::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

} // namespace duckdb
