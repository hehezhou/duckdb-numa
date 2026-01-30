//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pipeline_breaker.hpp
//
//! LogicalPipelineBreaker is a pass-through operator that marks where to break
//! pipelines for NUMA optimizations. It wraps the probe side of joins.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalPipelineBreaker represents a pipeline break point in the logical plan
class LogicalPipelineBreaker : public LogicalOperator {
	LogicalPipelineBreaker() : LogicalOperator(LogicalOperatorType::LOGICAL_PIPELINE_BREAKER) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PIPELINE_BREAKER;

public:
	explicit LogicalPipelineBreaker(unique_ptr<LogicalOperator> child);

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
