//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

//! A PipelineEvent is responsible for scheduling a pipeline
class PipelineEvent : public BasePipelineEvent {
public:
	explicit PipelineEvent(shared_ptr<Pipeline> pipeline);

public:
	void Schedule() override;
	void FinishEvent() override;

	void AddRuntimeDependency(PipelineEvent &event);

	void Start();

private:

	vector<weak_ptr<Event>> runtime_parents;

	std::atomic<bool> start{false};
};

} // namespace duckdb
