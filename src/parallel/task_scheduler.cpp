#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/parallel/task_concurrency_queue.hpp"
#include "duckdb/parallel/task_numa.hpp"

#include <thread>

#if defined(_WIN32)
#include <windows.h>
#elif defined(__GNUC__)
#include <sched.h>
#include <unistd.h>
#endif

namespace duckdb {

struct SchedulerThread {
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
};

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(db.config.options.allocator_background_threads), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(db.config.options.allocator_background_threads);
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	auto token = make_uniq<QueueProducerToken>(*queue);
	return make_uniq<ProducerToken>(*this, std::move(token));
}

void TaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, std::move(task));
}

void TaskScheduler::NUMAInit() {
	queue->stealable[0] = queue->stealable[1] = false;
}

void TaskScheduler::ScheduleTaskNUMA(ProducerToken &token, TaskNUMA *task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->EnqueueNUMA(token, task);
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

class HashJoinFinalizeTask;

void TaskScheduler::ExecuteForever(atomic<bool> *marker, idx_t cpu_id) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	shared_ptr<Task> task;
	TaskNUMA* task_numa;
	// loop until the marker is set to false
	while (*marker) {
		auto execute_type = queue->Dequeue(task, task_numa, cpu_id);
		if (execute_type == DequeueResult::NO_TASK) {
			continue;
		}
		// if (cpu_id == 3 || cpu_id == 4 || execute_type == DequeueResult::TASK_NUMA_STEAL) {
		// 	Printer::PrintF("Task Get %d %d %f", static_cast<int>(cpu_id), static_cast<int>(execute_type), GetNow() - numa_test_start);
		// }
		// if (execute_type == DequeueResult::TASK_NORMAL) {
		// 	Printer::PrintF("Task Get %d %s %d %f", static_cast<int>(cpu_id), typeid(*task).name(), static_cast<int>(execute_type), GetNow() - numa_test_start);
		// }
		if (execute_type == DequeueResult::TASK_NORMAL) {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			// if (typeid(*task) == typeid(PipelineTask) || strcmp(typeid(*task).name(), "N6duckdb20HashJoinFinalizeTaskE") == 0) {
			// 	auto &ptask = dynamic_cast<ExecutorTask&>(*task);
			// 	fprintf(stderr, "end %lld %d %f\n", reinterpret_cast<const uint64_t>(ptask.event.get()), cpu_id,  GetNow() - numa_test_start);
			// }

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
				task.reset();
				break;
			default:
				throw NotImplementedException("Disallowed in Research TaskScheduler::ExecuteForever");
			}
		} else if (execute_type == DequeueResult::TASK_NUMA_LOCAL) {
			task_numa->Execute(TaskNUMAExecutionMode::PROCESS_LOCAL, cpu_id);
		} else if (execute_type == DequeueResult::TASK_NUMA_STEAL) {
			task_numa->Execute(TaskNUMAExecutionMode::PROCESS_STEAL, cpu_id);
		} else {
			abort();
		}
		// if (cpu_id == 3 || cpu_id == 4 || execute_type == DequeueResult::TASK_NUMA_STEAL || execute_type == DequeueResult::TASK_NORMAL) {
		// 	Printer::PrintF("Task End %d %d %f", static_cast<int>(cpu_id), static_cast<int>(execute_type), GetNow() - numa_test_start);
		// }
	}
	// this thread will exit, flush all of its outstanding allocations
	if (Allocator::SupportsFlush()) {
		Allocator::ThreadFlush(allocator_background_threads, 0, NumericCast<idx_t>(requested_thread_count.load()));
		Allocator::ThreadIdle();
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void TaskScheduler::WorkOnTasks() {
	shared_ptr<Task> task;
	TaskNUMA* task_numa;
	while (true) {
		auto execute_type = queue->DequeueWithoutWait(task, task_numa, 0);
		if (execute_type == DequeueResult::NO_TASK) {
			return;
		}
		if (execute_type == DequeueResult::TASK_NORMAL) {
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
				task.reset();
				break;
			default:
				throw NotImplementedException("Disallowed in Research TaskScheduler::WorkOnTasks");
			}
		} else if (execute_type == DequeueResult::TASK_NUMA_LOCAL) {
			task_numa->Execute(TaskNUMAExecutionMode::PROCESS_LOCAL, 0);
		} else if (execute_type == DequeueResult::TASK_NUMA_STEAL) {
			task_numa->Execute(TaskNUMAExecutionMode::PROCESS_STEAL, 0);
		} else {
			abort();
		}
	}
}

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
	throw NotImplementedException("Disallowed in Research TaskScheduler::ExecuteTasks(marker, max_tasks)");
}

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
	throw NotImplementedException("Disallowed in Research TaskScheduler::ExecuteTasks(max_tasks)");
}

#ifndef DUCKDB_NO_THREADS
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker, int cpu_id) {
	cpu_set_t cpu_mask;
	CPU_ZERO(&cpu_mask);
	CPU_SET(cpu_id, &cpu_mask);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_mask);
	scheduler->ExecuteForever(marker, cpu_id);
}
#endif

int32_t TaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

void TaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
	if (total_threads == 0) {
		throw SyntaxException("Number of threads must be positive!");
	}
#ifndef DUCKDB_NO_THREADS
	if (total_threads < external_threads) {
		throw SyntaxException("Number of threads can't be smaller than number of external threads!");
	}
#else
	if (total_threads != external_threads) {
		throw NotImplementedException(
		    "DuckDB was compiled without threads! Setting total_threads != external_threads is not allowed.");
	}
#endif
	requested_thread_count = NumericCast<int32_t>(total_threads - external_threads);
}

void TaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void TaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void TaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	typedef std::make_signed<std::size_t>::type ssize_t;
	queue->SignAll(n);
#endif
}

void TaskScheduler::YieldThread() {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::yield();
#endif
}

idx_t TaskScheduler::GetEstimatedCPUId() {
#if defined(EMSCRIPTEN)
	// FIXME: Wasm + multithreads can likely be implemented as
	//   return return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	return 0;
#else
	// this code comes from jemalloc
#if defined(_WIN32)
	return (idx_t)GetCurrentProcessorNumber();
#elif defined(_GNU_SOURCE)
	auto cpu = sched_getcpu();
	if (cpu < 0) {
		// fallback to thread id
		return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	}
	return (idx_t)cpu;
#elif defined(__aarch64__) && defined(__APPLE__)
	/* Other oses most likely use tpidr_el0 instead */
	uintptr_t c;
	asm volatile("mrs %x0, tpidrro_el0" : "=r"(c)::"memory");
	return (idx_t)(c & (1 << 3) - 1);
#else
	// fallback to thread id
	return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#endif
#endif
}

void TaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n);
}

void TaskScheduler::RelaunchThreadsInternal(int32_t n) {
#ifndef DUCKDB_NO_THREADS

	cpu_set_t cpu_mask;
	CPU_ZERO(&cpu_mask);
	CPU_SET(0, &cpu_mask);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_mask);

	auto &config = DBConfig::GetConfig(db);
	auto new_thread_count = NumericCast<idx_t>(n);
	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
		return;
	}
	if (threads.size() > new_thread_count) {
		// we are reducing the number of threads: clear all threads first
		for (idx_t i = 0; i < threads.size(); i++) {
			*markers[i] = false;
		}
		Signal(threads.size());
		// now join the threads to ensure they are fully stopped before erasing them
		for (idx_t i = 0; i < threads.size(); i++) {
			threads[i]->internal_thread->join();
		}
		// erase the threads/markers
		threads.clear();
		markers.clear();
	}
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		idx_t create_new_threads = new_thread_count - threads.size();
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get(), threads.size() + 1);
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + config.options.external_threads);
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
#endif
}

} // namespace duckdb
