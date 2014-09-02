// Copyright 2014 Cloudera inc.

#include "kudu/codegen/compilation_manager.h"

#include <cstdlib>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/codegen/code_cache.h"
#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/jit_schema_pair.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

DEFINE_bool(time_codegen, false, "Whether to print time that each code "
            "generation request took.");

METRIC_DEFINE_counter(code_cache_hits, kudu::MetricUnit::kCacheHits,
                      "Number of codegen cache hits since start");
METRIC_DEFINE_counter(code_cache_queries, kudu::MetricUnit::kCacheQueries,
                      "Number of codegen cache queries (hits + misses) "
                      "since start");
namespace kudu {
namespace codegen {

namespace {

// A CompilationTask is a ThreadPool's Runnable which, given a
// pair of schemas and a cache to refer to, will generate code pertaining
// to the two schemas and store it in the cache when run.
class CompilationTask : public Runnable {
 public:
  // Requires that the cache and generator are valid for the lifetime
  // of this object.
  CompilationTask(const Schema& base, const Schema& proj, CodeCache* cache,
                  CodeGenerator* generator)
    : base_(base),
      proj_(proj),
      cache_(cache),
      generator_(generator) {}

  // Can only be run once.
  virtual void Run() {
    // We need to fail softly because the user could have just given
    // a malformed projection schema pair, but could be long gone by
    // now so there's nowhere to return the status to.
    WARN_NOT_OK(RunWithStatus(),
                "Failed compilation of row projector from base schema " +
                base_.ToString() + " to projection schema " +
                proj_.ToString());
  }

 private:
  Status RunWithStatus() {
    faststring fs;
    RETURN_NOT_OK(JITSchemaPair::EncodeKey(base_, proj_, &fs));
    Slice key(fs.data(), fs.size());

    // Check again to make sure we didn't compile it already.
    // This can occur if we request the same schema pair while the
    // first one's compiling.
    if (cache_->Lookup(key)) return Status::OK();

    RowProjector::CodegenFunctions rp_functions;
    scoped_refptr<JITSchemaPair> owner;
    LOG_TIMING_IF(INFO, FLAGS_time_codegen, "code-generating schema pair") {
      RETURN_NOT_OK(generator_->CompileSchemaPair(base_, proj_, &owner));
    }

    cache_->AddEntry(key, owner);
    return Status::OK();
  }

  Schema base_;
  Schema proj_;
  CodeCache* const cache_;
  CodeGenerator* const generator_;

  DISALLOW_COPY_AND_ASSIGN(CompilationTask);
};

} // anonymous namespace

CompilationManager::CompilationManager()
  : cache_(kDefaultCacheCapacity) {
  CHECK_OK(ThreadPoolBuilder("compiler_manager_pool")
           .set_min_threads(0)
           .set_max_threads(1)
           .set_idle_timeout(MonoDelta::FromMilliseconds(kThreadTimeoutMs))
           .Build(&pool_));
  // We call std::atexit after the implicit default construction of
  // generator_ to ensure static LLVM constants would not have been destructed
  // when the registered function is called (since this object is a singleton,
  // atexit will only be called once).
  CHECK(std::atexit(&CompilationManager::Shutdown) == 0)
    << "Compilation manager shutdown must be registered successfully with "
    << "std::atexit to be used.";
}

CompilationManager::~CompilationManager() {}

void CompilationManager::Wait() {
  pool_->Wait();
}

void CompilationManager::Shutdown() {
  GetSingleton()->pool_->Shutdown();
}

void CompilationManager::RegisterMetrics(MetricRegistry* metric_registry) {
  lock_guard<rw_spinlock> lk(&metric_lock_);
  metric_context_.reset(new MetricContext(metric_registry,
                                          "compilation manager"));
  hit_counter_ = METRIC_code_cache_hits.Instantiate(*metric_context_);
  query_counter_ = METRIC_code_cache_queries.Instantiate(*metric_context_);
}


bool CompilationManager::RequestRowProjector(const Schema* base_schema,
                                             const Schema* projection,
                                             gscoped_ptr<RowProjector>* out) {
  faststring fs;
  Status s = JITSchemaPair::EncodeKey(*base_schema, *projection, &fs);
  WARN_NOT_OK(s, "RowProjector compilation request failed");
  if (!s.ok()) return false;
  Slice key(fs.data(), fs.size());

  scoped_refptr<JITSchemaPair> cached(
    down_cast<JITSchemaPair*>(cache_.Lookup(key).get()));

  // If not cached, add a request to compilation pool
  if (!cached) {
    UpdateCounts(false);
    shared_ptr<Runnable> task(
      new CompilationTask(*base_schema, *projection, &cache_, &generator_));
    WARN_NOT_OK(pool_->Submit(task),
                "RowProjector compilation request failed");
    return false;
  }

  UpdateCounts(true);
  const RowProjector::CodegenFunctions& functions =
    cached->row_projector_functions();
  out->reset(new RowProjector(base_schema, projection, functions, cached));
  return true;
}

void CompilationManager::UpdateCounts(bool hit) {
  shared_lock<rw_spinlock> lk(&metric_lock_);
  if (!metric_context_) return;
  if (hit) hit_counter_->Increment();
  query_counter_->Increment();
}

} // namespace codegen
} // namespace kudu
