// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/codegen/compilation_manager.h"

#include <cstdlib>
#include <functional>
#include <memory>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/codegen/code_cache.h"
#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/jit_wrapper.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

using std::make_shared;
using std::shared_ptr;
using std::unique_ptr;

DEFINE_bool(codegen_time_compilation, false, "Whether to print time that each code "
            "generation request took.");
TAG_FLAG(codegen_time_compilation, experimental);
TAG_FLAG(codegen_time_compilation, runtime);

DEFINE_int32(codegen_cache_capacity, 100, "Number of entries which may be stored in the "
             "code generation cache.");
TAG_FLAG(codegen_cache_capacity, experimental);

DEFINE_int32(codegen_queue_capacity, 100, "Number of tasks which may be put in the code "
             "generation task queue.");
TAG_FLAG(codegen_queue_capacity, experimental);

METRIC_DEFINE_gauge_int64(server, code_cache_hits, "Codegen Cache Hits",
                          kudu::MetricUnit::kCacheHits,
                          "Number of codegen cache hits since start",
                          kudu::MetricLevel::kDebug,
                          kudu::EXPOSE_AS_COUNTER);
METRIC_DEFINE_gauge_int64(server, code_cache_queries, "Codegen Cache Queries",
                          kudu::MetricUnit::kCacheQueries,
                          "Number of codegen cache queries (hits + misses) "
                          "since start",
                          kudu::MetricLevel::kDebug,
                          kudu::EXPOSE_AS_COUNTER);
namespace kudu {
namespace codegen {

namespace {

// A CompilationTask is a task which, given a pair of schemas and a cache to
// refer to, will generate code pertaining to the two schemas and store it in
// the cache when run.
class CompilationTask {
 public:
  // Requires that the cache and generator are valid for the lifetime
  // of this object.
  CompilationTask(const Schema& base, const Schema& proj, CodeCache* cache,
                  CodeGenerator* generator)
    : base_(new Schema(base)),
      proj_(new Schema(proj)),
      cache_(cache),
      generator_(generator) {}

  // Can only be run once.
  void Run() {
    // We need to fail softly because the user could have just given
    // a malformed projection schema pair, but could be long gone by
    // now so there's nowhere to return the status to.
    WARN_NOT_OK(RunWithStatus(),
                "Failed compilation of row projector from base schema " +
                base_->ToString() + " to projection schema " +
                proj_->ToString());
  }

 private:
  Status RunWithStatus() {
    faststring key;
    RETURN_NOT_OK(RowProjectorFunctions::EncodeKey(*base_.get(), *proj_.get(), &key));

    // Check again to make sure we didn't compile it already.
    // This can occur if we request the same schema pair while the
    // first one's compiling.
    if (cache_->Lookup(key)) return Status::OK();

    scoped_refptr<RowProjectorFunctions> functions;
    LOG_TIMING_IF(INFO, FLAGS_codegen_time_compilation, "code-generating row projector") {
      RETURN_NOT_OK(generator_->CompileRowProjector(*base_.get(), *proj_.get(), &functions));
    }

    RETURN_NOT_OK(cache_->AddEntry(functions));
    return Status::OK();
  }

  SchemaPtr base_;
  SchemaPtr proj_;
  CodeCache* const cache_;
  CodeGenerator* const generator_;

  DISALLOW_COPY_AND_ASSIGN(CompilationTask);
};

} // anonymous namespace

CompilationManager::CompilationManager()
  : cache_(FLAGS_codegen_cache_capacity),
    hit_counter_(0),
    query_counter_(0) {
  CHECK_OK(ThreadPoolBuilder("compiler_manager_pool")
           .set_min_threads(0)
           .set_max_threads(1)
           .set_max_queue_size(FLAGS_codegen_queue_capacity)
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

Status CompilationManager::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity) {
  // Even though these function as counters, we use gauges instead, because
  // this is a singleton that is shared across multiple TS instances in a
  // minicluster setup. If we were to use counters, then we could not properly
  // register the same metric in multiple registries. Using a gauge which loads
  // an atomic int is a suitable workaround: each TS's registry ends up with a
  // unique gauge which reads the value of the singleton's integer.
  metric_entity->NeverRetire(
      METRIC_code_cache_hits.InstantiateFunctionGauge(
          metric_entity, [this]() { return this->hit_counter_.Load(kMemOrderNoBarrier); }));
  metric_entity->NeverRetire(
      METRIC_code_cache_queries.InstantiateFunctionGauge(
          metric_entity, [this]() { return this->query_counter_.Load(kMemOrderNoBarrier); }));
  return Status::OK();
}

bool CompilationManager::RequestRowProjector(const Schema* base_schema,
                                             const Schema* projection,
                                             unique_ptr<RowProjector>* out) {
  faststring key;
  Status s = RowProjectorFunctions::EncodeKey(*base_schema, *projection, &key);
  WARN_NOT_OK(s, "RowProjector compilation request encode key failed");
  if (!s.ok()) return false;
  query_counter_.Increment();

  scoped_refptr<RowProjectorFunctions> cached(
    down_cast<RowProjectorFunctions*>(cache_.Lookup(key).get()));

  // If not cached, add a request to compilation pool
  if (!cached) {
    shared_ptr<CompilationTask> task(make_shared<CompilationTask>(
        *base_schema, *projection, &cache_, &generator_));
    WARN_NOT_OK_EVERY_N_SECS(pool_->Submit([task]() { task->Run(); }),
                    "RowProjector compilation request submit failed", 10);
    return false;
  }

  hit_counter_.Increment();

  out->reset(new RowProjector(base_schema, projection, cached));
  return true;
}

} // namespace codegen
} // namespace kudu
