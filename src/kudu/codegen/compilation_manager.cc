// Copyright 2014 Cloudera inc.

#include "kudu/codegen/compilation_manager.h"

#include <cstdlib>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/codegen/code_cache.h"
#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

DEFINE_bool(time_codegen, false, "Whether to print time that each code "
            "generation request took.");

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
    // Check again to make sure we didn't compile it already.
    // This can occur if we request the same schema pair while the
    // first one's compiling.
    if (cache_->Lookup(base_, proj_)) return;

    RowProjector::CodegenFunctions rp_functions;
    scoped_refptr<JITCodeOwner> owner;
    Status s;
    LOG_TIMING_IF(INFO, FLAGS_time_codegen, "code-generating row projector") {
      s = generator_->CompileRowProjector(&base_, &proj_, &rp_functions, &owner);
    }

    if (s.ok()) {
      CodeCache::JITPayload* payload =
        new CodeCache::JITPayload(owner, rp_functions);
      s = cache_->AddEntry(base_, proj_, make_scoped_refptr(payload));
    }

    // We need to fail softly because the user could have just given
    // a malformed projection schema pair, but could be long gone by
    // now so there's nowhere to return the status to.
    if (!s.ok()) {
      LOG(WARNING) << "Failed compilation of row projector from base schema "
                   << base_.ToString() << " to projection schema "
                   << proj_.ToString() << ": " << s.ToString();
    }
  }

 private:
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

void CompilationManager::Shutdown() {
  GetSingleton()->pool_->Shutdown();
}

Status CompilationManager::RequestRowProjector(const Schema* base_schema,
                                               const Schema* projection,
                                               gscoped_ptr<RowProjector>* out) {
  scoped_refptr<CodeCache::JITPayload> cached =
    cache_.Lookup(*base_schema, *projection);

  // If not cached, add a request to compilation pool
  if (!cached) {
    shared_ptr<Runnable> task(
      new CompilationTask(*base_schema, *projection, &cache_, &generator_));
    RETURN_NOT_OK(pool_->Submit(task));
    return Status::NotFound("row projector not cached");
  }

  const RowProjector::CodegenFunctions& functions =
    cached->row_projector_functions();
  out->reset(new RowProjector(base_schema, projection, functions, cached));
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
