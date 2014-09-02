// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_COMPILATION_MANAGER_H
#define KUDU_CODEGEN_COMPILATION_MANAGER_H

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/code_cache.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class MetricContext;
class MetricRegistry;
class ThreadPool;

namespace codegen {

class RowProjector;

// The compilation manager is a top-level class which manages the actual
// delivery of a code generator's output by maintaining its own
// threadpool and code cache. It accepts requests to compile various classes
// (all the ones that the CodeGenerator offers) and attempts to retrieve a
// cached copy. If no such copy exists, it adds a request to generate it.
//
// Class is thread safe.
//
// The compilation manager is available as a global singleton only because
// it is intended to be used on a per-tablet-server basis. While in
// certain unit tests (that don't depend on compilation performance),
// there may be multiple TSs per processes, this will not occur in a
// distributed enviornment, where each TS has its own process.
// Furthermore, using a singleton ensures that lower-level classes which
// depend on code generation need not depend on a top-level class which
// instantiates them to provide a compilation manager. This avoids many
// unnecessary dependencies on state and lifetime of top-level and
// intermediary classes which should not be aware of the code generation's
// use in the first place.
class CompilationManager {
 public:
  // Waits for all async tasks to finish.
  ~CompilationManager();

  static CompilationManager* GetSingleton() {
    return Singleton<CompilationManager>::get();
  }

  // If a codegenned row projector with compatible schemas (see
  // codegen::JITSchemaPair::ProjectionsCompatible) is ready,
  // then it is written to 'out' and true is returned.
  // Otherwise, this enqueues a compilation task for the parameter
  // schemas in the CompilationManager's thread pool and returns
  // false. Upon any failure, false is returned.
  // Does not write to 'out' if false is returned.
  bool RequestRowProjector(const Schema* base_schema,
                           const Schema* projection,
                           gscoped_ptr<RowProjector>* out);

  // Waits for all asynchronous compilation tasks to finish.
  void Wait();

  // Sets CompilationManager to register its metrics with the parameter
  // registry. If the CompilationManager already has metrics registered,
  // then the old metrics are abandoned (the context is deleted and the
  // old counters are no longer written to).
  void RegisterMetrics(MetricRegistry* metric_registry);

 private:
  friend class Singleton<CompilationManager>;
  CompilationManager();

  static void Shutdown();

  void UpdateCounts(bool hit);

  CodeGenerator generator_;
  CodeCache cache_;
  gscoped_ptr<ThreadPool> pool_;

  // Read-write lock protects the metric members
  rw_spinlock metric_lock_;
  gscoped_ptr<MetricContext> metric_context_;
  Counter* hit_counter_;
  Counter* query_counter_;

  static const int kDefaultCacheCapacity = 100;
  static const int kThreadTimeoutMs = 100;

  DISALLOW_COPY_AND_ASSIGN(CompilationManager);
};

} // namespace codegen
} // namespace kudu

#endif
