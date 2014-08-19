// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_COMPILATION_MANAGER_H
#define KUDU_CODEGEN_COMPILATION_MANAGER_H

#include "kudu/codegen/code_generator.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/status.h"

namespace kudu {
namespace codegen {

class RowProjector;

// The compilation manager is a top-level class which manages the actual
// delivery of a code generator's output by maintaining its own
// threadpool and code cache (TODO: maintain a threadpool and code cache).
// It accepts requests to compile various classes (all the ones that the
// CodeGenerator offers) and attempts to retrieve a cached copy.
// If no such copy exists, it adds a request to generate it.
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
  static CompilationManager* GetSingleton() {
    return Singleton<CompilationManager>::get();
  }

  // Enqueues the task to be compile asyncronously with a callback
  // to call once compilation is complete. Upon success, out parameter
  // is written to with the row projector.
  // TODO: currently all requests are sychronous, offer async compilation
  Status RequestRowProjector(const Schema* base_schema,
                             const Schema* projection,
                             gscoped_ptr<RowProjector>* out);

 private:
  friend class Singleton<CompilationManager>;
  CompilationManager() {}

  CodeGenerator generator_;

  DISALLOW_COPY_AND_ASSIGN(CompilationManager);
};

} // namespace codegen
} // namespace kudu

#endif
