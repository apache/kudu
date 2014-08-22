// Copyright 2014 Cloudera inc.

#include "kudu/codegen/compilation_manager.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

DEFINE_bool(time_codegen, false, "Whether to print time that each code "
            "generation request took.");

namespace kudu {
namespace codegen {

Status CompilationManager::RequestRowProjector(const Schema* base_schema,
                                               const Schema* projection,
                                               gscoped_ptr<RowProjector>* out) {
  RowProjector::CodegenFunctions functions;
  scoped_refptr<JITCodeOwner> owner;
  LOG_TIMING_IF(INFO, FLAGS_time_codegen, "code-generating row projector") {
    RETURN_NOT_OK(generator_.CompileRowProjector(base_schema, projection,
                                                 &functions, &owner));
  }

  out->reset(new RowProjector(base_schema, projection, functions, owner));
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
