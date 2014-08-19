// Copyright 2014 Cloudera inc.

#include "kudu/codegen/compilation_manager.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

DEFINE_bool(time_codegen, false, "Whether to print time that each code "
            "generation request took.");

namespace kudu {
namespace codegen {

class RowProjector;

Status CompilationManager::RequestRowProjector(const Schema* base_schema,
                                               const Schema* projection,
                                               gscoped_ptr<RowProjector>* out) {
  LOG_TIMING_IF(INFO, FLAGS_time_codegen, "code-generating row projector") {
    return generator_.CompileRowProjector(base_schema, projection, out);
  }
}

} // namespace codegen
} // namespace kudu
