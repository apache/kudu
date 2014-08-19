// Copyright 2014 Cloudera inc.

#include "kudu/codegen/compilation_manager.h"

#include <glog/logging.h>

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/status.h"

namespace kudu {
namespace codegen {

class RowProjector;

Status CompilationManager::RequestRowProjector(const Schema* base_schema,
                                               const Schema* projection,
                                               gscoped_ptr<RowProjector>* out) {
  return generator_.CompileRowProjector(base_schema, projection, out);
}

} // namespace codegen
} // namespace kudu
