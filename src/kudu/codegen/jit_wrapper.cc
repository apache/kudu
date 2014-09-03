// Copyright 2014 Cloudera inc.

#include "kudu/codegen/jit_wrapper.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/faststring.h"

using llvm::ExecutionEngine;

namespace kudu {
namespace codegen {

JITWrapper::JITWrapper(gscoped_ptr<JITCodeOwner> owner)
  : owner_(owner.Pass()) {}

JITWrapper::~JITWrapper() {}

} // namespace codegen
} // namespace kudu
