// Copyright 2014 Cloudera inc.

#include "kudu/codegen/jit_owner.h"

#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "kudu/gutil/gscoped_ptr.h"

using llvm::ExecutionEngine;

namespace kudu {
namespace codegen {

JITCodeOwner::JITCodeOwner(gscoped_ptr<ExecutionEngine> engine)
  : engine_(engine.Pass()) {}

JITCodeOwner::~JITCodeOwner() {}

} // namespace codegen
} // namesapce kudu
