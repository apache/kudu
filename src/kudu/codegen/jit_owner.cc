// Copyright 2014 Cloudera inc.

#include "kudu/codegen/jit_owner.h"

#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "kudu/gutil/gscoped_ptr.h"

using llvm::ExecutionEngine;

namespace kudu {
namespace codegen {

JITCodeOwner::JITCodeOwner() {}

JITCodeOwner::JITCodeOwner(gscoped_ptr<ExecutionEngine> engine)
  : engine_(engine.Pass()) {}

JITCodeOwner::~JITCodeOwner() {}

void JITCodeOwner::Reset(JITCodeOwner* other) {
  engine_ = other->engine_.Pass();
}

} // namespace codegen
} // namespace kudu
