// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/codegen/jit_owner.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include "kudu/gutil/gscoped_ptr.h"

using llvm::ExecutionEngine;

namespace kudu {
namespace codegen {

JITCodeOwner::JITCodeOwner(gscoped_ptr<ExecutionEngine> engine)
  : engine_(engine.Pass()) {}

JITCodeOwner::~JITCodeOwner() {}

} // namespace codegen
} // namespace kudu
