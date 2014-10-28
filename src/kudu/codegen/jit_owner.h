// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CODEGEN_JIT_OWNER_H
#define KUDU_CODEGEN_JIT_OWNER_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"

namespace llvm {
class ExecutionEngine;
} // namespace llvm

namespace kudu {
namespace codegen {

// Wrapper around llvm::ExecutionEngine to prevent header dependencies,
// which would be necessary for the engine's destructor. This class allows
// for explicit lifetime management of jitted code.
class JITCodeOwner : public RefCountedThreadSafe<JITCodeOwner> {
 public:
  explicit JITCodeOwner(gscoped_ptr<llvm::ExecutionEngine> engine);

 protected:
  friend class RefCountedThreadSafe<JITCodeOwner>;
  virtual ~JITCodeOwner();

 private:
  gscoped_ptr<llvm::ExecutionEngine> engine_;

  DISALLOW_COPY_AND_ASSIGN(JITCodeOwner);
};

} // namespace codegen
} // namespace kudu

#endif
