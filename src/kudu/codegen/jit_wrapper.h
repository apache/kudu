// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_JIT_WRAPPER_H
#define KUDU_CODEGEN_JIT_WRAPPER_H

#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace llvm {
class ExecutionEngine;
} // namespace llvm

namespace kudu {

class faststring;

namespace codegen {

typedef llvm::ExecutionEngine JITCodeOwner;

// A JITWrapper is the combination of a jitted code and a pointer
// (or pointers) to function(s) within that jitted code. Holding
// a ref-counted pointer to a JITWrapper ensures the validity of
// the codegenned function. A JITWrapper owns its code uniquely.
//
// All independent units which should be codegenned should derive
// from this type and update the JITWrapperType enum below so that there is
// a consistent unique identifier among jitted cache keys (each class may
// have its own different key encodings after those bytes).
class JITWrapper : public RefCountedThreadSafe<JITWrapper> {
 public:
  enum JITWrapperType {
    ROW_PROJECTOR
  };

  // Returns the key encoding (for the code cache) for this upon success.
  // If two JITWrapper instances of the same type have the same key, then
  // their codegenned code should be functionally equivalent.
  // Appends key to 'out' upon success.
  // The key must be unique amongst all derived types of JITWrapper.
  // To do this, the type's enum value from JITWrapper::JITWrapperType
  // should be prefixed to out.
  virtual Status EncodeOwnKey(faststring* out) = 0;

 protected:
  explicit JITWrapper(gscoped_ptr<JITCodeOwner> owner);
  virtual ~JITWrapper();

 private:
  friend class RefCountedThreadSafe<JITWrapper>;

  gscoped_ptr<JITCodeOwner> owner_;

  DISALLOW_COPY_AND_ASSIGN(JITWrapper);
};

} // namespace codegen
} // namespace kudu

#endif
