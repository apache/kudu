// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_CODE_GENERATOR_H
#define KUDU_CODEGEN_CODE_GENERATOR_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace llvm {
class LLVMContext;
} // namespace llvm

namespace kudu {

class Schema;

namespace codegen {

class RowProjector;

// CodeGenerator is a top-level class that manages a per-module
// LLVM context, ExecutionEngine initialization, native target loading,
// and memory management.
//
// This generator is intended for JIT compilation of functions that
// are generated at runtime. These functions can make calls to pre-compiled
// C++ functions, which must be loaded from their *.ll files.
//
// Since the CodeGenerator is the owner of most of the LLVM compilation
// mechanisms (which in turn own most of the LLVM generated code), it also
// functions as a factory for the classes that use LLVM constructs dependent
// on the CodeGenerator's information.
//
// This class is NOT thread-safe.
//
// The execution engine has a global lock for compilations. When a function
// is compiling, other threads will be blocked on their own compilations or
// runs. Because of this, a CodeGenerator should be assigned to a single
// compilation thread (See CompilationManager class). Threads may run
// codegen'd functions concurrently.
//
// This code generator should survive longer than any of its compiled objects.
class CodeGenerator {
 public:
  CodeGenerator();
  ~CodeGenerator();

  // Generates a row projector with compiled code. Code is freed when
  // this object is destroyed. Writes to out parameter iff successful.
  Status CompileRowProjector(const Schema* base, const Schema* proj,
                             gscoped_ptr<RowProjector>* out);

 private:
  static void GlobalInit();

  // TODO static ObjectCache shared b/w engines

  DISALLOW_COPY_AND_ASSIGN(CodeGenerator);
};

} // namespace codegen
} // namespace kudu

#endif
