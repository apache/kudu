// Copyright 2014 Cloudera inc.

#ifndef KUDU_CODEGEN_FUNCTION_BUILDER_H
#define KUDU_CODEGEN_FUNCTION_BUILDER_H

#include <string>
#include <vector>

#include "kudu/codegen/llvm_include.h"
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/status.h"

namespace llvm {

class ExecutionEngine;
class Function;
class LLVMContext;
class Module;
class Type;
class Value;

} // namespace llvm

namespace kudu {
namespace codegen {

// A ModuleBuilder provides an interface to generate code for procedures
// given a CodeGenerator to refer to. Builder can be used to create multiple
// functions. It is intended to make building functions easier than using
// LLVM's IRBuilder<> directly. Finally, a builder also provides an interface
// to precompiled functions and makes sure that the bytecode is linked to
// the working module.
//
// This class is not thread-safe. It is intended to be used by a single
// thread to build a set of functions.
//
// This class is just a helper for other classes within the codegen
// directory. It is intended to be used within *.cc files, and not to be
// included in outward-facing classes so other directories do not have a
// dependency on LLVM (this class is necessary because the templated
// IRBuilder<> cannot be forward-declared since it has default arguments).
// This class, however, can easily be forward-declared.
class ModuleBuilder {
 private:
  typedef void* FunctionAddress;

 public:
  // Provide alias so template arguments can be changed in one place
  typedef llvm::IRBuilder<> LLVMBuilder;

  // Creates a builder with the given context, does not take ownership.
  explicit ModuleBuilder(llvm::LLVMContext* context);

  // Does not delete own module, should be managed externally.
  ~ModuleBuilder();

  // Inits a new module with parsed precompiled data from the
  // precompiled.ll file.
  // TODO: with multiple *.ll files, each file should be loaded on demand
  Status Init();

  // Assorted function-building utility methods
  llvm::Function* GetFunction(const std::string& name);
  llvm::Value* GetPointerValue(void* ptr) const;

  // TODO function attributes

  // TODO function pass manager

  // TODO if the number of functions to be precompiled gets too large,
  // this class could use some mapping to manage multiple .ll files

  llvm::Module* module() { return module_; }
  LLVMBuilder* builder() { return &builder_; }

  // Once a function is complete, it may be offered to the module builder
  // along with the location of the function pointer to be written to
  // with the value of the JIT-compiled function pointer. Once the module
  // builder's Compile() method is called, these value are filled.
  // Requires that llvm::Function belong to this ModuleBuilder's module.
  template<class FuncPtr>
  void AddJITPromise(llvm::Function* llvm_f, FuncPtr* actual_f) {
    // The below cast is technically yields undefined behavior for
    // versions of the standard prior to C++0x. However, the llvm
    // interface forces us to use object-pointer to function-pointer
    // casting.
    AddJITPromise(llvm_f, reinterpret_cast<FunctionAddress*>(actual_f));
  }

  // Compiles all promised functions. Builder may not be used after
  // this method, only destructed. Writes the generated execution engine
  // for the module into the parameter pointer if no errors occur.
  Status Compile(gscoped_ptr<llvm::ExecutionEngine>* ee);

 private:
  // The different states a ModuleBuilder can be in.
  enum MBState {
    kUninitialized,
    kBuilding,
    kCompiled
  };
  // Basic POD which associates an llvm::Function to the location where its
  // function pointer should be written to after compilation.
  struct JITFuture {
    llvm::Function* llvm_f_;
    FunctionAddress* actual_f_;
  };

  void AddJITPromise(llvm::Function* llvm_f, FunctionAddress* actual_f);

  // Absolute path to .ll file
  // TODO this should not be source-code dependent but rather configured
  // at runtime.
  static const char* const kKuduIRFile;

  MBState state_;
  LLVMBuilder builder_;
  std::vector<JITFuture> futures_;
  llvm::LLVMContext* context_; // not owned
  llvm::Module* module_; // not owned

  DISALLOW_COPY_AND_ASSIGN(ModuleBuilder);
};

} // namespace codegen
} // namespace kudu

#endif
