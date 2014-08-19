// Copyright 2014 Cloudera inc.

// Requires that the location of the precompiled.ll file is defined
#ifndef KUDU_CODEGEN_MODULE_BUILDER_PRECOMPILED_LL
#error "KUDU_CODEGEN_MODULE_BUILDER_PRECOMPILED_LL should be defined to " \
  "the location of the LLVM IR file for kudu/codegen/precompiled.cc"
#endif

#include "kudu/codegen/module_builder.h"

#include <algorithm>
#include <cstdlib>
#include <ostream>
#include <sstream>
#include <string>

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_os_ostream.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

using llvm::CodeGenOpt::Level;
using llvm::ConstantExpr;
using llvm::ConstantInt;
using llvm::EngineBuilder;
using llvm::ExecutionEngine;
using llvm::Function;
using llvm::IntegerType;
using llvm::LLVMContext;
using llvm::Module;
using llvm::PointerType;
using llvm::raw_os_ostream;
using llvm::SMDiagnostic;
using llvm::Type;
using llvm::Value;
using std::ostream;
using std::string;
using std::stringstream;

namespace kudu {
namespace codegen {

namespace {

string ToString(const SMDiagnostic& err) {
  stringstream sstr;
  raw_os_ostream os(sstr);
  err.print("kudu/codegen", os);
  os.flush();
  return sstr.str();
}

string ToString(const Module& m) {
  stringstream sstr;
  raw_os_ostream os(sstr);
  os << m;
  return sstr.str();
}

// This method is needed for the implicit conversion from
// llvm::StringRef to std::string
string ToString(const Function* f) {
  return f->getName();
}

bool ModuleContains(const Module& m, const Function* fptr) {
  for (Module::const_iterator it = m.begin(); it != m.end(); ++it) {
    if (&*it == fptr) return true;
  }
  return false;
}

} // anonymous namespace

const char* const ModuleBuilder::kKuduIRFile =
  KUDU_CODEGEN_MODULE_BUILDER_PRECOMPILED_LL;

ModuleBuilder::ModuleBuilder(LLVMContext* context)
  : state_(kUninitialized), builder_(*context), context_(context) {}

ModuleBuilder::~ModuleBuilder() {}

Status ModuleBuilder::Init() {
  CHECK_EQ(state_, kUninitialized) << "Cannot Init() twice";
  // Parse IR file
  SMDiagnostic err;
  module_ = llvm::ParseIRFile(kKuduIRFile, err, *context_);
  if (!module_) {
    return Status::ConfigurationError("Could not parse IR file",
                                      ToString(err));
  }
  VLOG(3) << "Successfully parsed IR file at " << kKuduIRFile << ":\n"
          << ToString(*module_);

  // TODO: consider loading this module once and then just copying it
  // from memory. If this strategy is used it may be worth trying to
  // reduce the .ll file size.

  state_ = kBuilding;
  return Status::OK();
}

Function* ModuleBuilder::GetFunction(const std::string& name) {
  CHECK_EQ(state_, kBuilding);
  return CHECK_NOTNULL(module_->getFunction(name));
}

Value* ModuleBuilder::GetPointerValue(void* ptr) const {
  CHECK_EQ(state_, kBuilding);
  // No direct way of creating constant pointer values in LLVM, so
  // first a constant int has to be created and then casted to a pointer
  IntegerType* llvm_uintptr_t = Type::getIntNTy(*context_, 8 * sizeof(ptr));
  uintptr_t int_value = reinterpret_cast<uintptr_t>(ptr);
  ConstantInt* llvm_int_value = ConstantInt::get(llvm_uintptr_t,
                                                 int_value, false);
  Type* llvm_ptr_t = Type::getInt8PtrTy(*context_);
  return ConstantExpr::getIntToPtr(llvm_int_value, llvm_ptr_t);
}


void ModuleBuilder::AddJITPromise(llvm::Function* llvm_f,
                                  FunctionAddress* actual_f) {
  CHECK_EQ(state_, kBuilding);
  DCHECK(ModuleContains(*module_, llvm_f))
    << "Function " << ToString(llvm_f) << " does not belong to ModuleBuilder.";
  JITFuture fut;
  fut.llvm_f_ = llvm_f;
  fut.actual_f_ = actual_f;
  futures_.push_back(fut);
}

Status ModuleBuilder::Compile(gscoped_ptr<ExecutionEngine>* ee) {
  CHECK_EQ(state_, kBuilding);
  // Attempt to generate the engine
  string str;
#ifdef NDEBUG
  Level opt_level = llvm::CodeGenOpt::Aggressive;
#else
  Level opt_level = llvm::CodeGenOpt::None;
#endif
  gscoped_ptr<ExecutionEngine> local_engine(EngineBuilder(module_)
                                            .setErrorStr(&str)
                                            .setUseMCJIT(true)
                                            .setOptLevel(opt_level)
                                            .create());
  if (!local_engine) {
    return Status::ConfigurationError("Code generation for module failed. "
                                      "Could not start ExecutionEngine",
                                      str);
  }

  // Compile the module
  // TODO add a pass to internalize the linkage of all functions except the
  // ones that are JITPromised
  // TODO use various target machine options
  // TODO various engine builder options (opt level, etc)
  // TODO calling convention?
  // TODO whole-module optimizations
  local_engine->finalizeObject();

  // Satisfy the promises
  BOOST_FOREACH(JITFuture& fut, futures_) {
    *fut.actual_f_ = local_engine->getPointerToFunction(fut.llvm_f_);
    if (*fut.actual_f_ == NULL) {
      return Status::NotFound(
        "Code generation for module failed. Could not find function \""
        + ToString(fut.llvm_f_) + "\".");
    }
  }

  // Upon success write to the output parameter
  *ee = local_engine.Pass();
  state_ = kCompiled;
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
