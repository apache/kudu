// Copyright 2014 Cloudera inc.

#include "kudu/codegen/code_generator.h"

#include <string>

#include <glog/logging.h>
#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/TargetSelect.h>

#include "kudu/codegen/jit_owner.h"
#include "kudu/codegen/module_builder.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/once.h"
#include "kudu/util/status.h"


DEFINE_bool(codegen_dump_functions, false, "Whether to print the LLVM IR"
            " for generated functions");

using llvm::ExecutionEngine;
using llvm::LLVMContext;
using llvm::Module;
using std::string;

namespace kudu {
namespace codegen {

namespace {

// Returns Status::OK() if codegen is not disabled and an error status indicating
// that codegen has been disabled otherwise.
Status CheckCodegenEnabled() {
#ifdef KUDU_DISABLE_CODEGEN
  return Status::NotSupported("Code generation has been disabled at compile time.");
#else
  return Status::OK();
#endif
}

} // anonymous namespace

void CodeGenerator::GlobalInit() {
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
}

CodeGenerator::CodeGenerator() {
  static GoogleOnceType once = GOOGLE_ONCE_INIT;
  GoogleOnceInit(&once, &CodeGenerator::GlobalInit);
}

CodeGenerator::~CodeGenerator() {}

Status CodeGenerator::CompileRowProjector(const Schema* base,
                                          const Schema* proj,
                                          RowProjector::CodegenFunctions* projector_out,
                                          scoped_refptr<JITCodeOwner>* owner_out) {
  RETURN_NOT_OK(CheckCodegenEnabled());

  // Generate a module builder
  ModuleBuilder mbuilder;
  RETURN_NOT_OK(mbuilder.Init());

  // Load new functions into module.
  RETURN_NOT_OK(RowProjector::CodegenFunctions::Create(*base, *proj, &mbuilder,
                                                       projector_out));

  // Compile and get execution engine
  gscoped_ptr<ExecutionEngine> ee;
  RETURN_NOT_OK(mbuilder.Compile(&ee));

  owner_out->reset(new JITCodeOwner(ee.Pass()));
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
