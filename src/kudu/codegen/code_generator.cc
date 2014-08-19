// Copyright 2014 Cloudera inc.

#include "kudu/codegen/code_generator.h"

#include <string>

#include <glog/logging.h>
#include "kudu/codegen/llvm_include.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/Support/TargetSelect.h>

#include "kudu/codegen/module_builder.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/once.h"
#include "kudu/util/status.h"


DEFINE_bool(codegen_dump_functions, false, "Whether to print the LLVM IR"
            " for generated functions");

using llvm::ExecutionEngine;
using llvm::LLVMContext;
using std::string;

namespace kudu {
namespace codegen {

namespace {
GoogleOnceType once = GOOGLE_ONCE_INIT;
} // anonymous namespace

void CodeGenerator::GlobalInit() {
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
}

CodeGenerator::CodeGenerator()
  : context_(new LLVMContext()) {
  GoogleOnceInit(&once, &CodeGenerator::GlobalInit);
}

CodeGenerator::~CodeGenerator() {}

Status CodeGenerator::CompileRowProjector(const Schema* base,
                                          const Schema* proj,
                                          gscoped_ptr<RowProjector>* out) {
  // Load new functions into module by creating the row projector
  ModuleBuilder mbuilder(context_.get());
  RETURN_NOT_OK(mbuilder.Init());
  gscoped_ptr<RowProjector> ret(new RowProjector(base, proj, &mbuilder));

  // Compile and get execution engine
  gscoped_ptr<ExecutionEngine> ee;
  RETURN_NOT_OK(mbuilder.Compile(&ee));

  // Offer engine ownership to the row projector so that generated code
  // lives exactly as long as row projector does.
  ret->TakeEngine(ee.Pass());

  // Write to output parameter upon success.
  *out = ret.Pass();
  return Status::OK();
}

} // namespace codegen
} // namespace kudu
