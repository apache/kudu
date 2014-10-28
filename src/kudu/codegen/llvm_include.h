// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

// Include this file before any LLVM dependencies are added.
// Defines certain preprocessor macros which resolve conflicts
// between LLVM's includes and ours.

#ifndef KUDU_CODEGEN_LLVM_INCLUDE_H
#define KUDU_CODEGEN_LLVM_INCLUDE_H

// LLVM's file llvm/Support/Valgrind.h conflicts
// with our kudu/gutil/dynamic_annotations.h. This define
// activates LLVM's header guard.
#define LLVM_SUPPORT_VALGRIND_H

// If LLVM was built with clang, then it assumes certain compiler runtime
// libraries area available to it when they are not under a gcc build.
// We need to manually change some #defines in case the compiler is not clang.
// We retrieve the definitions once in llvm-config.h, then activate the header
// guard. We undefine the problematic macros.
//
// This addresses the following bug:
// http://llvm.org/bugs/show_bug.cgi?id=18566
// NOTE: this should be fixed in LLVM 3.5.
#ifndef __clang__
#include <llvm/Config/llvm-config.h>
#undef HAVE_SANITIZER_MSAN_INTERFACE_H
#define CONFIG_H
#endif

#endif
