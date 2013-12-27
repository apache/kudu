# Copyright (c) 2013, Cloudera, inc.

cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})

# Find LLVM
find_package(LLVM REQUIRED COMPONENTS
  option
  analysis
  bitreader
  mcparser
  instrumentation
)
find_package(Clang REQUIRED COMPONENTS
    clang
    clangAnalysis
    clangAST
    clangASTMatchers
    clangBasic
    clangCodeGen
    clangDriver
    clangEdit
    clangFrontend
    clangFrontendTool
    clangLex
    clangParse
    clangSema
    clangSerialization
    clangTooling
)

add_definitions(${LLVM_DEFINITIONS})
include_directories(${CLANG_INCLUDE})
include_directories(${LLVM_INCLUDE_DIRS} ${CLANG_INCLUDE_DIRS})
link_directories(${LLVM_LIBRARY_DIRS})

set(START_GROUP "-Wl,--start-group")
set(END_GROUP "-Wl,--end-group")

add_executable(kudu-lint kudu-lint.cc)
target_link_libraries(kudu-lint
  ${LLVM_LDFLAGS}
  ${START_GROUP}
  ${CLANG_LIBS}
  ${LLVM_LIBS}
  ${END_GROUP}
  )

# Disable RTTI since we have to inherit from Clang-provided classes,
# and Clang does not enable RTTI.
set_target_properties(kudu-lint PROPERTIES
    COMPILE_FLAGS "${LLVM_CFLAGS} -fno-rtti")
