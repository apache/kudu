# Copyright 2014 Cloudera inc.

################################################################################
# This target relies on special LLVM-configured cmake functions
# see http://llvm.org/docs/CMake.html#embedding-llvm-in-your-project
################################################################################

#######################################
# codegen
#######################################

add_library(codegen
  code_cache.cc
  code_generator.cc
  compilation_manager.cc
  jit_owner.cc
  jit_schema_pair.cc
  module_builder.cc
  row_projector.cc
)

#######################################
# Configure LLVM-specific dependencies
#######################################

set(LLVM_REQ_COMPONENTS
  analysis
  irreader
  instrumentation
  ipo
  mcdisassembler
  mcjit
  native
)

## Add preprocessor defs and include directories
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

## Get the required libraries to link to in llvm
# as of llvm 3.5, there are two different functions for this
if (${LLVM_PACKAGE_VERSION} VERSION_LESS 3.5)
  llvm_map_components_to_libraries(llvm_LIBRARIES "${LLVM_REQ_COMPONENTS}")
else()
  llvm_map_components_to_libnames(llvm_LIBRARIES "${LLVM_REQ_COMPONENETS}")
endif()
target_link_libraries(codegen
  ${llvm_LIBRARIES}
  kudu_common
  kudu_util
  gutil)

## Disabling codegen in the KUDU_EXPORTED_CLIENT build.
# In the exported build, symbol visibility is hidden. Codegen relies on
# visible symbols for runtime relocation. This #define allows us to compile
# outward-facing interfaces for code generation which return error statuses
# instead of crashing.
if(KUDU_EXPORTED_CLIENT)
  add_definitions(-DKUDU_DISABLE_CODEGEN)
endif()

#######################################
# Precompiling to LLVM bytecode
#######################################

## Create .ll file for precompiled functions (and their dependencies)
set(CLANG_EXEC ${THIRDPARTY_PREFIX}/bin/clang++)
set(IR_SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/precompiled.cc)
set(IR_OUTPUT ${BUILD_OUTPUT_ROOT_DIRECTORY}precompiled.ll)
# Retrieve all includes directories needed for precompilation
set(IR_INCLUDES
  -I ${CMAKE_SOURCE_DIR}/src
  -I ${GLOG_INCLUDE_DIR})
# Get preprocessing definitions, which enable directives for glog and gtest
get_directory_property(IR_PP_DEFINITIONS
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMPILE_DEFINITIONS)
foreach(noprefix ${IR_PP_DEFINITIONS})
    set(PREFIXED_IR_PP_DEFS ${PREFIXED_IR_PP_DEFS} -D${noprefix})
endforeach()
# Get flags related to actually compiling the source
set(IR_FLAGS
  -S -emit-llvm
  ${CMAKE_CXX_FLAGS}
  ${PREFIXED_IR_PP_DEFS}
  -Wno-c++11-extensions # OVERRIDE macro conflicts with c++11 override
  ${IR_INCLUDES})
separate_arguments(IR_FLAGS)
# Scrub ASAN from precompiled functions
# The runtime linker for MCJIT fails to find asan functions referenced by
# codegenned code that's generated with a module precompiled with the below
# flags.
list(REMOVE_ITEM IR_FLAGS "-fsanitize=address" "-DADDRESS_SANITIZER")

add_custom_command(
  OUTPUT ${IR_OUTPUT}
  COMMAND ${CLANG_EXEC}
    ${IR_FLAGS}
    ${IR_SOURCE}
    -o ${IR_OUTPUT}
  # We require an explicit dependency on kudu_common_proto because
  # the IR source file relies on certain headers from kudu/common,
  # which in turn indirectly rely on kudu/common/common.pb.h.
  # IMPLICIT_DEPENDS performs a static file scan and thus isn't able
  # to resolve common.pb.h against the appropriate protoc-based cmake
  # target, a step that is necessary to dynamically generate the header file.
  DEPENDS ${IR_SOURCE} kudu_common_proto
  IMPLICIT_DEPENDS ${IR_SOURCE})
add_custom_target(create-ir DEPENDS ${IR_OUTPUT})
add_dependencies(codegen create-ir)

## Define the location of the .ll file
# TODO: may have to reconsider how to make the ModuleBuilder aware of where
# the precompiled file is located once installation details are fleshed out.
# Perhaps use a command line flag?
add_definitions(-DKUDU_CODEGEN_MODULE_BUILDER_PRECOMPILED_LL="${IR_OUTPUT}")

#######################################
# Unit tests
#######################################

set(KUDU_TEST_LINK_LIBS codegen ${KUDU_MIN_TEST_LIBS})

# Can't be used with the exported client; it references hidden symbols.
if (NOT KUDU_EXPORTED_CLIENT)
  ADD_KUDU_TEST(codegen-test)
endif()
