# Copyright 2014 Cloudera inc.
# Confidential Cloudera Information: Covered by NDA.

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
  jit_wrapper.cc
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
set(IR_OUTPUT ${BUILD_OUTPUT_ROOT_DIRECTORY}/precompiled.ll)
set(IR_OUTPUT_ELF ${IR_OUTPUT}.o)
# Retrieve all includes directories needed for precompilation
set(IR_INCLUDES
  -I ${CMAKE_SOURCE_DIR}/src
  -I ${GLOG_INCLUDE_DIR}
  -I ${GTEST_INCLUDE_DIR})

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
  -DIR_BUILD
  ${CMAKE_CXX_FLAGS}
  ${PREFIXED_IR_PP_DEFS}
  -Wno-c++11-extensions # OVERRIDE macro conflicts with c++11 override
  ${IR_INCLUDES})
separate_arguments(IR_FLAGS)

# Avoid enabling ASAN in the precompiled IR.
#
# This avoids an issue when the main code is compiled with a different version
# of LLVM than we are using for JIT, and ASAN is enabled. In that case,
# the IR code will try to call __asan_init_v<X> while our runtime code will
# only have defined __asan_init_v<Y> (where X != Y).
#
# Disabling -fsanitize-address will prevent the instrumentation, so it doesn't
# try to link against these symbols.
#
# NOTE: we leave "-DADDRESS_SANITIZER" because this enables ASAN annotations
# from dynamic_annotations.h. These annotations are just extern function
# declarations which will link fine against the ASAN in our executable, even
# if the JIT code is not instrumented.
list(REMOVE_ITEM IR_FLAGS "-fsanitize=address")

# We need a library which depends on the IR source, because CMake+Ninja
# doesn't support IMPLICIT_DEPENDS in ADD_CUSTOM_COMMAND.
#
# Using a fake target like this allows us to pick up the dependencies
# of precompiled.ll, and then we make the IR generation depend on the fake
# target. We end up doing one extra compilation, but that's better than
# having stale IR.
#
# See: http://www.cmake.org/Bug/bug_relationship_graph.php?bug_id=13234
add_library(ir_fake_target ${IR_SOURCE})
# The IR uses protobufs from kudu_common, so we have to generate that code first.
target_link_libraries(ir_fake_target kudu_common_proto)

add_custom_command(
  OUTPUT ${IR_OUTPUT}
  COMMAND ${CLANG_EXEC}
    ${IR_FLAGS}
    ${IR_SOURCE}
    -o ${IR_OUTPUT}
  DEPENDS ir_fake_target)

# Use 'ld' to create an ELF wrapper around the precompiled IR. This creates
# an elf file which exposes the following symbols:
#   extern char _binary_precompiled_ll_start;
#   extern char _binary_precompiled_ll_end;
# which point to the beginning and end of the IR data, respectively.
# See http://stackoverflow.com/questions/4158900/embedding-resources-in-executable-using-gcc
add_custom_command(
  OUTPUT ${IR_OUTPUT_ELF}
  COMMAND ${CMAKE_SOURCE_DIR}/build-support/embed-data-in-elf.sh
          ${IR_OUTPUT} ${IR_OUTPUT_ELF}
  DEPENDS ${IR_OUTPUT}
)

# Link the generated IR into an archive, and make the codegen library
# include it.
add_library(codegen_ir STATIC ${IR_OUTPUT_ELF})
target_link_libraries(codegen codegen_ir)

# We have to explicitly set the linkage language for the library since
# it doesn't have any source files. CMake needs this in order to know
# how to link it.
set_target_properties(codegen_ir PROPERTIES LINKER_LANGUAGE C)

#######################################
# Unit tests
#######################################

set(KUDU_TEST_LINK_LIBS codegen ${KUDU_MIN_TEST_LIBS})

# Can't be used with the exported client; it references hidden symbols.
if (NOT KUDU_EXPORTED_CLIENT)
  ADD_KUDU_TEST(codegen-test)
endif()
