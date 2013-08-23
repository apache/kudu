# Copyright (c) 2013, Cloudera, inc
#
# Kudu RPC generator support
#########
find_package( Protobuf REQUIRED )

#
# Generate the KRPC files for a given .proto file.
#
function(KRPC_GENERATE SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: KRPC_GENERATE() called without protobuf files")
    return()
  endif(NOT ARGN)

  set(options)
  set(one_value_args SOURCE_ROOT BINARY_ROOT)
  set(multi_value_args EXTRA_PROTO_PATHS PROTO_FILES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()
  set(${SRCS})
  set(${HDRS})

  set(EXTRA_PROTO_PATH_ARGS)
  foreach(PP ${ARG_EXTRA_PROTO_PATHS})
    set(EXTRA_PROTO_PATH_ARGS ${EXTRA_PROTO_PATH_ARGS} --proto_path ${PP})
  endforeach()

  if("${ARG_SOURCE_ROOT}" STREQUAL "")
    SET(ARG_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()
  GET_FILENAME_COMPONENT(ARG_SOURCE_ROOT ${ARG_SOURCE_ROOT} ABSOLUTE)

  if("${ARG_BINARY_ROOT}" STREQUAL "")
    SET(ARG_BINARY_ROOT "${CMAKE_CURRENT_BINARY_DIR}")
  endif()
  GET_FILENAME_COMPONENT(ARG_BINARY_ROOT ${ARG_BINARY_ROOT} ABSOLUTE)

  foreach(FIL ${ARG_PROTO_FILES})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    # Ensure that the protobuf file is within the source root.
    # This is a requirement of protoc.
    FILE(RELATIVE_PATH PROTO_REL_TO_ROOT "${ARG_SOURCE_ROOT}" "${ABS_FIL}")

    GET_FILENAME_COMPONENT(REL_DIR "${PROTO_REL_TO_ROOT}" PATH)

    if(NOT REL_DIR STREQUAL "")
      SET(REL_DIR "${REL_DIR}/")
    endif()

    set(PROTO_CC_OUT "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.pb.cc")
    set(PROTO_H_OUT "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.pb.h")
    set(SERVICE_CC "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.service.cc")
    set(SERVICE_H "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.service.h")
    set(PROXY_CC "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.proxy.cc")
    set(PROXY_H "${ARG_BINARY_ROOT}/${REL_DIR}${FIL_WE}.proxy.h")
    list(APPEND ${SRCS} "${PROTO_CC_OUT}" "${SERVICE_CC}" "${PROXY_CC}")
    list(APPEND ${HDRS} "${PROTO_H_OUT}" "${SERVICE_H}" "${PROXY_H}")

    if(NOT EXISTS ${PROTO_DST_ROOT}/${FIL_PT})
        file(MAKE_DIRECTORY ${PROTO_DST_ROOT}/${FIL_PT})
    endif()

    GET_TARGET_PROPERTY(KRPC_BIN protoc-gen-krpc LOCATION)
    add_custom_command(
      OUTPUT "${PROTO_CC_OUT}" "${PROTO_H_OUT}"
      COMMAND  ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --cpp_out ${ARG_BINARY_ROOT} --proto_path ${ARG_SOURCE_ROOT} ${EXTRA_PROTO_PATH_ARGS} ${ABS_FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM )
    add_custom_command(
      OUTPUT "${SERVICE_CC}" "${SERVICE_H}" "${PROXY_CC}" "${PROXY_H}"
      COMMAND  ${PROTOBUF_PROTOC_EXECUTABLE}
      ARGS --plugin=${KRPC_BIN} --krpc_out ${ARG_BINARY_ROOT} --proto_path ${ARG_SOURCE_ROOT} ${EXTRA_PROTO_PATH_ARGS} ${ABS_FIL}
      DEPENDS ${ABS_FIL} "${PROTO_H_OUT}" "${PROTO_CC_OUT}" "${KRPC_BIN}"
      COMMENT "Running KRPC protocol buffer compiler on ${FIL}"
      VERBATIM)
  endforeach()

  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()
