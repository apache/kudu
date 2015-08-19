# - Find VMEM (libvmem.h, libvmem.so)
# This module defines
#  VMEM_INCLUDE_DIR, directory containing headers
#  VMEM LIBS, directory containing vmem libraries
#  VMEM_STATIC_LIB, path to libvmem.a
#  VMEM_SHARED_LIB, path to libvmem.so shared library
#  VMEM_FOUND, whether libvmem has been found


set(VMEM_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)
set(VMEM_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

find_path(VMEM_INCLUDE_DIR libvmem.h PATHS
  ${VMEM_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(VMEM_LIB_PATH NAMES vmem PATHS ${VMEM_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (VMEM_INCLUDE_DIR AND VMEM_LIB_PATH)
  set(VMEM_FOUND TRUE)
  set(VMEM_LIB_NAME libvmem)
  set(VMEM_LIBS ${VMEM_SEARCH_LIB_PATH})
  set(VMEM_STATIC_LIB ${VMEM_SEARCH_LIB_PATH}/${VMEM_LIB_NAME}.a)
  set(VMEM_SHARED_LIB ${VMEM_LIBS}/${VMEM_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(VMEM_FOUND FALSE)
endif ()

if (VMEM_FOUND)
  if (NOT VMEM_FIND_QUIETLY)
    message(STATUS "Found the vmem library: ${VMEM_LIB_PATH}")
  endif ()
else ()
  if (NOT VMEM_FIND_QUIETLY)
    set(VMEM_ERR_MSG "Could not find the vmem library. Looked for headers")
    set(VMEM_ERR_MSG "${VMEM_ERR_MSG} in ${VMEM_SEARCH_HEADER_PATHS}, and for libs")
    set(VMEM_ERR_MSG "${VMEM_ERR_MSG} in ${VMEM_SEARCH_LIB_PATH}")
    if (Vmem_FIND_REQUIRED)
      message(FATAL_ERROR "${VMEM_ERR_MSG}")
    else (Vmem_FIND_REQUIRED)
      message(STATUS "${VMEM_ERR_MSG}")
    endif (Vmem_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  VMEM_INCLUDE_DIR
  VMEM_LIBS
  VMEM_STATIC_LIB
  VMEM_SHARED_LIB
)
