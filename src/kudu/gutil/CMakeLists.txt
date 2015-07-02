# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(GUTIL_SRCS
  atomicops-internals-x86.cc
  bits.cc
  callback_internal.cc
  dynamic_annotations.c
  hash/city.cc
  hash/hash.cc
  hash/jenkins.cc
  int128.cc
  mathlimits.cc
  once.cc
  ref_counted.cc
  ref_counted_memory.cc
  spinlock.cc
  spinlock_internal.cc
  stringprintf.cc
  strings/ascii_ctype.cc
  strings/charset.cc
  strings/escaping.cc
  strings/human_readable.cc
  strings/join.cc
  strings/memutil.cc
  strings/numbers.cc
  strings/serialize.cc
  strings/split.cc
  strings/strcat.cc
  strings/stringpiece.cc
  strings/strip.cc
  strings/substitute.cc
  strings/util.cc
  strtoint.cc
  sysinfo.cc
  threading/thread_collision_warner.cc
  utf/rune.c
  walltime.cc)

set(GUTIL_LIBS
  glog
  protobuf
  rt) # clock_gettime() requires -lrt

ADD_EXPORTABLE_LIBRARY(gutil
  SRCS ${GUTIL_SRCS}
  DEPS ${GUTIL_LIBS}
  # Disable warnings which trigger a lot in the Google code:
  COMPILE_FLAGS "-funsigned-char -Wno-deprecated -Wno-char-subscripts")

add_kudu_test(strings/string_util-test)
