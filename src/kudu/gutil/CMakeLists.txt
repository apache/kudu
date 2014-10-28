# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

add_library(gutil
bits.cc
callback_internal.cc
dynamic_annotations.c
ref_counted.cc
stringprintf.cc
strings/memutil.cc
strings/charset.cc
strings/util.cc
strings/human_readable.cc
strings/join.cc
strings/numbers.cc
strings/split.cc
strings/strip.cc
strings/ascii_ctype.cc
strings/escaping.cc
strings/stringpiece.cc
strings/substitute.cc
strings/strcat.cc
strings/serialize.cc
threading/thread_collision_warner.cc
mathlimits.cc
int128.cc
strtoint.cc
once.cc
atomicops-internals-x86.cc
spinlock.cc
spinlock_internal.cc
sysinfo.cc
walltime.cc
hash/hash.cc
hash/jenkins.cc
hash/city.cc
utf/rune.c
)

target_link_libraries(gutil
  glog
  protobuf
  rt) # clock_gettime() requires -lrt

# Disable warnings which trigger a lot in the Google code:
set_target_properties(gutil PROPERTIES
  COMPILE_FLAGS "-funsigned-char -Wno-deprecated -Wno-char-subscripts")

set(GUTIL_LIBS gutil)
