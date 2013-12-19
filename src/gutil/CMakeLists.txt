PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../
  )
SET(GUTIL_SRCS ${PROTO_SRCS})

PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../
  PROTO_FILES proto/types.proto 
  )
SET(GUTIL_SRCS ${GUTIL_SRCS} ${PROTO_SRCS})


ADD_LIBRARY(gutil
  ${GUTIL_SRCS}
./bits.cc
./ref_counted.cc
./stringprintf.cc
./strings/memutil.cc
./strings/charset.cc
./strings/util.cc
./strings/human_readable.cc
./strings/join.cc
./strings/numbers.cc
./strings/split.cc
./strings/strip.cc
./strings/ascii_ctype.cc
./strings/escaping.cc
./strings/stringpiece.cc
./strings/substitute.cc
./strings/strcat.cc
./strings/serialize.cc
./threading/thread_collision_warner.cc
./mathlimits.cc
./int128.cc
./strtoint.cc
./once.cc
./atomicops-internals-x86.cc
./spinlock_wait.cc
./walltime.cc
./hash/hash.cc
./hash/jenkins.cc
./hash/city.cc
./utf/rune.c
)
# Disable warnings which trigger a lot in the Google code:
SET_TARGET_PROPERTIES(gutil PROPERTIES
  COMPILE_FLAGS "-funsigned-char -Wno-deprecated -Wno-char-subscripts")

SET(GUTIL_LIBS gutil)
