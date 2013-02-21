# Disable warnings which trigger a lot in the Google code:
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -Wno-deprecated")


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
./stringprintf.cc
./strings/memutil.cc
./strings/charset.cc
./strings/util.cc
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
)

SET(GUTIL_LIBS gutil)
