# Disable warnings which trigger a lot in the Google code:
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -Wno-deprecated")

SET(EXTRA_PROTO_PATHS ..)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
  proto/types.proto
  exception/stack_trace.proto
  )


ADD_LIBRARY(gutil
  ${PROTO_SRCS}
./exception/stack_trace.cc
./bits.cc
./file_util.cc
./atomic/locking.cc
./atomic/x86-gcc.cc
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
./demangle.cc
./file.cc
./int128.cc
./strtoint.cc
./random.cc
./once.cc
./atomicops-internals-x86.cc
./spinlock_wait.cc
./walltime.cc
./hash/hash.cc
./hash/jenkins.cc
./hash/city.cc
)

SET(GUTIL_LIBS gutil)

ADD_EXECUTABLE(stack_trace_test ./exception/stack_trace_test.cc)
TARGET_LINK_LIBRARIES(stack_trace_test gutil ${KUDU_TEST_LINK_LIBS})

ADD_EXECUTABLE(coowned_pointer_test ./exception/coowned_pointer_test.cc)
TARGET_LINK_LIBRARIES(coowned_pointer_test gutil ${KUDU_TEST_LINK_LIBS})

ADD_EXECUTABLE(failureor_test ./exception/failureor_test.cc)
TARGET_LINK_LIBRARIES(failureor_test gutil ${KUDU_TEST_LINK_LIBS})

# Disabled: relies on some stuff from base/
#ADD_EXECUTABLE(atomic_test ./atomic/atomic_test.cc)
#TARGET_LINK_LIBRARIES(atomic_test gutil)
