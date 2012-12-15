ADD_LIBRARY(kudu_util status.cc env.cc env_posix.cc logging.cc coding.cc
  hexdump.cc
  memory/arena.cc memory/memory.cc
)
SET(KUDU_UTIL_LIBS kudu_util)

# bitmap-test
ADD_EXECUTABLE(bitmap-test bitmap-test.cc)
TARGET_LINK_LIBRARIES(bitmap-test ${KUDU_TEST_LINK_LIBS})

# stringbag-test
ADD_EXECUTABLE(stringbag-test stringbag-test.cc)
TARGET_LINK_LIBRARIES(stringbag-test ${KUDU_TEST_LINK_LIBS})