ADD_LIBRARY(kudu_util status.cc env.cc env_posix.cc logging.cc coding.cc
  bloom_filter.cc
  group_varint.cc
  hexdump.cc
  memory/arena.cc memory/memory.cc
)
SET(KUDU_UTIL_LIBS kudu_util)

ADD_KUDU_TEST(bitmap-test)
ADD_KUDU_TEST(stringbag-test)
ADD_KUDU_TEST(memory/arena-test)
ADD_KUDU_TEST(bloom_filter-test)
ADD_KUDU_TEST(group_varint-test)