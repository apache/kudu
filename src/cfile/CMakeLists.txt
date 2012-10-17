SET(CFILE_SRCS cfile.cc cfile_reader.cc int_block.cc)

PROTOBUF_GENERATE_CPP(CFILE_PROTO_SRCS CFILE_PROTO_HDRS
  cfile.proto)
SET(CFILE_SRCS ${CFILE_SRCS} ${CFILE_PROTO_SRCS})
add_library(cfile ${CFILE_SRCS})

# cfile-test
add_executable(cfile-test cfile-test.cc)
TARGET_LINK_LIBRARIES(cfile-test cfile ${KUDU_TEST_LINK_LIBS})

# encoding-test
add_executable(encoding-test encoding-test.cc)
TARGET_LINK_LIBRARIES(encoding-test cfile ${KUDU_TEST_LINK_LIBS})