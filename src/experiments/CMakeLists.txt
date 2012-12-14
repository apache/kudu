# rwlock-perf
ADD_EXECUTABLE(rwlock-perf rwlock-perf.cc)
TARGET_LINK_LIBRARIES(rwlock-perf ${KUDU_TEST_LINK_LIBS})
