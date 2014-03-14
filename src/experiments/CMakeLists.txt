# rwlock-perf
add_executable(rwlock-perf rwlock-perf.cc)
target_link_libraries(rwlock-perf kudu_util ${KUDU_TEST_LINK_LIBS})

add_executable(merge-test merge-test.cc)
target_link_libraries(merge-test kudu_util ${KUDU_TEST_LINK_LIBS})
