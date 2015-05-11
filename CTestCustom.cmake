# Kudu data block storage requires hole punching support in the underlying
# filesystem, which we can crudely test via 'fallocate'.
if (NOT ("$ENV{TEST_TMPDIR}" STREQUAL ""))
  set(CTEST_CUSTOM_PRE_TEST
    "fallocate -l 4096 $ENV{TEST_TMPDIR}/preallocate_must_work"
    "rm -f $ENV{TEST_TMPDIR}/preallocate_must_work"
    )
endif()
