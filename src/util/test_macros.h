// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_UTIL_TEST_MACROS_H
#define KUDU_UTIL_TEST_MACROS_H


#define ASSERT_STATUS_OK(status) do { \
    Status _s = status; \
    if (_s.ok()) { \
      SUCCEED(); \
    } else { \
      FAIL() << "Bad status: " << _s.ToString();  \
    } \
  } while (0);


#endif
