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


// Like the above, but doesn't record successful
// tests.
#define ASSERT_STATUS_OK_FAST(status) do {      \
    Status _s = status; \
    if (!_s.ok()) { \
      FAIL() << "Bad status: " << _s.ToString();  \
    } \
  } while (0);

#define ASSERT_STR_CONTAINS(str, substr) do { \
  std::string _s = (str); \
  if (_s.find((substr)) == std::string::npos) { \
    FAIL() << "Expected to find substring '" << (substr) \
    << "'. Got: '" << _s << "'"; \
  } \
  } while (0);


#endif
