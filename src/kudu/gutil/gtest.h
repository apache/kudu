// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef BASE_GTEST_H
#define BASE_GTEST_H

#ifdef KUDU_HEADERS_USE_GTEST
#include <gtest/gtest_prod.h>
#else

#ifndef FRIEND_TEST
#define FRIEND_TEST(klass, test)
#endif

#endif

#endif
