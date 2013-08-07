# Copyright (c) 2013, Cloudera, inc.

FIND_LIBRARY(LIBOAUTH_LIBRARY NAMES oauth)
FIND_PACKAGE(CURL)
IF(NOT CURL_FOUND OR NOT LIBOAUTH_LIBRARY)
  MESSAGE(WARNING "libcurl or liboauth not found on system. Skipping twitter demo")
ELSE()

  ADD_LIBRARY(twitter_demo
    oauth.cc
    twitter_streamer.cc
    )
  TARGET_LINK_LIBRARIES(twitter_demo ${KUDU_LINK_LIBS} ${LIBOAUTH_LIBRARY} ${CURL_LIBRARIES})


  ADD_EXECUTABLE(ingest_firehose ingest_firehose.cc)
  TARGET_LINK_LIBRARIES(ingest_firehose twitter_demo)

  # Tests
  ADD_KUDU_TEST(oauth-test)
  TARGET_LINK_LIBRARIES(oauth-test twitter_demo)

ENDIF() # library checks