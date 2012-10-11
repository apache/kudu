ADD_LIBRARY(kudu_util status.cc env.cc env_posix.cc logging.cc coding.cc)
SET(KUDU_UTIL_LIBS kudu_util)
