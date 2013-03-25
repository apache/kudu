// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

DEFINE_bool(test_leave_files, false,
            "Whether to leave test files around after the test run");


int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
