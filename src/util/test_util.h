// Copyright (c) 2013, Cloudera, inc.
//
// Base test class, with various utility functions.
#ifndef KUDU_UTIL_TEST_UTIL_H
#define KUDU_UTIL_TEST_UTIL_H

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>
#include <strings.h>
#include "util/env.h"
#include "util/test_macros.h"
#include "util/path_util.h"
#include "util/random.h"
#include "util/spinlock_profiling.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"

DECLARE_bool(test_leave_files);
DEFINE_int32(test_random_seed, 0, "Random seed to use for randomized tests");

static const char* const kSlowTestsEnvVariable = "KUDU_ALLOW_SLOW_TESTS";
namespace kudu {

class KuduTest : public ::testing::Test {
 public:
  KuduTest() :
    env_(new EnvWrapper(Env::Default()))
  {}

  // env passed in from subclass, for tests that run in-memory
  explicit KuduTest(Env *env) :
    env_(env)
  {}

  virtual void SetUp() OVERRIDE {
    kudu::InitSpinLockContentionProfiling();

    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

    CHECK_OK(env_->GetTestDirectory(&test_dir_));

    test_dir_ += strings::Substitute(
      "/$0.$1.$2-$3",
      StringReplace(test_info->test_case_name(), "/", "_", true).c_str(),
      StringReplace(test_info->name(), "/", "_", true).c_str(),
      env_->NowMicros(), getpid());

    ASSERT_STATUS_OK(env_->CreateDir(test_dir_));
  }

  virtual ~KuduTest() {
    // Clean up the test directory in the destructor instead of a TearDown
    // method. This is better because it ensures that the child-class
    // dtor runs first -- so, if the child class is using a minicluster, etc,
    // we will shut that down before we remove files underneath.
    if (FLAGS_test_leave_files) {
      LOG(INFO) << "-----------------------------------------------";
      LOG(INFO) << "--test_leave_files specified, leaving files in " << test_dir_;
    } else if (HasFatalFailure()) {
      LOG(INFO) << "-----------------------------------------------";
      LOG(INFO) << "Had fatal failures, leaving test files at " << test_dir_;
    } else {
      VLOG(1) << "Cleaning up temporary test files...";
      WARN_NOT_OK(env_->DeleteRecursively(test_dir_),
                  "Couldn't remove test files");
    }
  }

 protected:
  string GetTestPath(const string &relative_path) {
    CHECK(!test_dir_.empty()) << "Call SetUp() first";
    return JoinPathSegments(test_dir_, relative_path);
  }

  bool AllowSlowTests() {
    char *e = getenv(kSlowTestsEnvVariable);
    if ((e == NULL) ||
        (strlen(e) == 0) ||
        (strcasecmp(e, "false") == 0) ||
        (strcasecmp(e, "0") == 0) ||
        (strcasecmp(e, "no") == 0)) {
      return false;
    }
    if ((strcasecmp(e, "true") == 0) ||
        (strcasecmp(e, "1") == 0) ||
        (strcasecmp(e, "yes") == 0)) {
      return true;
    }
    LOG(FATAL) << "Unrecognized value for " << kSlowTestsEnvVariable << ": " << e;
    return false;
  }

  // Call srand() with a random seed based on the current time, reporting
  // that seed to the logs. The time-based seed may be overridden by passing
  // --test_random_seed= from the CLI in order to reproduce a failed randomized
  // test. Returns the seed.
  int SeedRandom() {
    int seed;
    // Initialize random seed
    if (FLAGS_test_random_seed == 0) {
      // Not specified by user
      seed = time(NULL);
    } else {
      seed = FLAGS_test_random_seed;
    }
    LOG(INFO) << "Using random seed: " << seed;
    srand(seed);
    return seed;
  }

  gscoped_ptr<Env> env_;
  string test_dir_;
};

} // namespace kudu
#endif
