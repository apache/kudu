// Copyright (c) 2014, Cloudera, inc.
#include "kudu/util/test_util.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/spinlock_profiling.h"

DEFINE_bool(test_leave_files, false,
            "Whether to leave test files around after the test run");

DEFINE_int32(test_random_seed, 0, "Random seed to use for randomized tests");

using std::string;
using strings::Substitute;

namespace kudu {

static const char* const kSlowTestsEnvVariable = "KUDU_ALLOW_SLOW_TESTS";

static const uint64 kTestBeganAtMicros = Env::Default()->NowMicros();

///////////////////////////////////////////////////
// KuduTest
///////////////////////////////////////////////////

KuduTest::KuduTest()
  : env_(new EnvWrapper(Env::Default())),
    test_dir_(GetTestDataDirectory()) {
}

// env passed in from subclass, for tests that run in-memory
KuduTest::KuduTest(Env *env)
  : env_(env),
    test_dir_(GetTestDataDirectory()) {
}

KuduTest::~KuduTest() {
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

void KuduTest::SetUp() {
  InitSpinLockContentionProfiling();
}

string KuduTest::GetTestPath(const string& relative_path) {
  CHECK(!test_dir_.empty()) << "Call SetUp() first";
  return JoinPathSegments(test_dir_, relative_path);
}

///////////////////////////////////////////////////
// Test utility functions
///////////////////////////////////////////////////

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

string GetTestDataDirectory() {
  const ::testing::TestInfo* const test_info =
    ::testing::UnitTest::GetInstance()->current_test_info();
  CHECK(test_info) << "Must be running in a gtest unit test to call this function";
  string dir;
  CHECK_OK(Env::Default()->GetTestDirectory(&dir));

  dir += Substitute("/$0.$1.$2-$3",
    StringReplace(test_info->test_case_name(), "/", "_", true).c_str(),
    StringReplace(test_info->name(), "/", "_", true).c_str(),
    kTestBeganAtMicros,
    getpid());
  Status s = Env::Default()->CreateDir(dir);
  CHECK(s.IsAlreadyPresent() || s.ok())
    << "Could not create directory " << dir << ": " << s.ToString();
  if (s.ok()) {
    string metadata;

    StrAppend(&metadata, Substitute("PID=$0\n", getpid()));

    StrAppend(&metadata, Substitute("PPID=$0\n", getppid()));

    char* jenkins_build_id = getenv("BUILD_ID");
    if (jenkins_build_id) {
      StrAppend(&metadata, Substitute("BUILD_ID=$0\n", jenkins_build_id));
    }

    CHECK_OK(WriteStringToFile(Env::Default(), metadata,
                               Substitute("$0/test_metadata", dir)));
  }
  return dir;
}

} // namespace kudu
