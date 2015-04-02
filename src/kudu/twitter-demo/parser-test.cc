// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/twitter-demo/parser.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace twitter_demo {

// Return the directory of the currently-running executable.
static string GetExecutableDir() {
  string exec;
  CHECK_OK(Env::Default()->GetExecutablePath(&exec));
  return DirName(exec);
}

static Status LoadFile(const string& name, vector<string>* lines) {
  // The test runs from build/debug|release/, so go up two directories
  // to get back to the source dir.
  string path = GetExecutableDir() + "/" + "../../src/kudu/twitter-demo/" + name;
  faststring data;
  RETURN_NOT_OK(ReadFileToString(Env::Default(), path, &data));

  *lines = strings::Split(data.ToString(), "\n");
  return Status::OK();
}

static void EnsureFileParses(const char* file, TwitterEventType expected_type) {
  TwitterEventParser p;
  TwitterEvent event;

  SCOPED_TRACE(file);
  vector<string> jsons;
  CHECK_OK(LoadFile(file, &jsons));

  int line_number = 1;
  BOOST_FOREACH(const string& json, jsons) {
    if (json.empty()) continue;
    SCOPED_TRACE(json);
    SCOPED_TRACE(line_number);
    ASSERT_OK(p.Parse(json, &event));
    ASSERT_EQ(expected_type, event.type);
    line_number++;
  }
}

// example-tweets.txt includes a few hundred tweets collected
// from the sample hose.
TEST(ParserTest, TestParseTweets) {
  EnsureFileParses("example-tweets.txt", TWEET);
}

// example-deletes.txt includes a few hundred deletes collected
// from the sample hose.
TEST(ParserTest, TestParseDeletes) {
  EnsureFileParses("example-deletes.txt", DELETE_TWEET);
}

TEST(ParserTest, TestReformatTime) {
  ASSERT_EQ("20130814063107", TwitterEventParser::ReformatTime("Wed Aug 14 06:31:07 +0000 2013"));
}

} // namespace twitter_demo
} // namespace kudu
