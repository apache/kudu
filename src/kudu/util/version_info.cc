// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/version_info.h"

#include <string>

#include "kudu/generated/version_defines.h"
#include "kudu/gutil/strings/substitute.h"

using std::string;

namespace kudu {

string VersionInfo::GetGitHash() {
  string ret = KUDU_GIT_HASH;
  if (!KUDU_BUILD_CLEAN_REPO) {
    ret += "-dirty";
  }
  return ret;
}

string VersionInfo::GetShortVersionString() {
  return strings::Substitute("kudu $0 (rev $1)",
                             KUDU_VERSION_STRING,
                             GetGitHash());
}

string VersionInfo::GetAllVersionInfo() {
  string ret = strings::Substitute(
      "kudu $0\n"
      "revision $1\n"
      "build type $2\n"
      "built by $3 at $4 on $5",
      KUDU_VERSION_STRING,
      GetGitHash(),
      KUDU_BUILD_TYPE,
      KUDU_BUILD_USERNAME,
      KUDU_BUILD_TIMESTAMP,
      KUDU_BUILD_HOSTNAME);
  if (strlen(KUDU_BUILD_ID) > 0) {
    strings::SubstituteAndAppend(&ret, "\nbuild id $0", KUDU_BUILD_ID);
  }
#ifdef ADDRESS_SANITIZER
  ret += "\nASAN enabled";
#endif
#ifdef THREAD_SANITIZER
  ret += "\nTSAN enabled";
#endif
  return ret;
}

} // namespace kudu
