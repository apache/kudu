// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: tkaftal@google.com (Tomasz Kaftal)
//
// File management utilities' implementation.
#include "gutil/file_util.h"

#include <sys/stat.h>
#include <stdio.h>

#include <vector>
using std::vector;

#include "gutil/walltime.h"
#include "gutil/timer.h"
#include <glog/logging.h>
#include "gutil/logging-inl.h"
#include "gutil/stringprintf.h"

using std::vector;

static inline File* TryCreate(const char *directory_prefix) {
  // Attempt to create a temporary file.
  string filename;
  if (!TempFile::TempFilename(directory_prefix, &filename))
    return NULL;
  File* fp = File::Create(filename, "w+");
  if (fp && fp->Open()) {
    DLOG(INFO) << "Created fname: " << fp->CreateFileName();
    return fp;
  }
  return NULL;
}

// Tries to create a tempfile in directory 'directory_prefix' or get a
// directory from GetExistingTempDirectories().
/* static */
File* TempFile::Create(const char *directory_prefix) {
  // If directory_prefix is not provided an already-existing temp directory
  // will be used
  if (!(directory_prefix && *directory_prefix)) {
    TryCreate(NULL);
  }

  struct stat st;
  if (!(stat(directory_prefix, &st) == 0 && S_ISDIR(st.st_mode))) {
    // Directory_prefix does not point to a directory.
    LOG(ERROR) << "Not a directory: " << directory_prefix;
    return NULL;
  }
  return TryCreate(directory_prefix);
}

// Creates a temporary file name using standard library utilities.
static inline void TempFilenameInDir(const char *directory_prefix,
                                     string *filename) {
  int32 tid = static_cast<int32>(pthread_self());
  int32 pid = static_cast<int32>(getpid());
  int64 now = CycleClock::Now();
  int64 now_usec = GetCurrentTimeMicros();
  *filename = File::JoinPath(
      directory_prefix,
      StringPrintf("tempfile-%x-%d-%llx-%llx",
                   tid, pid, now, now_usec));
}

/* static */
bool TempFile::TempFilename(const char *directory_prefix, string *filename) {
  CHECK(filename != NULL);
  filename->clear();

  if (directory_prefix != NULL) {
    TempFilenameInDir(directory_prefix, filename);
    return true;
  }

  // Re-fetching available dirs ensures thread safety.
  vector<string> dirs;
  // TODO(tkaftal): Namespace might vary depending on glog installation options,
  // I should probably use a configure/make flag here.
  google::GetExistingTempDirectories(&dirs);

  // Try each directory, as they might be full, have inappropriate
  // permissions or have different problems at times.
  for (int i = 0; i < dirs.size(); ++i) {
    TempFilenameInDir(dirs[i].c_str(), filename);
    if (File::Exists(*filename)) {
      LOG(WARNING) << "unique tempfile already exists in " << *filename;
      filename->clear();
    } else {
      return true;
    }
  }

  LOG(ERROR) << "Couldn't find a suitable TempFile anywhere. Tried "
             << dirs.size() << " directories";
  return false;
}
