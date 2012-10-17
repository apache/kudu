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
// The file provides file management utilities.
#ifndef SUPERSONIC_OPENSOURCE_FILE_FILE_UTIL_H_
#define SUPERSONIC_OPENSOURCE_FILE_FILE_UTIL_H_

#include "gutil/file.h"
#include "gutil/macros.h"

class TempFile {
 public:
  // Creates a file with a temporary-looking filename in read/write
  // mode in directory 'directory_prefix' (which can be followed or
  // not by a "/") or, if 'directory_prefix' is NULL or the empty string,
  // in a temporary directory (as provided by GetExistingTempDirectories()).
  //
  // Returns: a new File*, opened for read/write or NULL if it couldn't create
  // one.
  static File* Create(const char *directory_prefix);

  // The following method returns a temporary-looking filename. Be
  // advised that it might change behavior in the future and the
  // name generated might not follow any specific rule or pattern.
  //
  // Returns a unique filename in directory 'directory_prefix' (which
  // can be followed or not by a "/") or, if 'directory_prefix' is
  // NULL in a local scratch directory. Unique filenames are based on
  // localtime, cycle counts, hostname, pid and tid, to ensure
  // uniqueness. Returns: true if 'filename' contains a unique
  // filename, otherwise false (and 'filename' is left unspecified).
  static bool TempFilename(const char *directory_prefix, string *filename);
};

class FileDeleter {
 public:
  // Takes ownership of 'fp' and deletes it upon going out of
  // scope.
  explicit FileDeleter(File* fp) : fp_(fp) { }
  File* get() const { return fp_; }
  File& operator*() const { return *fp_; }
  File* operator->() const { return fp_; }
  File* release() {
    File* fp = fp_;
    fp_ = NULL;
    return fp;
  }
  // TODO(user): Change reset() to return a bool (the result
  // of the call to Delete() && Close())
  void reset(File* new_fp) {
    if (fp_) {
      fp_->Delete();
      fp_->Close();
    }
    fp_ = new_fp;
  }
  // Delete (unlink, remove) the underlying file.
  ~FileDeleter() { reset(NULL); }

 private:
  File* fp_;
  DISALLOW_COPY_AND_ASSIGN(FileDeleter);
};
#endif  // SUPERSONIC_OPENSOURCE_FILE_FILE_UTIL_H_
