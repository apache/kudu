// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_UTIL_ENV_UTIL_H
#define KUDU_UTIL_ENV_UTIL_H

#include <tr1/memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/env.h"

namespace kudu {
namespace env_util {

using std::string;
using std::tr1::shared_ptr;

Status OpenFileForWrite(Env *env, const string &path,
                        shared_ptr<WritableFile> *file);

Status OpenFileForWrite(const WritableFileOptions& opts,
                        Env *env, const string &path,
                        shared_ptr<WritableFile> *file);

Status OpenFileForRandom(Env *env, const string &path,
                         shared_ptr<RandomAccessFile> *file);

Status OpenFileForSequential(Env *env, const string &path,
                             shared_ptr<SequentialFile> *file);

// Read exactly 'n' bytes from the given file. If fewer than 'n' bytes
// are read, returns an IOError. This differs from the underlying
// RandomAccessFile::Read(), which may return a "short read".
//
// Similar to RandomAccessFile::Read(), '*result' is modified to point
// to the bytes which were read. These bytes may be a copy placed in
// the 'scratch' buffer, or result may point into the underlying file
// (e.g. via mmap or other zero-copy mechanism).
//
// NOTE: even if this returns an error, some data _may_ be read into
// the provided scratch buffer, but no guarantee that that will be the
// case.
Status ReadFully(RandomAccessFile* file, uint64_t offset, size_t n,
                 Slice* result, uint8_t* scratch);

// Creates the directory given by 'path', unless it already exists.
//
// If 'created' is not NULL, sets it to true if the directory was
// created, false otherwise.
Status CreateDirIfMissing(Env* env, const std::string& path,
                          bool* created = NULL);

// Deletes a file or directory when this object goes out of scope.
//
// The deletion may be cancelled by calling .Cancel().
// This is typically useful for cleaning up temporary files if the
// creation of the tmp file may fail.
class ScopedFileDeleter {
 public:
  ScopedFileDeleter(Env* env, const std::string& path);
  ~ScopedFileDeleter();

  // Do not delete the file when this object goes out of scope.
  void Cancel() {
    should_delete_ = false;
  }

 private:
  Env* const env_;
  const std::string path_;
  bool should_delete_;

  DISALLOW_COPY_AND_ASSIGN(ScopedFileDeleter);
};

} // namespace env_util
} // namespace kudu

#endif
