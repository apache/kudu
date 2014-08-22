// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <tr1/memory>

#include <glog/logging.h>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/status.h"

using strings::Substitute;
using std::tr1::shared_ptr;

namespace kudu {
namespace env_util {

Status OpenFileForWrite(Env* env, const string& path,
                        shared_ptr<WritableFile>* file) {
  return OpenFileForWrite(WritableFileOptions(), env, path, file);
}

Status OpenFileForWrite(const WritableFileOptions& opts,
                        Env *env, const string &path,
                        shared_ptr<WritableFile> *file) {
  WritableFile *w;
  RETURN_NOT_OK(env->NewWritableFile(opts, path, &w));
  file->reset(w);
  return Status::OK();
}

Status OpenFileForRandom(Env *env, const string &path,
                         shared_ptr<RandomAccessFile> *file) {
  RandomAccessFile *r;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &r));
  file->reset(r);
  return Status::OK();
}

Status OpenFileForSequential(Env *env, const string &path,
                             shared_ptr<SequentialFile> *file) {
  SequentialFile *r;
  RETURN_NOT_OK(env->NewSequentialFile(path, &r));
  file->reset(r);
  return Status::OK();
}

Status ReadFully(RandomAccessFile* file, uint64_t offset, size_t n,
                 Slice* result, uint8_t* scratch) {

  bool first_read = true;

  int rem = n;
  uint8_t* dst = scratch;
  while (rem > 0) {
    Slice this_result;
    RETURN_NOT_OK(file->Read(offset, rem, &this_result, dst));
    DCHECK_LE(this_result.size(), rem);
    if (this_result.size() == 0) {
      // EOF
      return Status::IOError(Substitute("EOF trying to read $0 bytes at offset $1",
                                        n, offset));
    }

    if (first_read && this_result.size() == n) {
      // If it's the first read, we can return a zero-copy array.
      *result = this_result;
      return Status::OK();
    }
    first_read = false;

    // Otherwise, we're going to have to do more reads and stitch
    // each read together.
    this_result.relocate(dst);
    dst += this_result.size();
    rem -= this_result.size();
    offset += this_result.size();
  }
  DCHECK_EQ(0, rem);
  *result = Slice(scratch, n);
  return Status::OK();
}

ScopedFileDeleter::ScopedFileDeleter(Env* env, const std::string& path)
  : env_(DCHECK_NOTNULL(env)),
    path_(path),
    should_delete_(true) {
}

ScopedFileDeleter::~ScopedFileDeleter() {
  if (should_delete_) {
    WARN_NOT_OK(env_->DeleteFile(path_), "Failed to remove temporary file");
  }
}

} // namespace env_util
} // namespace kudu
