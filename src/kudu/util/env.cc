// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "kudu/util/env.h"

#include <memory>

#include <glog/logging.h>

#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

using std::unique_ptr;

namespace kudu {

Env::~Env() {
}

File::~File() {
}

FileLock::~FileLock() {
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync,
                                  bool is_sensitive) {
  unique_ptr<WritableFile> file;
  WritableFileOptions opts;
  opts.is_sensitive = is_sensitive;
  Status s = env->NewWritableFile(opts, fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  file.reset();  // Will auto-close if we did not close above
  if (!s.ok()) {
    WARN_NOT_OK(env->DeleteFile(fname),
                "Failed to delete partially-written file " + fname);
  }
  return s;
}

// TODO: move these utils into env_util
Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true, false);
}

Status DoReadFileToString(Env* env, const std::string& fname, faststring* data, bool is_sensitive) {
  data->clear();
  unique_ptr<SequentialFile> file;
  SequentialFileOptions opts;
  opts.is_sensitive = is_sensitive;
  Status s = env->NewSequentialFile(opts, fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  unique_ptr<uint8_t[]> scratch(new uint8_t[kBufferSize]);
  while (true) {
    Slice fragment(scratch.get(), kBufferSize);
    s = file->Read(&fragment);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  return s;
}

Status ReadFileToString(Env* env, const std::string& fname, faststring* data) {
  return DoReadFileToString(env, fname, data, false);
}

}  // namespace kudu
