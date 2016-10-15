// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/env_util.h"

#include <algorithm>
#include <cstdint>
#include <cerrno>
#include <ctime>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/slice.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

DEFINE_int64(disk_reserved_bytes_free_for_testing, -1,
             "For testing only! Set to number of bytes free on each filesystem. "
             "Set to -1 to disable this test-specific override");
TAG_FLAG(disk_reserved_bytes_free_for_testing, runtime);
TAG_FLAG(disk_reserved_bytes_free_for_testing, unsafe);

// We define some flags for testing purposes: Two prefixes and their associated
// "bytes free" overrides.
DEFINE_string(disk_reserved_override_prefix_1_path_for_testing, "",
              "For testing only! Specifies a prefix to override the visible 'bytes free' on. "
              "Use --disk_reserved_override_prefix_1_bytes_free_for_testing to set the number of "
              "bytes free for this path prefix. Set to empty string to disable.");
DEFINE_int64(disk_reserved_override_prefix_1_bytes_free_for_testing, -1,
             "For testing only! Set number of bytes free on the path prefix specified by "
             "--disk_reserved_override_prefix_1_path_for_testing. Set to -1 to disable.");
DEFINE_string(disk_reserved_override_prefix_2_path_for_testing, "",
              "For testing only! Specifies a prefix to override the visible 'bytes free' on. "
              "Use --disk_reserved_override_prefix_2_bytes_free_for_testing to set the number of "
              "bytes free for this path prefix. Set to empty string to disable.");
DEFINE_int64(disk_reserved_override_prefix_2_bytes_free_for_testing, -1,
             "For testing only! Set number of bytes free on the path prefix specified by "
             "--disk_reserved_override_prefix_2_path_for_testing. Set to -1 to disable.");
TAG_FLAG(disk_reserved_override_prefix_1_path_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_2_path_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_1_bytes_free_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_2_bytes_free_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_1_bytes_free_for_testing, runtime);
TAG_FLAG(disk_reserved_override_prefix_2_bytes_free_for_testing, runtime);

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace env_util {

Status OpenFileForWrite(Env* env, const string& path,
                        shared_ptr<WritableFile>* file) {
  return OpenFileForWrite(WritableFileOptions(), env, path, file);
}

Status OpenFileForWrite(const WritableFileOptions& opts,
                        Env *env, const string &path,
                        shared_ptr<WritableFile> *file) {
  unique_ptr<WritableFile> w;
  RETURN_NOT_OK(env->NewWritableFile(opts, path, &w));
  file->reset(w.release());
  return Status::OK();
}

Status OpenFileForRandom(Env *env, const string &path,
                         shared_ptr<RandomAccessFile> *file) {
  unique_ptr<RandomAccessFile> r;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

Status OpenFileForSequential(Env *env, const string &path,
                             shared_ptr<SequentialFile> *file) {
  unique_ptr<SequentialFile> r;
  RETURN_NOT_OK(env->NewSequentialFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

// If any of the override gflags specifies an override for the given path, then
// override the free bytes to match what is specified in the flag. See the
// definitions of these test-only flags for more information.
static void OverrideBytesFreeWithTestingFlags(const string& path, int64_t* bytes_free) {
  const string* prefixes[] = { &FLAGS_disk_reserved_override_prefix_1_path_for_testing,
                               &FLAGS_disk_reserved_override_prefix_2_path_for_testing };
  const int64_t* overrides[] = { &FLAGS_disk_reserved_override_prefix_1_bytes_free_for_testing,
                                 &FLAGS_disk_reserved_override_prefix_2_bytes_free_for_testing };
  for (int i = 0; i < arraysize(prefixes); i++) {
    if (*overrides[i] != -1 && !prefixes[i]->empty() && HasPrefixString(path, *prefixes[i])) {
      *bytes_free = *overrides[i];
      return;
    }
  }
}

Status VerifySufficientDiskSpace(Env *env, const std::string& path,
                                 int64_t requested_bytes, int64_t reserved_bytes) {
  const int64_t kOnePercentReservation = -1;
  DCHECK_GE(requested_bytes, 0);

  SpaceInfo space_info;
  RETURN_NOT_OK(env->GetSpaceInfo(path, &space_info));
  int64_t available_bytes = space_info.free_bytes;

  // Allow overriding these values by tests.
  if (PREDICT_FALSE(FLAGS_disk_reserved_bytes_free_for_testing > -1)) {
    available_bytes = FLAGS_disk_reserved_bytes_free_for_testing;
  }
  if (PREDICT_FALSE(FLAGS_disk_reserved_override_prefix_1_bytes_free_for_testing != -1 ||
                    FLAGS_disk_reserved_override_prefix_2_bytes_free_for_testing != -1)) {
    OverrideBytesFreeWithTestingFlags(path, &available_bytes);
  }

  // If they requested a one percent reservation, calculate what that is in bytes.
  if (reserved_bytes == kOnePercentReservation) {
    reserved_bytes = space_info.capacity_bytes / 100;
  }

  if (available_bytes - requested_bytes < reserved_bytes) {
    return Status::IOError(Substitute("Insufficient disk space to allocate $0 bytes under path $1 "
                                      "($2 bytes available vs $3 bytes reserved)",
                                      requested_bytes, path, available_bytes, reserved_bytes),
                           "", ENOSPC);
  }
  return Status::OK();
}

Status CreateDirIfMissing(Env* env, const string& path, bool* created) {
  Status s = env->CreateDir(path);
  if (created != nullptr) {
    *created = s.ok();
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

Status CreateDirsRecursively(Env* env, const string& path) {
  vector<string> segments = SplitPath(path);
  string partial_path;
  for (const string& segment : segments) {
    partial_path = partial_path.empty() ? segment : JoinPathSegments(partial_path, segment);
    bool is_dir;
    Status s = env->IsDirectory(partial_path, &is_dir);
    if (s.ok()) {
      // We didn't get a NotFound error, so something is there.
      if (is_dir) continue; // It's a normal directory.
      // Maybe a file or a symlink. Let's try to follow the symlink.
      string real_partial_path;
      RETURN_NOT_OK(env->Canonicalize(partial_path, &real_partial_path));
      s = env->IsDirectory(real_partial_path, &is_dir);
      if (s.ok() && is_dir) continue; // It's a symlink to a directory.
    }
    RETURN_NOT_OK_PREPEND(env->CreateDir(partial_path), "Unable to create directory");
  }
  return Status::OK();
}

Status CopyFile(Env* env, const string& source_path, const string& dest_path,
                WritableFileOptions opts) {
  unique_ptr<SequentialFile> source;
  RETURN_NOT_OK(env->NewSequentialFile(source_path, &source));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(source_path, &size));

  unique_ptr<WritableFile> dest;
  RETURN_NOT_OK(env->NewWritableFile(opts, dest_path, &dest));
  RETURN_NOT_OK(dest->PreAllocate(size));

  const int32_t kBufferSize = 1024 * 1024;
  unique_ptr<uint8_t[]> scratch(new uint8_t[kBufferSize]);

  uint64_t bytes_read = 0;
  while (bytes_read < size) {
    uint64_t max_bytes_to_read = std::min<uint64_t>(size - bytes_read, kBufferSize);
    Slice data(scratch.get(), max_bytes_to_read);
    RETURN_NOT_OK(source->Read(&data));
    RETURN_NOT_OK(dest->Append(data));
    bytes_read += data.size();
  }
  return Status::OK();
}

Status DeleteExcessFilesByPattern(Env* env, const string& pattern, int max_matches) {
  // Negative numbers don't make sense for our interface.
  DCHECK_GE(max_matches, 0);

  vector<string> matching_files;
  RETURN_NOT_OK(env->Glob(pattern, &matching_files));

  if (matching_files.size() <= max_matches) {
    return Status::OK();
  }

  vector<pair<time_t, string>> matching_file_mtimes;
  for (string& matching_file_path : matching_files) {
    int64_t mtime;
    RETURN_NOT_OK(env->GetFileModifiedTime(matching_file_path, &mtime));
    matching_file_mtimes.emplace_back(mtime, std::move(matching_file_path));
  }

  // Use mtime to determine which matching files to delete. This could
  // potentially be ambiguous, depending on the resolution of last-modified
  // timestamp in the filesystem, but that is part of the contract.
  std::sort(matching_file_mtimes.begin(), matching_file_mtimes.end());
  matching_file_mtimes.resize(matching_file_mtimes.size() - max_matches);

  for (const auto& matching_file : matching_file_mtimes) {
    RETURN_NOT_OK(env->DeleteFile(matching_file.second));
  }

  return Status::OK();
}

// Callback for DeleteTmpFilesRecursively().
//
// Tests 'basename' for the Kudu-specific tmp file infix, and if found,
// deletes the file.
static Status DeleteTmpFilesRecursivelyCb(Env* env,
                                          Env::FileType file_type,
                                          const string& dirname,
                                          const string& basename) {
  if (file_type != Env::FILE_TYPE) {
    // Skip directories.
    return Status::OK();
  }

  if (basename.find(kTmpInfix) != string::npos) {
    string filename = JoinPathSegments(dirname, basename);
    WARN_NOT_OK(env->DeleteFile(filename),
                Substitute("Failed to remove temporary file $0", filename));
  }
  return Status::OK();
}

Status DeleteTmpFilesRecursively(Env* env, const string& path) {
  return env->Walk(path, Env::PRE_ORDER, Bind(&DeleteTmpFilesRecursivelyCb, env));
}

ScopedFileDeleter::ScopedFileDeleter(Env* env, std::string path)
    : env_(DCHECK_NOTNULL(env)), path_(std::move(path)), should_delete_(true) {}

ScopedFileDeleter::~ScopedFileDeleter() {
  if (should_delete_) {
    bool is_dir;
    Status s = env_->IsDirectory(path_, &is_dir);
    WARN_NOT_OK(s, Substitute(
        "Failed to determine if path is a directory: $0", path_));
    if (!s.ok()) {
      return;
    }
    if (is_dir) {
      WARN_NOT_OK(env_->DeleteDir(path_),
                  Substitute("Failed to remove directory: $0", path_));
    } else {
      WARN_NOT_OK(env_->DeleteFile(path_),
          Substitute("Failed to remove file: $0", path_));
    }
  }
}

} // namespace env_util
} // namespace kudu
