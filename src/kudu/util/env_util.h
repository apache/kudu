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
#ifndef KUDU_UTIL_ENV_UTIL_H
#define KUDU_UTIL_ENV_UTIL_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

class Env;
class RandomAccessFile;
class SequentialFile;
class WritableFile;
struct WritableFileOptions;

namespace env_util {

Status OpenFileForWrite(Env *env, const std::string &path,
                        std::shared_ptr<WritableFile> *file);

Status OpenFileForWrite(const WritableFileOptions& opts,
                        Env *env, const std::string &path,
                        std::shared_ptr<WritableFile> *file);

Status OpenFileForRandom(Env *env, const std::string &path,
                         std::shared_ptr<RandomAccessFile> *file);

Status OpenFileForSequential(Env *env, const std::string &path,
                             std::shared_ptr<SequentialFile> *file);

// Returns Status::IOError with POSIX code ENOSPC if there is not sufficient
// disk space to write 'requested_bytes' bytes to the file system represented by 'path'.
// Otherwise returns OK.
// If 'reserved_bytes' equals -1, it is interpreted as a 1% reservation. No
// other values less than 0 are supported at this time.
// If 'available_bytes' is not null, it will contain the amount of free disk space (in bytes)
// in 'path' when the function finishes. This will happen even if the function returns IOError
// with ENOSPC, but not on any other error.
Status VerifySufficientDiskSpace(Env *env, const std::string& path, int64_t requested_bytes,
                                 int64_t reserved_bytes, int64_t* available_bytes = nullptr);

// Creates the directory given by 'path', unless it already exists.
//
// If 'created' is not NULL, sets it to true if the directory was
// created, false otherwise.
Status CreateDirIfMissing(Env* env, const std::string& path,
                          bool* created = NULL);

// Recursively create directories, if they do not exist, along the given path.
// Returns OK if successful or if the given path already existed.
// Upon failure, it is possible that some part of the directory structure may
// have been successfully created. Emulates the behavior of `mkdir -p`.
Status CreateDirsRecursively(Env* env, const std::string& path);

// Copy the contents of file source_path to file dest_path.
// This is not atomic, and if there is an error while reading or writing,
// a partial copy may be left in 'dest_path'. Does not fsync the parent
// directory of dest_path -- if you need durability then do that yourself.
Status CopyFile(Env* env, const std::string& source_path, const std::string& dest_path,
                WritableFileOptions opts);

// Deletes files matching 'pattern' in excess of 'max_matches' files.
// 'max_matches' must be greater than or equal to 0.
// The oldest files are deleted first, as determined by last modified time.
// In the case that multiple files have the same last modified time, it is not
// defined which file will be deleted first.
Status DeleteExcessFilesByPattern(Env* env, const std::string& pattern, int max_matches);

// Traverses 'path' recursively and deletes all files matching the special Kudu
// tmp file infix. Does not follow symlinks.
//
// Deletion errors generate warnings but do not halt the traversal.
Status DeleteTmpFilesRecursively(Env* env, const std::string& path);

// Checks if 'path' is an empty directory.
//
// Returns an error if it's not a directory. Otherwise, sets 'is_empty'
// accordingly.
Status IsDirectoryEmpty(Env* env, const std::string& path, bool* is_empty);

// Synchronize all of the parent directories belonging to 'dirs' and 'files'
// to disk.
Status SyncAllParentDirs(Env* env,
                         const std::vector<std::string>& dirs,
                         const std::vector<std::string>& files);

// Return a list of files within the given 'path'.
Status ListFilesInDir(Env* env,
                      const std::string& path,
                      std::vector<std::string>* entries);

} // namespace env_util
} // namespace kudu

#endif
