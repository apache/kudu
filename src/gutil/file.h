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
// The file provides simple file functionalities.
// TODO(tkaftal): Tests needed.
#ifndef SUPERSONIC_OPENSOURCE_FILE_FILE_H_
#define SUPERSONIC_OPENSOURCE_FILE_FILE_H_

#include <string>
using std::string;

#include "gutil/integral_types.h"
#include "gutil/macros.h"
#include "gutil/strings/join.h"

// Use this file mode value, if you want the file system default behaviour when
// creating a file. The exact behaviour depends on the file system.
static const mode_t DEFAULT_FILE_MODE = static_cast<mode_t>(0x7FFFFFFFU);

// Wrapper class for system functions which handle basic file operations.
// The operations are virtual to enable subclassing, if there is a need for
// different filesystem/file-abstraction support.
class File {
 public:
  // Do *not* call the destructor directly (with the "delete" keyword)
  // nor use scoped_ptr; instead use Close().
  virtual ~File();

  // Factory method to create a new file object. Call Open on the
  // resulting object to open the file.  Using the appropriate flags
  // (+) will result in the file being created if it does not already
  // exist
  static File* Create(const std::string& file_name,
                      const std::string& mode);

  // Utility static method for checking file existence.
  static bool Exists(const string& file);

  // Join two path components, adding a slash if necessary.  If basename is an
  // absolute path then JoinPath ignores dirname and simply returns basename.
  static string JoinPath(const string& dirname, const string& basename);

  // Return true if file exists.  Returns false if file does not exist or if an
  // error is encountered.
  virtual bool Exists() const ABSTRACT;

  // Open a file that has already been created
  virtual bool Open() ABSTRACT;

  // Deletes the file returning true iff successful.
  virtual bool Delete() ABSTRACT;

  // Flush and Close access to a file handle and delete this File
  // object. Returns true on success.
  virtual bool Close() ABSTRACT;

  // Reads data and returns it in OUTPUT. Returns a value < 0 on error,
  // or the of bytes read otherwise. Returns zero on end-of-file.
  virtual int64 Read(void* OUTPUT, uint64 length) ABSTRACT;

  // Try to write 'length' bytes from 'buffer', returning
  // the number of bytes that were actually written.
  // Return <= 0 on error.
  virtual int64 Write(const void* buffer, uint64 length) ABSTRACT;

  // Traditional seek + read/write interface.
  // We do not support seeking beyond the end of the file and writing to
  // extend the file. Use Append() to extend the file.
  virtual bool Seek(int64 position) ABSTRACT;

  // If we're currently at eof.
  virtual bool eof() ABSTRACT;

  // Returns the file name given during File::Create(...) call.
  virtual const string& CreateFileName() { return create_file_name_; }

 protected:
  explicit File(const string& create_file_name);

  // Name of the created file.
  const string create_file_name_;
};
#endif  // SUPERSONIC_OPENSOURCE_FILE_FILE_H_
