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
// File wrapper implementation.

#include "gutil/file.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>

#include <glog/logging.h>
#include "gutil/logging-inl.h"

// Returns true if a uint64 actually looks like a negative int64. This checks
// if the most significant bit is one.
//
// This function exists because the file interface declares some length/size
// fields to be uint64, and we want to catch the error case where someone
// accidently passes an negative number to one of the interface routines.
inline static bool IsUInt64ANegativeInt64(uint64 num) {
  return (static_cast<int64>(num) < 0);
}

File::File(const string& name)
    : create_file_name_(name) { }

File::~File() { }

/* static */
string File::JoinPath(const string& dirname, const string& basename) {
  if ((!basename.empty() && basename[0] == '/') || dirname.empty()) {
    return basename;
  } else if (dirname[dirname.size() - 1] == '/') {
    return StrCat(dirname, basename);
  } else {
    return StrCat(dirname, "/", basename);
  }
}

namespace {
// ----------------- LocalFileImpl --------------------------------------------
// Simple file implementation used for local-machine files (mainly temporary)
// only.
class LocalFileImpl : public File {
 public:
  LocalFileImpl(const std::string& file_name,
           const std::string& mode,
           const mode_t& permissions);

  virtual ~LocalFileImpl();

  // Return true if file exists.  Returns false if file does not exist or if an
  // error is encountered.
  virtual bool Exists() const;

  // File handling methods.
  virtual bool Open();
  virtual bool Delete();
  virtual bool Close();
  virtual int64 Read(void* OUTPUT, uint64 length);
  virtual int64 Write(const void* buffer, uint64 length);
  virtual bool Seek(int64 position);
  virtual bool eof();

 protected:
  FILE* internal_file_;

 private:
  string file_name_;
  string file_mode_;
  mode_t permissions_;

  bool IsOpenedWritable() const;

  DISALLOW_COPY_AND_ASSIGN(LocalFileImpl);
};

LocalFileImpl::LocalFileImpl(const std::string& file_name,
                   const std::string& mode,
                   const mode_t& permissions)
  : File(file_name),
    internal_file_(NULL),
    file_name_(file_name),
    file_mode_(mode),
    permissions_(permissions) { }

LocalFileImpl::~LocalFileImpl() { }

bool LocalFileImpl::Exists() const {
  return access(file_name_.c_str(), F_OK) != -1;
}

bool LocalFileImpl::Open() {
  if (internal_file_ != NULL) {
    LOG(ERROR) << "File already open: " << internal_file_;
    return false;
  }

  {
    // Using sys/stat.h to check if file can be opened.
    struct stat tmp;
    if (stat(create_file_name_.c_str(), &tmp) != 0) {
      if (errno != ENOENT) {
        // In case of an error (ENOENT only means a directory on
        // create_file_name_'s has not been found - it will be created).
        LOG(WARNING) << "Can't open " << create_file_name_
                     << " because stat() failed "
                     << "(errno = " << strerror(errno) << ").";
        return false;
      }
    } else if (S_ISDIR(tmp.st_mode)) {
      LOG(ERROR) << "Can't open " << create_file_name_
                 << " because it's a directory.";
      return false;
    }
  }

  mode_t permissions = permissions_;
  if (permissions == DEFAULT_FILE_MODE)
    permissions = 0666;

  // Get mode flags
  bool must_use_fopen = false;
  int mode_flags = 0;
  if (file_mode_ == "r") {
    mode_flags = O_RDONLY;
  } else if (file_mode_ == "r+") {
    mode_flags = O_RDWR;
  } else if (file_mode_ == "w") {
    mode_flags = O_CREAT | O_WRONLY | O_TRUNC;
  } else if (file_mode_ == "w+") {
    mode_flags = O_CREAT | O_RDWR | O_TRUNC;
  } else if (file_mode_ == "a") {
    mode_flags = O_CREAT | O_WRONLY | O_APPEND;
  } else if (file_mode_ == "a+") {
    mode_flags = O_CREAT | O_RDWR | O_APPEND;
  } else {
    must_use_fopen = true;
  }
  if (must_use_fopen) {
    // We don't understand the file mode; see if we can let fopen handle it.
    if (permissions_ == DEFAULT_FILE_MODE) {
      internal_file_ = fopen(file_name_.c_str(), file_mode_.c_str());
    }
  } else {
    int fd = open(file_name_.c_str(), mode_flags, permissions);
    if (fd >= 0) {
      internal_file_ = fdopen(fd, file_mode_.c_str());

      if (internal_file_ != NULL) {
        LOG(ERROR) << "fdopen failed: " << strerror(errno);
        close(fd);
      }
    }
  }
  return (internal_file_ != NULL);
}

bool LocalFileImpl::Delete() {
  int err;
  if ((err = unlink(file_name_.c_str())) == 0) {
    return true;
  } else {
    return false;
  }
}

bool LocalFileImpl::Close() {
  bool result = false;
  if (internal_file_ != NULL) {
    bool error = false;
    int rc;
    error |= (IsOpenedWritable() && ferror(internal_file_));
    rc = fclose(internal_file_);
    error |= (rc != 0);
    result = !error;
    internal_file_ = NULL;
  }
  delete this;
  return result;
}

int64 LocalFileImpl::Read(void* buffer, uint64 length) {
  if ((buffer == NULL) || IsUInt64ANegativeInt64(length)) {
    LOG(ERROR) << "Bad read arguments.  Buff: " << buffer
               << " length: " << length << " file: "
               << create_file_name_;
    return -1;
  }
  if (internal_file_ == NULL) {
    return -1;
  }
  const uint64 max_bytes_to_read = INT_MAX;
  uint64 bytes_to_read = 0;
  uint64 bytes_read = 0;
  uint64 total_bytes_read = 0;
  do {
    bytes_to_read = min(length-total_bytes_read, max_bytes_to_read);
    bytes_read = fread(static_cast<char *>(buffer) + total_bytes_read, 1,
                       bytes_to_read, internal_file_);
    if (bytes_read < bytes_to_read && ferror(internal_file_)) {
      LOG(ERROR) << "Error on read, " << bytes_read << " out of "
                 << bytes_to_read << " bytes read; file: "
                 << create_file_name_;
    }
    total_bytes_read += bytes_read;
  } while (total_bytes_read != length && bytes_read == bytes_to_read);
  return total_bytes_read;
}

int64 LocalFileImpl::Write(const void* buffer, uint64 length) {
  if ((buffer == NULL) || IsUInt64ANegativeInt64(length)) {
    LOG(ERROR) << "Bad write arguments.  Buff: " << buffer
               << " length: " << length << " file: "
               << create_file_name_;
    return -1;
  }
  if (internal_file_ == NULL) {
    return -1;
  } else {
    const int64 bytes_written = fwrite(buffer, 1, length, internal_file_);

    // Checking ferror() here flags errors sooner, though we could skip it
    // since caller should not assume that a "successful" write makes it to
    // disk before Flush() or Close(), which already check ferror().
    if (ferror(internal_file_)) {
      return 0;
    } else {
      return bytes_written;
    }
  }
}

// The following require a bunch of assertions to make sure
// the 32 to 64 bit conversions are ok.
bool LocalFileImpl::Seek(int64 position) {
  if (internal_file_ == NULL) {
    LOG(ERROR) << "Can't seek on an un-open file: " << create_file_name_;
    return false;
  }
  if (position < 0) {
    LOG(ERROR) << "Invalid seek position parameter: " << position
               << " on file " << create_file_name_;
    return false;
  }
  if (FSEEKO(internal_file_, static_cast<off_t>(position), SEEK_SET) != 0) {
    return false;
  }
  return true;
}

bool LocalFileImpl::eof() {
  if (internal_file_ == NULL) return true;
  return static_cast<bool>(feof(internal_file_));
}

// Is the file opened writable?
inline bool LocalFileImpl::IsOpenedWritable() const {
  return ((file_mode_[0] == 'w') ||
          (file_mode_[0] == 'a') ||
          ((file_mode_[0] != '\0') && (file_mode_[1] == '+')));
}

}  // namespace

/* static */
File* File::Create(const std::string& file_name,
                   const std::string& mode) {
  return new LocalFileImpl(file_name, mode, DEFAULT_FILE_MODE);
}

/* static */
bool File::Exists(const string& fname) {
  return LocalFileImpl(fname, NULL, DEFAULT_FILE_MODE).Exists();
}
