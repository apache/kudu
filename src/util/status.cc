// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <stdint.h>
#include "status.h"
#include "gutil/strings/fastmem.h"

namespace kudu {


const char* Status::CopyState(const char* state) {
  uint32_t size;
  strings::memcpy_inlined((char *)&size, state, sizeof(size));
  char* result = new char[size + 7];
  strings::memcpy_inlined(result, state, size + 7);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2,
               int16_t posix_code) {
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 7];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, &posix_code, sizeof(posix_code));
  memcpy(result + 7, msg.data(), len1);
  if (len2) {
    result[7 + len1] = ':';
    result[8 + len1] = ' ';
    memcpy(result + 9 + len1, msg2.data(), len2);
  }
  state_ = result;
}

std::string Status::ToString() const {
  if (state_ == NULL) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      case kAlreadyPresent:
        type = "Already present: ";
        break;
      case kRuntimeError:
        type = "Runtime error: ";
        break;
      case kNetworkError:
        type = "Network error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    int16_t posix = posix_code();
    result.append(state_ + 7, length);
    if (posix != -1) {
      char buf[64];
      snprintf(buf, sizeof(buf), " (error %d)", posix);
      result.append(buf);
    }
    return result;
  }
}

int16_t Status::posix_code() const {
  if (state_ == NULL) {
    return 0;
  }
  int16_t posix_code;
  memcpy(&posix_code, state_ + 5, sizeof(posix_code));
  return posix_code;
}

}  // namespace kudu
