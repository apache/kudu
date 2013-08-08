// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/status.h"

#include <stdio.h>
#include <stdint.h>
#include "gutil/strings/fastmem.h"

namespace kudu {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  strings::memcpy_inlined(&size, state, sizeof(size));
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

std::string Status::CodeAsString() const {
  if (state_ == NULL) {
    return "OK";
  }

  char tmp[30];
  const char* type;
  switch (code()) {
    case kOk:
      type = "OK";
      break;
    case kNotFound:
      type = "Not found";
      break;
    case kCorruption:
      type = "Corruption";
      break;
    case kNotSupported:
      type = "Not implemented";
      break;
    case kInvalidArgument:
      type = "Invalid argument";
      break;
    case kIOError:
      type = "IO error";
      break;
    case kAlreadyPresent:
      type = "Already present";
      break;
    case kRuntimeError:
      type = "Runtime error";
      break;
    case kNetworkError:
      type = "Network error";
      break;
    default:
      snprintf(tmp, sizeof(tmp), "Unknown code(%d)",
               static_cast<int>(code()));
      type = tmp;
      break;
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }

  result.append(": ");
  Slice msg = message();
  result.append(reinterpret_cast<const char*>(msg.data()), msg.size());
  int16_t posix = posix_code();
  if (posix != -1) {
    char buf[64];
    snprintf(buf, sizeof(buf), " (error %d)", posix);
    result.append(buf);
  }
  return result;
}

Slice Status::message() const {
  if (state_ == NULL) {
    return Slice();
  }

  uint32_t length;
  memcpy(&length, state_, sizeof(length));
  return Slice(state_ + 7, length);
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
