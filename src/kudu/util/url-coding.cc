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

#include "kudu/util/url-coding.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <exception>
#include <functional>
#include <iterator>
#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <glog/logging.h>

using boost::archive::iterators::base64_from_binary;
using boost::archive::iterators::binary_from_base64;
using boost::archive::iterators::transform_width;
using std::string;
using std::vector;

// TODO(aserbin): consider replacing boost-based implementation of base64
//                encoding/decoding with https://github.com/tplgy/cppcodec
//                per list at https://en.cppreference.com/w/cpp/links/libs

namespace kudu {

// Hive selectively encodes characters. This is the whitelist of
// characters it will encode.
// See common/src/java/org/apache/hadoop/hive/common/FileUtils.java
// in the Hive source code for the source of this list.
static std::function<bool (char)> HiveShouldEscape = boost::is_any_of("\"#%\\*/:=?\u00FF"); // NOLINT(*)

// It is more convenient to maintain the complement of the set of
// characters to escape when not in Hive-compat mode.
static std::function<bool (char)> ShouldNotEscape = boost::is_any_of("-_.~"); // NOLINT(*)

static inline void UrlEncode(const char* in, int in_len, string* out, bool hive_compat) {
  (*out).reserve(in_len);
  std::ostringstream ss;
  for (int i = 0; i < in_len; ++i) {
    const char ch = in[i];
    // Escape the character iff a) we are in Hive-compat mode and the
    // character is in the Hive whitelist or b) we are not in
    // Hive-compat mode, and the character is not alphanumeric or one
    // of the four commonly excluded characters.
    if ((hive_compat && HiveShouldEscape(ch)) ||
        (!hive_compat && !(isalnum(ch) || ShouldNotEscape(ch)))) {
      ss << '%' << std::uppercase << std::hex << static_cast<uint32_t>(ch);
    } else {
      ss << ch;
    }
  }

  (*out) = ss.str();
}

void UrlEncode(const vector<uint8_t>& in, string* out, bool hive_compat) {
  if (in.empty()) {
    *out = "";
  } else {
    UrlEncode(reinterpret_cast<const char*>(&in[0]), in.size(), out, hive_compat);
  }
}

void UrlEncode(const string& in, string* out, bool hive_compat) {
  UrlEncode(in.c_str(), in.size(), out, hive_compat);
}

string UrlEncodeToString(const std::string& in, bool hive_compat) {
  string ret;
  UrlEncode(in, &ret, hive_compat);
  return ret;
}

// Adapted from
// http://www.boost.org/doc/libs/1_40_0/doc/html/boost_asio/
//   example/http/server3/request_handler.cpp
// See http://www.boost.org/LICENSE_1_0.txt for license for this method.
bool UrlDecode(const string& in, string* out, bool hive_compat) {
  out->clear();
  out->reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    if (in[i] == '%') {
      if (i + 3 <= in.size()) {
        int value = 0;
        std::istringstream is(in.substr(i + 1, 2));
        if (is >> std::hex >> value) {
          (*out) += static_cast<char>(value);
          i += 2;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (!hive_compat && in[i] == '+') { // Hive does not encode ' ' as '+'
      (*out) += ' ';
    } else {
      (*out) += in[i];
    }
  }
  return true;
}

static inline void Base64Encode(const char* in, int in_len, std::ostringstream* out) {
  typedef base64_from_binary<transform_width<const char*, 6, 8> > base64_encode;
  // Base64 encodes 8 byte chars as 6 bit values.
  std::ostringstream::pos_type len_before = out->tellp();
  copy(base64_encode(in), base64_encode(in + in_len), std::ostream_iterator<char>(*out));
  int bytes_written = out->tellp() - len_before;
  // Pad with = to make it valid base64 encoded string
  int num_pad = bytes_written % 4;
  if (num_pad != 0) {
    num_pad = 4 - num_pad;
    for (int i = 0; i < num_pad; ++i) {
      (*out) << "=";
    }
  }
  DCHECK_EQ(out->str().size() % 4, 0);
}

void Base64Encode(const vector<uint8_t>& in, string* out) {
  if (in.empty()) {
    *out = "";
  } else {
    std::ostringstream ss;
    Base64Encode(in, &ss);
    *out = ss.str();
  }
}

void Base64Encode(const vector<uint8_t>& in, std::ostringstream* out) {
  if (!in.empty()) {
    // Boost does not like non-null terminated strings
    string tmp(reinterpret_cast<const char*>(&in[0]), in.size());
    Base64Encode(tmp.c_str(), tmp.size(), out);
  }
}

void Base64Encode(const string& in, string* out) {
  std::ostringstream ss;
  Base64Encode(in.c_str(), in.size(), &ss);
  *out = ss.str();
}

void Base64Encode(const string& in, std::ostringstream* out) {
  Base64Encode(in.c_str(), in.size(), out);
}

bool Base64Decode(const string& in, string* out) {
  typedef transform_width<binary_from_base64<string::const_iterator>, 8, 6> base64_decode;

  if (in.empty()) {
    *out = "";
    return true;
  }

  // Boost's binary_from_base64 translates '=' symbols into null/zero bytes [1],
  // so the corresponding trailing null/zero extra bytes must be removed after
  // the transformation.
  //
  // [1] https://www.boost.org/doc/libs/1_87_0/boost/archive/iterators/binary_from_base64.hpp
  size_t padded_num = 0;
  auto padding_it = in.crbegin();
  for (; padding_it != in.crend(); ++padding_it) {
    if (*padding_it != '=') {
      break;
    }
    ++padded_num;
  }
  if (padded_num > 0) {
    // If padding is present, be strict on the length of the input
    // because of the following reasons, at least:
    //  * padding is to provide proper 'alignment' for 6-to-8 bit transcoding,
    //    and any length mismatch means corruption of the input data
    //  * trailing extra zero bytes are removed below with an assumption
    //    of proper 8-to-6 'alignment' to guarantee the consistency of output
    if (in.size() % 4 != 0) {
      return false;
    }
    if (padded_num > 2) {
      // Invalid base64-encoded sequence.
      return false;
    }
  }

  // It's not enforced in release builds for backward compatibility reasons,
  // but '=' symbols are expected to be present only in the end of the input
  // string. This implementation doesn't provide functionality to handle
  // a concatenation of base64-encoded sequences in a consistent manner.
  DCHECK(!std::any_of(padding_it, in.crend(), [](char c) { return c == '='; }))
      << "non-trailing '='";

  string tmp;
  try {
    tmp = string(base64_decode(in.begin()), base64_decode(in.end()));
  } catch (std::exception&) {
    return false;
  }
  DCHECK_GT(tmp.size(), padded_num);
#if DCHECK_IS_ON()
  for (auto it = tmp.crbegin(); it != tmp.crbegin() + padded_num; ++it) {
    // Make sure only null/zero bytes are removed when binary_from_base64 adds
    // those for the padding '=' symbols in the input.
    DCHECK_EQ('\0', *it);
  }
#endif
  // Remove trailing zero bytes produced by binary_from_base64 for the padding.
  tmp.erase(tmp.end() - padded_num, tmp.end());

  DCHECK(padded_num == 0 || (tmp.size() % 3 == 3 - padded_num));
  *out = std::move(tmp);
  return true;
}

void EscapeForHtml(const string& in, std::ostringstream* out) {
  DCHECK(out != nullptr);
  for (const char& c : in) {
    switch (c) {
      case '<': (*out) << "&lt;";
                break;
      case '>': (*out) << "&gt;";
                break;
      case '&': (*out) << "&amp;";
                break;
      default: (*out) << c;
    }
  }
}

std::string EscapeForHtmlToString(const std::string& in) {
  std::ostringstream str;
  EscapeForHtml(in, &str);
  return str.str();
}

} // namespace kudu
