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
#pragma once

#include <iosfwd>
#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

// A struct representing a parsed version. Versions are expected to look like
//
//  <major>.<minor>.<maintenance>[[.-]<extra>]
//
// e.g. 1.6.0, 1.7.1-SNAPSHOT, 1.8.0-RC1-SNAPSHOT, 1.11.0.7.0.0.0-SNAPSHOT, etc.
//
// This struct can be used with versions reported by ksck to determine if and
// how certain tools should function depending on what versions are running in
// the cluster.
struct Version {
  bool operator==(const Version& other) const;

  // Return 'canonical' version string, i.e. the concatenation of the version
  // components transformed back into the string representation. The parser
  // implementation has its quirks, so the canonical version string does not
  // always match the raw input string.
  std::string ToString() const;

  // The original version string.
  std::string raw_version;

  // The parsed version numbers.
  int major;
  int minor;
  int maintenance;

  // The character separating the maintenance version from the extra component.
  //
  // Even if there was a delimiter, this will be boost::none unless there was
  // a non-empty extra component.
  boost::optional<char> extra_delimiter;

  // The extra component. Empty if there was no extra component.
  std::string extra;
};

std::ostream& operator<<(std::ostream& os, const Version& v);

// Parse 'version_str' into 'v'. 'v' must not be null.
Status ParseVersion(const std::string& version_str,
                    Version* v) WARN_UNUSED_RESULT;

} // namespace kudu
