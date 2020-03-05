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
//
// Utility methods for dealing with file paths.
#pragma once

#include <string>
#include <vector>

#include "kudu/gutil/port.h"

namespace kudu {

class Status;

// Common tmp infix
extern const char kTmpInfix[];
// Infix from versions of Kudu prior to 1.2.
extern const char kOldTmpInfix[];

// Join two path segments with the appropriate path separator,
// if necessary.
std::string JoinPathSegments(const std::string& a,
                             const std::string& b);

// Join each path segment in a list with a common suffix segment.
std::vector<std::string> JoinPathSegmentsV(const std::vector<std::string>& v,
                                           const std::string& s);

// Split a path into segments with the appropriate path separator.
std::vector<std::string> SplitPath(const std::string& path);

// Return the enclosing directory of path.
// This is like dirname(3) but for C++ strings.
std::string DirName(const std::string& path);

// Return the terminal component of a path.
// This is like basename(3) but for C++ strings.
std::string BaseName(const std::string& path);

// Attempts to find the path to the executable, searching the provided locations
// as well as the $PATH environment variable.
Status FindExecutable(const std::string& binary,
                      const std::vector<std::string>& search,
                      std::string* path) WARN_UNUSED_RESULT;

} // namespace kudu
