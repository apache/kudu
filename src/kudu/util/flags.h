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
#ifndef KUDU_UTIL_FLAGS_H
#define KUDU_UTIL_FLAGS_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "kudu/util/status.h"

namespace google {
  struct CommandLineFlagInfo;
}

namespace kudu {

// The umask of the process, set based on the --umask flag during
// HandleCommonFlags().
extern uint32_t g_parsed_umask;

// Looks for flags in argv and parses them.  Rearranges argv to put
// flags first, or removes them entirely if remove_flags is true.
// If a flag is defined more than once in the command line or flag
// file, the last definition is used.  Returns the index (into argv)
// of the first non-flag argument.
//
// This is a wrapper around google::ParseCommandLineFlags, but integrates
// with Kudu flag tags. For example, --helpxml will include the list of
// tags for each flag. This should be be used instead of
// google::ParseCommandLineFlags in any user-facing binary.
//
// See gflags.h for more information.
int ParseCommandLineFlags(int* argc, char*** argv, bool remove_flags);

// Handle common flags such as -version, -disable_core_dumps, etc.
// This includes the GFlags common flags such as "-help".
//
// Requires that flags have already been parsed using
// google::ParseCommandLineNonHelpFlags().
void HandleCommonFlags();

enum class EscapeMode {
  HTML,
  NONE
};

// Stick the flags into a string. If redaction is enabled, the values of
// flags tagged as sensitive will be redacted. Otherwise, the values
// will be written to the string as-is. The values will be HTML escaped
// if EscapeMode is HTML.
std::string CommandlineFlagsIntoString(EscapeMode mode);

typedef std::unordered_map<std::string, google::CommandLineFlagInfo> GFlagsMap;

// Get all the flags different from their defaults. The output is a nicely
// formatted string with --flag=value pairs per line. Redact any flags that
// are tagged as sensitive, if redaction is enabled.
std::string GetNonDefaultFlags(const GFlagsMap& default_flags);

GFlagsMap GetFlagsMap();

enum class TriStateFlag {
  DISABLED,
  OPTIONAL,
  REQUIRED,
};

Status ParseTriState(const char* flag_name, const std::string& flag_value,
    TriStateFlag* tri_state);

} // namespace kudu
#endif /* KUDU_UTIL_FLAGS_H */
