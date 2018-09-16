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

#include <mutex>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <regex.h>

// Wrapper around regex_t to get the regex matches from a string.
class KuduRegex {
 public:
  // A regex for the given pattern that is expected to have 'num_matches'
  // matches.
  KuduRegex(const char* pattern, size_t num_matches)
      : num_matches_(num_matches) {
    CHECK_EQ(0, regcomp(&re_, pattern, REG_EXTENDED));
  }

  bool Match(const std::string& in, std::vector<std::string>* matches) {
    regmatch_t match[num_matches_ + 1];
    if (regexec(&re_, in.c_str(), num_matches_ + 1, match, 0) != 0) {
      return false;
    }
    for (size_t i = 1; i < num_matches_ + 1; ++i) {
      if (match[i].rm_so == -1) {
        // An empty string for each optional regex component.
        matches->emplace_back("");
      } else {
        matches->emplace_back(in.substr(match[i].rm_so,
                                        match[i].rm_eo - match[i].rm_so));
      }
    }
    return true;
  }

 private:
   const size_t num_matches_;
   regex_t re_;
};
