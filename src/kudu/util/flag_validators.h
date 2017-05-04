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

#include "kudu/gutil/macros.h"

#include <functional>
#include <map>
#include <string>

namespace kudu {

// The validation function: takes no parameters and returns a boolean. A group
// validator should return 'true' if validation was successful, or 'false'
// otherwise.
typedef std::function<bool(void)> FlagValidator;

// The group validator registry's representation for as seen from the outside:
// the key is the name of the group validator, the value is the validation
// function.
typedef std::map<std::string, FlagValidator> FlagValidatorsMap;

// Register a 'group' validator for command-line flags. In contrast with the
// standard (built-in) gflag validators registered by the DEFINE_validator()
// macro, group validators are run at a later phase in the context of the main()
// function. A group validator has a guarantee that all command-line flags have
// been parsed, individually validated (via standard validators), and their
// values are already set at the time when the validator runs.
//
// The first macro parameter is the name of the validator, the second parameter
// is the validation function as is. The name must be unique across all
// registered group validators.
//
// The validation function takes no parameters and returns 'true' in case of
// successful validation, otherwise it returns 'false'. If at least one of the
// registered group validators returns 'false', exit(1) is called.
//
// Usage guideline:
//
//   * Use the DEFINE_validator() macro if you need to validate an individual
//     gflag's value
//
//   * Use the GROUP_FLAG_VALIDATOR() macro only if you need to validate a set
//     of gflag values against one another, having the guarantee that their
//     values are already set when the validation function runs.
//
// Sample usage:
//
//  static bool ValidateGroupedFlags() {
//    bool has_a = !FLAGS_a.empty();
//    bool has_b = !FLAGS_b.empty();
//
//    if (has_a != has_b) {
//      LOG(ERROR) << "--a and --b must be set as a group";
//      return false;
//    }
//
//    return true;
//  }
//  GROUP_FLAG_VALIDATOR(grouped_flags_validator, ValidateGroupedFlags);
//
#define GROUP_FLAG_VALIDATOR(name, func) \
  namespace {                                               \
    ::kudu::flag_validation_internal::Registrator v_##name( \
        AS_STRING(name), (func));                           \
  }

// Get all registered group flag validators.
const FlagValidatorsMap& GetFlagValidators();

namespace flag_validation_internal {

// This is a utility class which registers a group validator upon instantiation.
class Registrator {
 public:
  // The constructor registers a group validator with the specified name and
  // the given validation function. The name must be unique among all group
  // validators.
  Registrator(const char* name, const FlagValidator& validator);

 private:
  DISALLOW_COPY_AND_ASSIGN(Registrator);
};

} // namespace flag_validation_internal

} // namespace kudu
