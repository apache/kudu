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

#include "kudu/util/flag_validators.h"

#include <string>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/singleton.h"

using std::string;

namespace kudu {
namespace flag_validation_internal {

// A singleton registry for storing group flag validators.
class FlagValidatorRegistry {
 public:
  static FlagValidatorRegistry* GetInstance() {
    return Singleton<FlagValidatorRegistry>::get();
  }

  void Register(const string& name, const FlagValidator& func) {
    InsertOrDie(&validators_, name, func);
  }

  const FlagValidatorsMap& validators() {
    return validators_;
  }

 private:
  friend class Singleton<FlagValidatorRegistry>;
  FlagValidatorRegistry() {}

  FlagValidatorsMap validators_;

  DISALLOW_COPY_AND_ASSIGN(FlagValidatorRegistry);
};


Registrator::Registrator(const char* name, const FlagValidator& validator) {
  FlagValidatorRegistry::GetInstance()->Register(name, validator);
}

} // namespace flag_validation_internal


const FlagValidatorsMap& GetFlagValidators() {
  using flag_validation_internal::FlagValidatorRegistry;
  return FlagValidatorRegistry::GetInstance()->validators();
}

} // namespace kudu
