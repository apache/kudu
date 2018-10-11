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

#include <string>

#include "kudu/master/authz_provider.h"
#include "kudu/util/status.h"

namespace kudu {
namespace master {

// Default AuthzProvider which always authorizes any operations.
class DefaultAuthzProvider : public AuthzProvider {
 public:

  Status Start() override WARN_UNUSED_RESULT { return Status::OK(); }

  void Stop() override {};

  Status AuthorizeCreateTable(const std::string& /*table_name*/,
                              const std::string& /*user*/,
                              const std::string& /*owner*/) override WARN_UNUSED_RESULT {
    return Status::OK();
  }

  Status AuthorizeDropTable(const std::string& /*table_name*/,
                            const std::string& /*user*/) override WARN_UNUSED_RESULT {
    return Status::OK();
  }

  Status AuthorizeAlterTable(const std::string& /*old_table*/,
                             const std::string& /*new_table*/,
                             const std::string& /*user*/) override WARN_UNUSED_RESULT {
    return Status::OK();
  }

  Status AuthorizeGetTableMetadata(const std::string& /*table_name*/,
                                   const std::string& /*user*/) override WARN_UNUSED_RESULT {
    return Status::OK();
  }
};

} // namespace master
} // namespace kudu
