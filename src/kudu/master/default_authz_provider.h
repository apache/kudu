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
#include <unordered_set>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/master/authz_provider.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class SchemaPB;

namespace master {

// Default AuthzProvider which always authorizes any operations.
class DefaultAuthzProvider : public AuthzProvider {
 public:
  Status Start() override WARN_UNUSED_RESULT { return Status::OK(); }

  void Stop() override {}

  Status ResetCache() override {
    return Status::NotSupported("provider does not have privileges cache");
  }

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

  Status AuthorizeListTables(const std::string& /*user*/,
                             std::unordered_set<std::string>* /*table_names*/,
                             bool* checked_table_names) override WARN_UNUSED_RESULT {
    *checked_table_names = false;
    return Status::OK();
  }

  Status AuthorizeGetTableStatistics(const std::string& /*table_name*/,
                                     const std::string& /*user*/) override WARN_UNUSED_RESULT {
    return Status::OK();
  }

  Status FillTablePrivilegePB(const std::string& /*table_name*/,
                              const std::string& /*user*/,
                              const SchemaPB& /*schema_pb*/,
                              security::TablePrivilegePB* pb) override WARN_UNUSED_RESULT {
    DCHECK(pb);
    DCHECK(pb->has_table_id());
    pb->set_delete_privilege(true);
    pb->set_insert_privilege(true);
    pb->set_scan_privilege(true);
    pb->set_update_privilege(true);
    return Status::OK();
  }
};

} // namespace master
} // namespace kudu
