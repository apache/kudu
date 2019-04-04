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

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class SchemaPB;

namespace security {
class TablePrivilegePB;
} // namespace security

namespace master {

// An interface for handling authorizations on Kudu operations.
class AuthzProvider {
 public:

  AuthzProvider();

  // Starts the AuthzProvider instance.
  virtual Status Start() = 0;

  // Stops the AuthzProvider instance.
  virtual void Stop() = 0;

  // Checks if the table creation is authorized for the given user.
  // If the table is being created with a different owner than the user,
  // then more strict privilege is required.
  //
  // If the operation is not authorized, returns Status::NotAuthorized().
  // Otherwise, may return other Status error codes depend on actual errors.
  virtual Status AuthorizeCreateTable(const std::string& table_name,
                                      const std::string& user,
                                      const std::string& owner) WARN_UNUSED_RESULT = 0;

  // Checks if the table deletion is authorized for the given user.
  //
  // If the operation is not authorized, returns Status::NotAuthorized().
  // Otherwise, may return other Status error codes depend on actual errors.
  virtual Status AuthorizeDropTable(const std::string& table_name,
                                    const std::string& user) WARN_UNUSED_RESULT = 0;

  // Checks if the table alteration is authorized for the given user.
  //
  // If the operation is not authorized, returns Status::NotAuthorized().
  // Otherwise, may return other Status error codes depend on actual errors.
  virtual Status AuthorizeAlterTable(const std::string& old_table,
                                     const std::string& new_table,
                                     const std::string& user) WARN_UNUSED_RESULT = 0;

  // Checks if retrieving metadata about the table is authorized for the
  // given user. For example, when checking for table presence or locations.
  //
  // If the operation is not authorized, returns Status::NotAuthorized().
  // Otherwise, may return other Status error codes depend on actual errors.
  virtual Status AuthorizeGetTableMetadata(const std::string& table_name,
                                           const std::string& user) WARN_UNUSED_RESULT = 0;

  // Populates the privilege fields of 'pb' with the table-specific privileges
  // for the given user, using 'schema_pb' for metadata (e.g. column IDs). This
  // does not populate the table ID field of 'pb' -- only the privilege fields;
  // as such, it is expected that the table ID field is already set.
  virtual Status FillTablePrivilegePB(const std::string& table_name,
                                      const std::string& user,
                                      const SchemaPB& schema_pb,
                                      security::TablePrivilegePB* pb) WARN_UNUSED_RESULT = 0;

  virtual ~AuthzProvider() {}

  // Checks if the given user is trusted and thus can be exempted from
  // authorization validation.
  bool IsTrustedUser(const std::string& user);

 private:
  std::unordered_set<std::string> trusted_users_;
};

} // namespace master
} // namespace kudu
