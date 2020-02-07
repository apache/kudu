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

#include <cstdint>
#include <string>

#include <krb5/krb5.h>

#include "kudu/util/status.h"

namespace kudu {
namespace security {

class KinitContext {
 public:
  KinitContext();
  ~KinitContext();

  // Equivalent implementation of 'kinit -kt <keytab path> <principal>'.
  //
  // This logs in from the given keytab as the given principal, returning
  // RuntimeError if any part of this process fails.
  //
  // If the log-in is successful, then the default ticket cache is overwritten
  // with the credentials of the newly logged-in principal.
  Status Kinit(const std::string& keytab_path, const std::string& principal);

  // Acquires a new Ticket Granting Ticket (TGT).
  //
  // Renews the existing ticket if possible, or acquires a new Ticket Granting
  // Ticket (TGT).
  Status DoRenewal();

  // Calculates the next sleep interval based on the 'ticket_end_timestamp_' and
  // adds some jitter so that all the nodes do not hit the KDC at the same time.
  //
  // If 'num_retries' > 0, it calls GetBackedOffRenewInterval() to return a backed
  // off interval.
  int32_t GetNextRenewInterval(uint32_t num_retries);

  // Returns a value based on 'time_remaining' that increases exponentially with
  // 'num_retries', with a random jitter of +/- 0%-50% of that value.
  static int32_t GetBackedOffRenewInterval(int32_t time_remaining, uint32_t num_retries);

  const std::string& principal_str() const { return principal_str_; }
  const std::string& username_str() const { return username_str_; }

 private:
  Status KinitInternal();

  // Helper for DoRenewal() that tries to do a renewal. On success, returns OK and sets
  // *found_in_cache = true. If there is an error doing the renewal itself, returns an
  // error. If the TGT to be renewed was not found in the cache, return OK and set
  // *found_in_cache = false.
  Status DoRenewalInternal(bool* found_in_cache);

  krb5_principal principal_ = nullptr;
  krb5_keytab keytab_ = nullptr;
  krb5_ccache ccache_ = nullptr;
  krb5_get_init_creds_opt* opts_ = nullptr;

  // The stringified principal and username that we are logged in as.
  std::string principal_str_, username_str_;

  // This is the time that the current TGT in use expires.
  int32_t ticket_end_timestamp_;
};

} // namespace security
} // namespace kudu
