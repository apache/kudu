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

#include "kudu/transactions/txn_status_entry.h"

#include <mutex>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"

using std::string;
using std::vector;

namespace kudu {
namespace transactions {

TransactionEntry::TransactionEntry(int64_t txn_id, std::string user)
    : txn_id_(txn_id),
      user_(std::move(user)),
      last_heartbeat_time_(MonoTime::Now()) {
}

scoped_refptr<ParticipantEntry> TransactionEntry::GetOrCreateParticipant(
    const string& tablet_id) {
  DCHECK(metadata_.IsReadLocked());

  // In the expected case, this participant hasn't been added; add it.
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<ParticipantEntry> participant = FindPtrOrNull(participants_, tablet_id);
  if (PREDICT_TRUE(!participant)) {
    participant = new ParticipantEntry();
    EmplaceOrDie(&participants_, tablet_id, participant);
  }
  return participant;
}

vector<string> TransactionEntry::GetParticipantIds() const {
  std::lock_guard<simple_spinlock> l(lock_);
  vector<string> ret;
  AppendKeysFromMap(participants_, &ret);
  return ret;
}

TxnStatePB TransactionEntry::state() const {
  CowLock<PersistentTransactionEntry> l(&metadata_, LockMode::READ);
  return l.data().pb.state();
}

} // namespace transactions
} // namespace kudu
