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
#include <utility>
#include <vector>

#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;

namespace tablet {
class TabletReplica;
}  // namespace tablet

namespace tserver {
class WriteRequestPB;
} // namespace tserver

namespace transactions {

typedef std::pair<std::string, TxnParticipantEntryPB> ParticipantIdAndPB;
// Encapsulates the reading of state of the transaction status tablet.
// Different implementations of visitors may store the contents of the
// transactions differently in-memory.
class TransactionsVisitor {
 public:
  // Signal to the visitor that a transaction exists with the given transaction
  // ID and status, with the given group of participants.
  virtual void VisitTransactionEntries(int64_t txn_id, TxnStatusEntryPB status_entry_pb,
                                       std::vector<ParticipantIdAndPB> participants) = 0;
};

// TxnStatusTablet is a partition of a Kudu table that keeps track of
// transaction-related system information like the statuses and IDs of
// transactions and participants.
//
// It has the schema:
//
//   (txn_id INT64, entry_type INT8, identifier STRING) -> metadata STRING
//
// - txn_id: the transaction ID associated with the entry. It is the first part
//   of the compound key so iteration over the table entries yields all state
//   relevant to a given transaction ID at once.
// - entry_type: a TxnStatusEntryType that indicates whether the entry is a
//   transaction entry, participant entry, etc.
// - identifier: an extra identifier record that is used differently by
//   different kinds of entries. For participant entries, this is the
//   participant's tablet ID.
// - metadata: a protobuf message whose type depends on the entry_type.
//
// While the methods of this class are thread-safe, it is up to the caller
// to enforce any desired consistency constraints. E.g. if requested, the
// TxnStatusTablet will happily write entries indicating the presence of
// participants without there being a corresponding transaction status entry.
// If that behavior is not desirable, callers should coordinate calls to the
// TxnStatusTablet to avoid it.
//
// Expected usage of this class is to have a management layer that reads and
// writes to the underlying replica only if it is leader.
//
// TODO(awong): ensure that only the leader TxnStatusManager can call these.
// TODO(awong): delete transactions that are entirely aborted or committed.
// TODO(awong): consider batching writes.
class TxnStatusTablet {
 public:
  enum TxnStatusEntryType {
    TRANSACTION = 1,
    PARTICIPANT = 2,
  };
  explicit TxnStatusTablet(tablet::TabletReplica* tablet_replica);

  // Returns the schema of the transactions status table.
  static const Schema& GetSchemaWithoutIds();

  // Uses the given visitor to iterate over the entries in the rows of the
  // underlying tablet replica. This allows the visitor to load the on-disk
  // contents of the tablet into a more usable memory representation.
  Status VisitTransactions(TransactionsVisitor* visitor);

  // Writes to the underlying storage. Returns an error if there was an error
  // writing the new entry.
  Status AddNewTransaction(int64_t txn_id, const std::string& user);
  Status UpdateTransaction(int64_t txn_id, const TxnStatusEntryPB& pb);
  Status AddNewParticipant(int64_t txn_id, const std::string& tablet_id);
  Status UpdateParticipant(int64_t txn_id, const std::string& tablet_id,
                           const TxnParticipantEntryPB& pb);

 private:
  // Writes 'req' to the underlying tablet replica, returning an error if there
  // was a problem replicating the request, or if there were any row errors.
  Status SyncWrite(const tserver::WriteRequestPB& req);

  // The tablet replica that backs this transaction status tablet.
  tablet::TabletReplica* tablet_replica_;
};

} // namespace transactions
} // namespace kudu
