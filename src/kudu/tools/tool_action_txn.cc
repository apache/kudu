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

#include <sys/types.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/value.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_string(columns);

DEFINE_int64(min_txn_id, -1, "Inclusive minimum transaction ID to display, or -1 for no minimum.");
DEFINE_int64(max_txn_id, -1, "Inclusive maximum transaction ID to display, or -1 for no maximum.");
DEFINE_string(included_states, "open,abort_in_progress,commit_in_progress,finalize_in_progress",
              "Comma-separated list of transaction states to display. Supported states "
              "are 'open', 'abort_in_progress', 'aborted', 'commit_in_progress', "
              "'finalize_in_progress', and 'committed', or '*' for all states. By default, shows "
              "currently active transactions.");

DECLARE_int64(timeout_ms);
DECLARE_string(sasl_protocol_name);

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::clock::HybridClock;
using kudu::tablet::TxnMetadataPB;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnStatusEntryPB;
using kudu::transactions::TxnStatusTablet;
using kudu::transactions::TxnSystemClient;
using kudu::tserver::ParticipantOpPB;
using std::cout;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;
using strings::Split;

namespace kudu {
namespace tools {

namespace {

enum class ListTxnsField : int {
  kTxnId,
  kUser,
  kState,
  kCommitDateTime,
  kCommitHybridTime,
  kStartDatetime,
  kLastTransitionDatetime,
};

const unordered_map<string, ListTxnsField> kStrToTxnField {
  { "txn_id", ListTxnsField::kTxnId },
  { "user", ListTxnsField::kUser },
  { "state", ListTxnsField::kState },
  { "commit_datetime", ListTxnsField::kCommitDateTime },
  { "commit_hybridtime", ListTxnsField::kCommitHybridTime },
  { "start_datetime", ListTxnsField::kStartDatetime },
  { "last_transition_datetime", ListTxnsField::kLastTransitionDatetime },
};

enum class ParticipantField : int {
  kTabletId,
  kAborted,
  kFlushedCommittedMrs,
  kBeginCommitDateTime,
  kBeginCommitHybridTime,
  kCommitDateTime,
  kCommitHybridTime,
};

constexpr const char* kParticipantPrefix = "participant_";
const unordered_map<string, ParticipantField> kStrToParticipantFields {
  { "tablet_id", ParticipantField::kTabletId },
  { "aborted", ParticipantField::kAborted },
  { "flushed_committed_mrs", ParticipantField::kFlushedCommittedMrs },
  { "begin_commit_datetime", ParticipantField::kBeginCommitDateTime },
  { "begin_commit_hybridtime", ParticipantField::kBeginCommitHybridTime },
  { "commit_datetime", ParticipantField::kCommitDateTime },
  { "commit_hybridtime", ParticipantField::kCommitHybridTime },
};

constexpr const char* kTxnIdArg = "txn_id";
const unordered_map<string, TxnStatePB> kStrToState = {
  { "open", TxnStatePB::OPEN },
  { "abort_in_progress", TxnStatePB::ABORT_IN_PROGRESS },
  { "aborted", TxnStatePB::ABORTED },
  { "commit_in_progress", TxnStatePB::COMMIT_IN_PROGRESS },
  { "finalize_in_progress", TxnStatePB::FINALIZE_IN_PROGRESS },
  { "committed", TxnStatePB::COMMITTED },
};

unordered_set<TxnStatePB> kIncludedStates;
bool ValidateStatesList(const char* /*flag_name*/, const string& flag_val) {
  if (flag_val == "*") {
    for (const auto& [_, state] : kStrToState) {
      EmplaceOrDie(&kIncludedStates, state);
    }
    return true;
  }
  vector<string> included_states_list = Split(flag_val, ",", strings::SkipEmpty());
  unordered_set<TxnStatePB> included_states;
  for (auto state_lower : included_states_list) {
    ToLowerCase(&state_lower);
    auto* state = FindOrNull(kStrToState, state_lower);
    if (state) {
      EmplaceIfNotPresent(&included_states, *state);
    } else {
      LOG(ERROR) << Substitute("Unexpected state provided: $0", state_lower);
      return false;
    }
  }
  kIncludedStates = std::move(included_states);
  return true;
}
DEFINE_validator(included_states, &ValidateStatesList);

string HybridTimeToDateTime(int64_t ht) {
  const auto physical_micros = HybridClock::GetPhysicalValueMicros(Timestamp(ht));
  time_t physical_secs(physical_micros / 1000000);
  char buf[kFastToBufferSize];
  return FastTimeToBuffer(physical_secs, buf);
}

Status GetFields(vector<string>* txn_field_names, vector<ListTxnsField>* txn_fields,
                 vector<string>* participant_field_names = nullptr,
                 vector<ParticipantField>* participant_fields = nullptr) {
  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    string column_lower;
    ToLowerCase(column.ToString(), &column_lower);
    if (participant_field_names && participant_fields) {
      // If the field is prefixed with "participant_", get the suffix and look
      // through the participant fields.
      const char* participant_field_suffix =
          strnprefix(column_lower.c_str(), column_lower.size(),
                     kParticipantPrefix, strlen(kParticipantPrefix));
      if (participant_field_suffix) {
        auto* field = FindOrNull(kStrToParticipantFields, participant_field_suffix);
        if (field) {
          participant_fields->emplace_back(*field);
          participant_field_names->emplace_back(participant_field_suffix);
          continue;
        }
      }
    }
    // Otherwise, check the transaction fields.
    auto* field = FindOrNull(kStrToTxnField, column_lower);
    if (!field) {
      return Status::InvalidArgument("unknown column specified in --columns", column);
    }
    txn_fields->emplace_back(*field);
    txn_field_names->emplace_back(std::move(column_lower));
  }
  return Status::OK();
}

void AddTxnStatusRow(const vector<ListTxnsField>& fields, int64_t txn_id,
                     const TxnStatusEntryPB& txn_entry_pb, DataTable* data_table) {
  string commit_ts_ht_str = "<none>";
  string commit_ts_date_str = "<none>";
  vector<string> col_vals;
  for (const auto& field : fields) {
    switch (field) {
      case ListTxnsField::kTxnId:
        col_vals.emplace_back(SimpleItoa(txn_id));
        break;
      case ListTxnsField::kUser:
        col_vals.emplace_back(txn_entry_pb.user());
        break;
      case ListTxnsField::kState:
        col_vals.emplace_back(TxnStatePB_Name(txn_entry_pb.state()));
        break;
      case ListTxnsField::kCommitDateTime:
        col_vals.emplace_back(txn_entry_pb.has_commit_timestamp() ?
            HybridTimeToDateTime(txn_entry_pb.commit_timestamp()) : "<none>");
        break;
      case ListTxnsField::kCommitHybridTime:
        col_vals.emplace_back(txn_entry_pb.has_commit_timestamp() ?
            HybridClock::StringifyTimestamp(Timestamp(txn_entry_pb.commit_timestamp())) : "<none>");
        break;
      case ListTxnsField::kStartDatetime: {
        char buf[kFastToBufferSize];
        col_vals.emplace_back(txn_entry_pb.has_start_timestamp() ?
            FastTimeToBuffer(txn_entry_pb.start_timestamp(), buf) : "<none>");
        break;
      }
      case ListTxnsField::kLastTransitionDatetime: {
        char buf[kFastToBufferSize];
        col_vals.emplace_back(txn_entry_pb.has_last_transition_timestamp() ?
            FastTimeToBuffer(txn_entry_pb.last_transition_timestamp(), buf) : "<none>");
        break;
      }
    }
  }
  data_table->AddRow(std::move(col_vals));
}

} // anonymous namespace

Status ListTxns(const RunnerContext& context) {
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(TxnStatusTablet::kTxnStatusTableName, &table));

  vector<string> field_names;
  vector<ListTxnsField> fields;
  RETURN_NOT_OK(GetFields(&field_names, &fields));

  KuduScanner scanner(table.get());
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  auto* pred = table->NewComparisonPredicate(
      TxnStatusTablet::kEntryTypeColName,
      KuduPredicate::EQUAL, KuduValue::FromInt(TxnStatusTablet::TRANSACTION));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(pred));
  if (FLAGS_min_txn_id > 0) {
    auto* min_pred = table->NewComparisonPredicate(
        TxnStatusTablet::kTxnIdColName,
        KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(FLAGS_min_txn_id));
    RETURN_NOT_OK(scanner.AddConjunctPredicate(min_pred));
  }
  if (FLAGS_max_txn_id > 0) {
    auto* max_pred = table->NewComparisonPredicate(
        TxnStatusTablet::kTxnIdColName,
        KuduPredicate::LESS_EQUAL, KuduValue::FromInt(FLAGS_max_txn_id));
    RETURN_NOT_OK(scanner.AddConjunctPredicate(max_pred));
  }
  RETURN_NOT_OK(scanner.Open());

  DataTable data_table(field_names);
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (const auto& iter : batch) {
      Slice metadata_bytes;
      string metadata;
      RETURN_NOT_OK(iter.GetString(TxnStatusTablet::kMetadataColName, &metadata_bytes));
      TxnStatusEntryPB txn_entry_pb;
      RETURN_NOT_OK(pb_util::ParseFromArray(&txn_entry_pb, metadata_bytes.data(),
                                            metadata_bytes.size()));
      if (!ContainsKey(kIncludedStates, txn_entry_pb.state())) {
        continue;
      }
      int8_t entry_type;
      int64_t txn_id;
      RETURN_NOT_OK(iter.GetInt8(TxnStatusTablet::kEntryTypeColName, &entry_type));
      CHECK_EQ(TxnStatusTablet::TRANSACTION, entry_type);
      RETURN_NOT_OK(iter.GetInt64(TxnStatusTablet::kTxnIdColName, &txn_id));
      AddTxnStatusRow(fields, txn_id, txn_entry_pb, &data_table);
    }
  }
  return data_table.PrintTo(std::cout);
}

Status ShowTxn(const RunnerContext& context) {
  const auto& txn_id_str = FindOrDie(context.required_args, kTxnIdArg);
  int64_t txn_id = -1;
  if (!SimpleAtoi(txn_id_str, &txn_id) || txn_id < 0) {
    return Status::InvalidArgument(
        Substitute("Must supply a valid transaction ID: $0", txn_id_str));
  }
  vector<string> txn_field_names;
  vector<ListTxnsField> txn_fields;
  vector<string> participant_field_names;
  vector<ParticipantField> participant_fields;
  RETURN_NOT_OK(GetFields(&txn_field_names, &txn_fields,
                          &participant_field_names, &participant_fields));
  DCHECK_EQ(txn_field_names.size(), txn_fields.size());
  DCHECK_EQ(participant_field_names.size(), participant_fields.size());

  // First set up our clients so we can be sure we can connect to the cluster.
  std::unique_ptr<TxnSystemClient> txn_client;
  vector<HostPort> master_hps;
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, kMasterAddressesArg, &master_addresses));
  for (const auto& m : master_addresses) {
    HostPort hp;
    hp.ParseString(m, master::Master::kDefaultPort);
    master_hps.emplace_back(hp);
  }
  RETURN_NOT_OK(TxnSystemClient::Create(master_hps,
                                        FLAGS_sasl_protocol_name,
                                        &txn_client));
  RETURN_NOT_OK(txn_client->OpenTxnStatusTable());
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  // Scan the transaction status table for its table entries.
  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(TxnStatusTablet::kTxnStatusTableName, &table));
  KuduScanner scanner(table.get());
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  auto* pred = table->NewComparisonPredicate(
      TxnStatusTablet::kTxnIdColName, KuduPredicate::EQUAL, KuduValue::FromInt(txn_id));
  RETURN_NOT_OK(scanner.AddConjunctPredicate(pred));
  RETURN_NOT_OK(scanner.Open());
  KuduScanBatch batch;

  // Extract transaction statuses and participant IDs from the results.
  vector<string> participant_ids;
  DataTable txn_table(txn_field_names);
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (const auto& iter : batch) {
      Slice metadata_bytes;
      string metadata;
      RETURN_NOT_OK(iter.GetString(TxnStatusTablet::kMetadataColName, &metadata_bytes));
      int8_t entry_type;
      int64_t fetched_txn_id;
      RETURN_NOT_OK(iter.GetInt8(TxnStatusTablet::kEntryTypeColName, &entry_type));
      if (entry_type == TxnStatusTablet::TRANSACTION && !txn_fields.empty()) {
        TxnStatusEntryPB txn_entry_pb;
        RETURN_NOT_OK(pb_util::ParseFromArray(&txn_entry_pb, metadata_bytes.data(),
                                              metadata_bytes.size()));
        RETURN_NOT_OK(iter.GetInt64(TxnStatusTablet::kTxnIdColName, &fetched_txn_id));
        DCHECK_EQ(txn_id, fetched_txn_id);
        AddTxnStatusRow(txn_fields, fetched_txn_id, txn_entry_pb, &txn_table);
      } else if (entry_type == TxnStatusTablet::PARTICIPANT && !participant_fields.empty()) {
        Slice participant_id;
        RETURN_NOT_OK(iter.GetString(TxnStatusTablet::kIdentifierColName, &participant_id));
        participant_ids.emplace_back(participant_id.ToString());
      }
    }
  }
  if (!txn_fields.empty()) {
    RETURN_NOT_OK(txn_table.PrintTo(std::cout));
    std::cout << std::endl;
  }
  if (participant_field_names.empty()) {
    return Status::OK();
  }

  // Get further details from the participants.
  DataTable participant_table(participant_field_names);
  for (const auto& id : participant_ids) {
    TxnMetadataPB meta_pb;
    ParticipantOpPB pb;
    pb.set_txn_id(txn_id);
    pb.set_type(ParticipantOpPB::GET_METADATA);
    RETURN_NOT_OK(txn_client->ParticipateInTransaction(
        id, pb, MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_timeout_ms),
        /*begin_commit_timestamp=*/nullptr, &meta_pb));
    vector<string> col_vals;
    for (const auto& field : participant_fields) {
      switch (field) {
        case ParticipantField::kTabletId:
          col_vals.emplace_back(id);
          break;
        case ParticipantField::kAborted:
          col_vals.emplace_back(meta_pb.aborted() ? "true" : "false");
          break;
        case ParticipantField::kFlushedCommittedMrs:
          col_vals.emplace_back(meta_pb.flushed_committed_mrs() ? "true" : "false");
          break;
        case ParticipantField::kBeginCommitDateTime:
          col_vals.emplace_back(meta_pb.has_commit_mvcc_op_timestamp() ?
              HybridTimeToDateTime(meta_pb.commit_mvcc_op_timestamp()) : "<none>");
          break;
        case ParticipantField::kBeginCommitHybridTime:
          col_vals.emplace_back(meta_pb.has_commit_mvcc_op_timestamp() ?
              HybridClock::StringifyTimestamp(Timestamp(meta_pb.commit_mvcc_op_timestamp())) :
              "<none>");
          break;
        case ParticipantField::kCommitDateTime:
          col_vals.emplace_back(meta_pb.has_commit_timestamp() ?
              HybridTimeToDateTime(meta_pb.commit_timestamp()) : "<none>");
          break;
        case ParticipantField::kCommitHybridTime:
          col_vals.emplace_back(meta_pb.has_commit_timestamp() ?
              HybridClock::StringifyTimestamp(Timestamp(meta_pb.commit_timestamp())) : "<none>");
          break;
      }
    }
    participant_table.AddRow(std::move(col_vals));
  }
  return participant_table.PrintTo(std::cout);
}

unique_ptr<Mode> BuildTxnMode() {
  unique_ptr<Action> list =
      ClusterActionBuilder("list", &ListTxns)
      .Description("Show details of multi-row transactions in the cluster")
      .AddOptionalParameter(
          "columns",
          string("txn_id,user,state,commit_datetime,start_datetime,last_transition_datetime"),
          string("Comma-separated list of transaction-related info fields to include "
                 "in the output.\nPossible values: txn_id, user, state, commit_datetime, "
                 "commit_hybridtime, start_datetime, last_transition_datetime"))
      .AddOptionalParameter("max_txn_id")
      .AddOptionalParameter("min_txn_id")
      .AddOptionalParameter("included_states")
      .Build();

  // TODO(awong): extend the GET_METADATA call for participants to also return
  // table ID, partition info, etc. so we can display it.
  unique_ptr<Action> show =
      ClusterActionBuilder("show", &ShowTxn)
      .AddRequiredParameter({ kTxnIdArg, "Transaction ID on which to operate" })
      .AddOptionalParameter(
          "columns",
          string("txn_id,user,state,commit_datetime,start_datetime,last_transition_datetime,"
                 "participant_tablet_id,participant_begin_commit_datetime,"
                 "participant_commit_datetime"),
          string("Comma-separated list of transaction-related info fields to include "
                 "in the output.\nPossible values: txn_id, user, state, commit_datetime, "
                 "commit_hybridtime, start_datetime, last_transition_datetime, "
                 "participant_tablet_id, participant_is_aborted, "
                 "participant_flushed_committed_mrs, participant_begin_commit_datetime, "
                 "participant_begin_commit_hybridtime, participant_commit_datetime, "
                 "participant_commit_hybridtime"))
      .AddOptionalParameter("timeout_ms")
      .Description("Show details of a specific transaction")
      .Build();

  return ModeBuilder("txn")
      .Description("Operate on multi-row transactions")
      .AddAction(std::move(list))
      .AddAction(std::move(show))
      .Build();
}

} // namespace tools
} // namespace kudu
