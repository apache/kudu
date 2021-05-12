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
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_string(columns);

DEFINE_bool(show_hybrid_timestamps, false,
            "Whether to show commit timestamps as hybrid timestamps in "
            "addition to datetimes.");
DEFINE_int64(min_txn_id, -1, "Inclusive minimum transaction ID to display, or -1 for no minimum.");
DEFINE_int64(max_txn_id, -1, "Inclusive maximum transaction ID to display, or -1 for no maximum.");
DEFINE_string(included_states, "open,abort_in_progress,commit_in_progress,finalize_in_progress",
              "Comma-separated list of transaction states to display. Supported states "
              "are 'open', 'abort_in_progress', 'aborted', 'commit_in_progress', "
              "'finalize_in_progress', and 'committed', or '*' for all states. By default, shows "
              "currently active transactions.");

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::clock::HybridClock;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnStatusEntryPB;
using kudu::transactions::TxnStatusTablet;
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

enum ListTxnsField : int {
  kTxnId,
  kUser,
  kState,
  kCommitDateTime,
  kCommitHybridTime,
};

const unordered_map<string, ListTxnsField> kStrToListField {
  { "txn_id", kTxnId },
  { "user", kUser },
  { "state", kState },
  { "commit_datetime", kCommitDateTime },
  { "commit_hybridtime", kCommitHybridTime },
};

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

} // anonymous namespace

Status ListTxns(const RunnerContext& context) {
  vector<ListTxnsField> fields;
  vector<string> col_names;
  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    string column_lower;
    ToLowerCase(column.ToString(), &column_lower);
    auto* field = FindOrNull(kStrToListField, column_lower);
    if (!field) {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }
    fields.emplace_back(*field);
    col_names.emplace_back(std::move(column_lower));
  }

  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(TxnStatusTablet::kTxnStatusTableName, &table));

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

  DataTable data_table(col_names);
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
      string commit_ts_ht_str = "<none>";
      string commit_ts_date_str = "<none>";
      vector<string> col_vals;
      for (const auto& field : fields) {
        switch (field) {
          case kTxnId:
            col_vals.emplace_back(SimpleItoa(txn_id));
            break;
          case kUser:
            col_vals.emplace_back(txn_entry_pb.user());
            break;
          case kState:
            col_vals.emplace_back(TxnStatePB_Name(txn_entry_pb.state()));
            break;
          case kCommitDateTime: {
            if (txn_entry_pb.has_commit_timestamp()) {
              const auto commit_ts = Timestamp(txn_entry_pb.commit_timestamp());
              auto physical_micros = HybridClock::GetPhysicalValueMicros(commit_ts);
              time_t physical_secs(physical_micros / 1000000);
              char buf[kFastToBufferSize];
              commit_ts_date_str = FastTimeToBuffer(physical_secs, buf);
            }
            col_vals.emplace_back(commit_ts_date_str);
            break;
          }
          case kCommitHybridTime:
            if (txn_entry_pb.has_commit_timestamp()) {
              const auto commit_ts = Timestamp(txn_entry_pb.commit_timestamp());
              commit_ts_ht_str = HybridClock::StringifyTimestamp(commit_ts);
            }
            col_vals.emplace_back(commit_ts_ht_str);
            break;
        }
      }
      data_table.AddRow(col_vals);
    }
  }
  return data_table.PrintTo(std::cout);
}

unique_ptr<Mode> BuildTxnMode() {
  unique_ptr<Action> list =
      ClusterActionBuilder("list", &ListTxns)
      .Description("Show details of multi-row transactions in the cluster")
      .AddOptionalParameter(
          "columns",
          string("txn_id,user,state,commit_datetime"),
          string("Comma-separated list of transaction-related info fields to include "
                 "in the output.\nPossible values: txn_id, user, state, commit_datetime, "
                 "commit_hybridtime"))
      .AddOptionalParameter("max_txn_id")
      .AddOptionalParameter("min_txn_id")
      .AddOptionalParameter("included_states")
      .Build();
  return ModeBuilder("txn")
      .Description("Operate on multi-row transactions")
      .AddAction(std::move(list))
      .Build();
}

} // namespace tools
} // namespace kudu
