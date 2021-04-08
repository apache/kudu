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
// TODO(awong): add a state filter.

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::clock::HybridClock;
using kudu::transactions::TxnParticipantEntryPB;
using kudu::transactions::TxnStatusEntryPB;
using kudu::transactions::TxnStatusTablet;
using kudu::pb_util::SecureShortDebugString;
using std::cout;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

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

} // anonymous namespace

namespace kudu {
namespace tools {

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
      int8_t entry_type;
      RETURN_NOT_OK(iter.GetInt8(TxnStatusTablet::kEntryTypeColName, &entry_type));
      CHECK_EQ(TxnStatusTablet::TRANSACTION, entry_type);
      int64_t txn_id;
      Slice metadata_bytes;
      RETURN_NOT_OK(iter.GetInt64(TxnStatusTablet::kTxnIdColName, &txn_id));
      RETURN_NOT_OK(iter.GetString(TxnStatusTablet::kMetadataColName, &metadata_bytes));
      string metadata;
      TxnStatusEntryPB txn_entry_pb;
      RETURN_NOT_OK(pb_util::ParseFromArray(&txn_entry_pb, metadata_bytes.data(),
                                            metadata_bytes.size()));
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
      .Build();
  return ModeBuilder("txn")
      .Description("Operate on multi-row transactions")
      .AddAction(std::move(list))
      .Build();
}

} // namespace tools
} // namespace kudu
