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

#include "kudu/tools/tool_action_common.h"

#include <iostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_bool(print_meta, true, "Include metadata in output");
DEFINE_string(print_entries, "decoded",
              "How to print entries:\n"
              "  false|0|no = don't print\n"
              "  true|1|yes|decoded = print them decoded\n"
              "  pb = print the raw protobuf\n"
              "  id = print only their ids");
DEFINE_int32(truncate_data, 100,
             "Truncate the data fields to the given number of bytes "
             "before printing. Set to 0 to disable");

namespace kudu {
namespace tools {

using consensus::ReplicateMsg;
using log::LogEntryPB;
using log::LogEntryReader;
using log::ReadableLogSegment;
using rpc::RequestIdPB;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;
using tserver::WriteRequestPB;

namespace {

enum PrintEntryType {
  DONT_PRINT,
  PRINT_PB,
  PRINT_DECODED,
  PRINT_ID
};

PrintEntryType ParsePrintType() {
  if (ParseLeadingBoolValue(FLAGS_print_entries.c_str(), true) == false) {
    return DONT_PRINT;
  } else if (ParseLeadingBoolValue(FLAGS_print_entries.c_str(), false) == true ||
             FLAGS_print_entries == "decoded") {
    return PRINT_DECODED;
  } else if (FLAGS_print_entries == "pb") {
    return PRINT_PB;
  } else if (FLAGS_print_entries == "id") {
    return PRINT_ID;
  } else {
    LOG(FATAL) << "Unknown value for --print_entries: " << FLAGS_print_entries;
  }
}

void PrintIdOnly(const LogEntryPB& entry) {
  switch (entry.type()) {
    case log::REPLICATE:
    {
      cout << entry.replicate().id().term() << "." << entry.replicate().id().index()
           << "@" << entry.replicate().timestamp() << "\t";
      cout << "REPLICATE "
           << OperationType_Name(entry.replicate().op_type());
      break;
    }
    case log::COMMIT:
    {
      cout << "COMMIT " << entry.commit().commited_op_id().term()
           << "." << entry.commit().commited_op_id().index();
      break;
    }
    default:
      cout << "UNKNOWN: " << entry.ShortDebugString();
  }

  cout << endl;
}

Status PrintDecodedWriteRequestPB(const string& indent,
                                  const Schema& tablet_schema,
                                  const WriteRequestPB& write,
                                  const RequestIdPB* request_id) {
  Schema request_schema;
  RETURN_NOT_OK(SchemaFromPB(write.schema(), &request_schema));

  Arena arena(32 * 1024, 1024 * 1024);
  RowOperationsPBDecoder dec(&write.row_operations(), &request_schema, &tablet_schema, &arena);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(dec.DecodeOperations(&ops));

  cout << indent << "Tablet: " << write.tablet_id() << endl;
  cout << indent << "RequestId: "
      << (request_id ? request_id->ShortDebugString() : "None") << endl;
  cout << indent << "Consistency: "
       << ExternalConsistencyMode_Name(write.external_consistency_mode()) << endl;
  if (write.has_propagated_timestamp()) {
    cout << indent << "Propagated TS: " << write.propagated_timestamp() << endl;
  }

  int i = 0;
  for (const DecodedRowOperation& op : ops) {
    // TODO (KUDU-515): Handle the case when a tablet's schema changes
    // mid-segment.
    cout << indent << "op " << (i++) << ": " << op.ToString(tablet_schema) << endl;
  }

  return Status::OK();
}

Status PrintDecoded(const LogEntryPB& entry, const Schema& tablet_schema) {
  PrintIdOnly(entry);

  const string indent = "\t";
  if (entry.has_replicate()) {
    // We can actually decode REPLICATE messages.

    const ReplicateMsg& replicate = entry.replicate();
    if (replicate.op_type() == consensus::WRITE_OP) {
      RETURN_NOT_OK(PrintDecodedWriteRequestPB(
          indent,
          tablet_schema,
          replicate.write_request(),
          replicate.has_request_id() ? &replicate.request_id() : nullptr));
    } else {
      cout << indent << replicate.ShortDebugString() << endl;
    }
  } else if (entry.has_commit()) {
    // For COMMIT we'll just dump the PB
    cout << indent << entry.commit().ShortDebugString() << endl;
  }

  return Status::OK();
}

} // anonymous namespace

Status PrintSegment(const scoped_refptr<ReadableLogSegment>& segment) {
  PrintEntryType print_type = ParsePrintType();
  if (FLAGS_print_meta) {
    cout << "Header:\n" << segment->header().DebugString();
  }
  if (print_type != DONT_PRINT) {
    Schema tablet_schema;
    RETURN_NOT_OK(SchemaFromPB(segment->header().schema(), &tablet_schema));

    LogEntryReader reader(segment.get());
    LogEntryPB entry;
    while (true) {
      Status s = reader.ReadNextEntry(&entry);
      if (s.IsEndOfFile()) break;
      RETURN_NOT_OK(s);

      if (print_type == PRINT_PB) {
        if (FLAGS_truncate_data > 0) {
          pb_util::TruncateFields(&entry, FLAGS_truncate_data);
        }

        cout << "Entry:\n" << entry.DebugString();
      } else if (print_type == PRINT_DECODED) {
        RETURN_NOT_OK(PrintDecoded(entry, tablet_schema));
      } else if (print_type == PRINT_ID) {
        PrintIdOnly(entry);
      }
    }
  }
  if (FLAGS_print_meta && segment->HasFooter()) {
    cout << "Footer:\n" << segment->footer().DebugString();
  }

  return Status::OK();
}

} // namespace tools
} // namespace kudu
