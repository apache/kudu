// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <vector>

#include "common/row_operations.h"
#include "common/wire_protocol.h"
#include "consensus/consensus.pb.h"
#include "consensus/log_reader.h"
#include "gutil/stl_util.h"
#include "gutil/strings/numbers.h"
#include "util/env.h"
#include "util/pb_util.h"

DEFINE_bool(print_headers, true, "print the log segment headers");
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
namespace log {

using consensus::OperationPB;
using consensus::OperationType;
using consensus::ReplicateMsg;
using tserver::WriteRequestPB;
using std::string;
using std::vector;
using std::cout;
using std::endl;

enum PrintEntryType {
  DONT_PRINT,
  PRINT_PB,
  PRINT_DECODED,
  PRINT_ID
};

static PrintEntryType ParsePrintType() {
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
  cout << entry.operation().id().term() << "." << entry.operation().id().index() << "\t";
  if (entry.operation().has_commit()) {
    cout << "COMMIT " << entry.operation().commit().commited_op_id().term()
         << "." << entry.operation().commit().commited_op_id().index();
  } else if (entry.operation().has_replicate()) {
    cout << "REPLICATE "
         << OperationType_Name(entry.operation().replicate().op_type());
  } else {
    cout << "UNKNOWN";
  }
  cout << endl;
}

void PrintDecodedWriteRequestPB(const string& indent,
                                const WriteRequestPB& write) {
  Schema s;
  CHECK_OK(SchemaFromPB(write.schema(), &s));
  Arena arena(32 * 1024, 1024 * 1024);
  RowOperationsPBDecoder dec(&write.row_operations(), &s, &s, &arena);
  vector<DecodedRowOperation> ops;
  CHECK_OK(dec.DecodeOperations(&ops));

  cout << indent << "Tablet: " << write.tablet_id() << endl;
  cout << indent << "Consistency: "
       << ExternalConsistencyMode_Name(write.external_consistency_mode()) << endl;
  if (write.has_propagated_timestamp()) {
    cout << indent << "Propagated TS: " << write.propagated_timestamp() << endl;
  }

  int i = 0;
  BOOST_FOREACH(const DecodedRowOperation& op, ops) {
    cout << indent << "op " << (i++) << ": " << op.ToString(s) << endl;
  }
}

void PrintDecoded(const LogEntryPB& entry) {
  CHECK_EQ(entry.type(), log::OPERATION);

  PrintIdOnly(entry);

  const string indent = "\t";
  if (entry.operation().has_replicate()) {
    // We can actually decode REPLICATE messages.

    const ReplicateMsg& replicate = entry.operation().replicate();
    if (replicate.op_type() == consensus::WRITE_OP) {
      PrintDecodedWriteRequestPB(indent, replicate.write_request());
    } else {
      cout << indent << replicate.ShortDebugString() << endl;
    }
  } else if (entry.operation().has_commit()) {
    // For COMMIT we'll just dump the PB
    cout << indent << entry.operation().commit().ShortDebugString() << endl;
  }
}

void PrintSegment(LogReader* reader,
                  const shared_ptr<ReadableLogSegment>& segment) {
  PrintEntryType print_type = ParsePrintType();
  if (FLAGS_print_headers) {
    cout << "Header:\n" << segment->header().DebugString();
  }
  vector<LogEntryPB*> entries;
  CHECK_OK(reader->ReadEntries(segment, &entries));

  if (print_type == DONT_PRINT) return;

  BOOST_FOREACH(LogEntryPB* entry, entries) {

    if (print_type == PRINT_PB) {
      if (FLAGS_truncate_data > 0) {
        pb_util::TruncateFields(entry, FLAGS_truncate_data);
      }

      cout << "Entry:\n" << entry->DebugString();
    } else if (print_type == PRINT_DECODED) {
      PrintDecoded(*entry);
    } else if (print_type == PRINT_ID) {
      PrintIdOnly(*entry);
    }
  }
}

void DumpLog(const string &tserver_root_path, const string& tablet_oid) {
  Env *env = Env::Default();
  gscoped_ptr<LogReader> reader;
  FsManager fs_manager(env, tserver_root_path);
  CHECK_OK(LogReader::Open(&fs_manager, tablet_oid, &reader));

  vector<LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  BOOST_FOREACH(const shared_ptr<ReadableLogSegment>& segment, reader->segments()) {
    PrintSegment(reader.get(), segment);
  }
}

void DumpSegment(const string &segment_path) {
  Env *env = Env::Default();
  gscoped_ptr<LogReader> reader;
  shared_ptr<ReadableLogSegment> segment;
  CHECK_OK(LogReader::InitSegment(env, segment_path, &segment));
  CHECK(segment);
  PrintSegment(reader.get(), segment);
}

} // namespace log
} // namespace kudu

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc < 2 || argc > 3) {
    std::cerr << "usage: " << argv[0] << " <tserver root path> <tablet_name>"
        " | <log segment path> " << std::endl;
    return 1;
  }
  if (argc == 2) {
    kudu::log::DumpSegment(argv[1]);
  } else {
    kudu::log::DumpLog(argv[1], argv[2]);
  }

  return 0;
}
