// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "consensus/log_reader.h"
#include "gutil/stl_util.h"
#include "util/env.h"
#include "util/pb_util.h"

DEFINE_bool(print_headers, true, "print the log segment headers");
DEFINE_bool(print_entries, true, "print all log entries");
DEFINE_int32(truncate_data, 100,
             "Truncate the data fields to the given number of bytes "
             "before printing. Set to 0 to disable");
namespace kudu {
namespace log {

using std::string;
using std::cout;
using std::endl;

void PrintSegment(LogReader* reader,
                  const shared_ptr<ReadableLogSegment>& segment) {
  if (FLAGS_print_headers) {
    cout << "Header:\n" << segment->header().DebugString();
  }
  vector<LogEntryPB*> entries;
  CHECK_OK(reader->ReadEntries(segment, &entries));
  if (FLAGS_print_entries) {
    BOOST_FOREACH(LogEntryPB* entry, entries) {
      if (FLAGS_truncate_data > 0) {
        pb_util::TruncateFields(entry, FLAGS_truncate_data);
      }
      cout << "Entry:\n" << entry->DebugString();
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
