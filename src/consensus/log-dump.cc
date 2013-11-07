// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "consensus/log_reader.h"
#include "gutil/stl_util.h"
#include "util/env.h"

DEFINE_bool(print_headers, true, "print the log segment headers");
DEFINE_bool(print_entries, true, "print all log entries");

namespace kudu {
namespace log {

using std::string;
using std::cout;
using std::endl;

void DumpLog(const string &tserver_root_path, const string& tablet_oid) {
  Env *env = Env::Default();
  gscoped_ptr<LogReader> reader;
  FsManager fs_manager(env, tserver_root_path);
  CHECK_OK(LogReader::Open(&fs_manager, tablet_oid, &reader));

  vector<LogEntry*> entries;
  ElementDeleter deleter(&entries);
  BOOST_FOREACH(const shared_ptr<ReadableLogSegment>& segment, reader->segments()) {
    cout << "Reading Segment: " << segment->path() << std::endl;
    if (FLAGS_print_headers) {
      cout << "Header:\n" << segment->header().DebugString();
    }
    entries.clear();
    CHECK_OK(reader->ReadEntries(segment, &entries));
    if (FLAGS_print_entries) {
      BOOST_FOREACH(const LogEntry* entry, entries) {
        cout << "Entry:\n" << entry->DebugString();
      }
    }
  }
}

} // namespace log
} // namespace kudu

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    std::cerr << "usage: " << argv[0] << " <tserver root path> <tablet_name>" << std::endl;
    return 1;
  }
  kudu::log::DumpLog(argv[1], argv[2]);

  return 0;
}
