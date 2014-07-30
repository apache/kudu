// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/util/env.h"

DEFINE_bool(print_meta, true, "print the header and footer from the file");
DEFINE_bool(iterate_rows, true, "iterate each row in the file");
DEFINE_bool(print_rows, true, "print each row in the file");
DEFINE_int32(num_iterations, 1, "number of times to iterate the file");

namespace kudu {
namespace cfile {

using std::string;
using std::cout;
using std::endl;

void DumpFile(const string &path) {
  Env *env = Env::Default();
  gscoped_ptr<CFileReader> reader;
  CHECK_OK(CFileReader::Open(env, path, ReaderOptions(), &reader));

  if (FLAGS_print_meta) {
    cout << "Header:\n" << reader->header().DebugString() << endl;
    cout << "Footer:\n" << reader->footer().DebugString() << endl;
  }

  if (FLAGS_iterate_rows) {
    gscoped_ptr<CFileIterator> it;
    CHECK_OK(reader->NewIterator(&it));

    DumpIteratorOptions opts;
    opts.print_rows = FLAGS_print_rows;
    for (int i = 0; i < FLAGS_num_iterations; i++) {
      CHECK_OK(it->SeekToFirst());
      CHECK_OK(DumpIterator(*reader, it.get(), &cout, opts));
    }
  }
}

} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    std::cerr << "usage: " << argv[0] << " <path>" << std::endl;
    return 1;
  }

  if (!FLAGS_iterate_rows) {
    FLAGS_print_rows = false;
  }

  kudu::cfile::DumpFile(argv[1]);

  return 0;
}
