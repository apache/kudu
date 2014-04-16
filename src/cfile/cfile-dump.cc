// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "cfile/cfile_reader.h"
#include "util/env.h"

DEFINE_bool(print_meta, true, "print the header and footer from the file");
DEFINE_bool(iterate_rows, true, "iterate each row in the file");
DEFINE_bool(print_rows, true, "print each row in the file");
DEFINE_int32(num_iterations, 1, "number of times to iterate the file");

namespace kudu {
namespace cfile {

using std::string;
using std::cout;
using std::endl;

static const int kBufSize = 1024*1024;

void DumpIterator(const CFileReader &reader, CFileIterator *it) {
  Arena arena(8192, 8*1024*1024);
  uint8_t buf[kBufSize];
  const TypeInfo *type = reader.type_info();
  int max_rows = kBufSize/type->size();
  uint8_t nulls[BitmapSize(max_rows)];
  ColumnBlock cb(type, reader.is_nullable() ? nulls : NULL, buf, max_rows, &arena);

  string strbuf;
  uint64_t count = 0;
  while (it->HasNext()) {
    size_t n = max_rows;
    CHECK_OK(it->CopyNextValues(&n, &cb));

    if (FLAGS_print_rows) {
      if (reader.is_nullable()) {
        for (size_t i = 0; i < n; i++) {
          const void *ptr = cb.nullable_cell_ptr(i);
          if (ptr != NULL) {
            type->AppendDebugStringForValue(ptr, &strbuf);
          } else {
            strbuf.append("NULL");
          }
          strbuf.push_back('\n');
        }
      } else {
        for (size_t i = 0; i < n; i++) {
          type->AppendDebugStringForValue(cb.cell_ptr(i), &strbuf);
          strbuf.push_back('\n');
        }
      }
    }
    arena.Reset();
    cout << strbuf;
    strbuf.clear();
    count += n;
  }

  LOG(INFO) << "Dumped " << count << " rows";

}

void DumpFile(const string &path) {
  Env *env = Env::Default();
  gscoped_ptr<CFileReader> reader;
  CHECK_OK(CFileReader::Open(env, path, ReaderOptions(), &reader));

  Schema schema(boost::assign::list_of
                (ColumnSchema("col", reader->data_type())),
                1);

  if (FLAGS_print_meta) {
    cout << "Header:\n" << reader->header().DebugString() << endl;
    cout << "Footer:\n" << reader->footer().DebugString() << endl;
  }

  if (FLAGS_iterate_rows) {
    gscoped_ptr<CFileIterator> it;
    CHECK_OK(reader->NewIterator(&it));

    gscoped_ptr<EncodedKey> encoded_key;
    for (int i = 0; i < FLAGS_num_iterations; i++) {
      CHECK_OK(it->SeekToFirst());
      DumpIterator(*reader, it.get());
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
