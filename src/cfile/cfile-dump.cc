// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "cfile/cfile_reader.h"
#include "util/env.h"

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
  size_t type_size = type->size();
  int max_rows = kBufSize/type_size;
  ColumnBlock cb(*type, buf, type_size, max_rows, &arena);

  string strbuf;

  while (it->HasNext()) {
    size_t n = max_rows;
    CHECK_OK(it->CopyNextValues(&n, &cb));

    for (size_t i = 0; i < n; i++) {
      type->AppendDebugStringForValue(cb.cell_ptr(i), &strbuf);
      strbuf.push_back('\n');
    }
    arena.Reset();
    cout << strbuf;
    strbuf.clear();
  }

}

void DumpFile(const string &path) {
  Env *env = Env::Default();

  RandomAccessFile *raf;
  uint64_t size;
  CHECK_OK(env->NewRandomAccessFile(path, &raf));
  CHECK_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  CHECK_OK(reader.Init());

  scoped_ptr<CFileIterator> it;
  CHECK_OK(reader.NewIterator(&it));

  it->SeekToOrdinal(0);
  DumpIterator(reader, it.get());
}

}
}

int main(int argc, char **argv) {
  using namespace std;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    cerr << "usage: " << argv[0] << " <path>" << endl;
    return 1;
  }

  kudu::cfile::DumpFile(argv[1]);

  return 0;
}
