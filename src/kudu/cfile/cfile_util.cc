// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/cfile/cfile_util.h"

#include <glog/logging.h>
#include <algorithm>
#include <string>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/util/env.h"
#include "kudu/util/mem_tracker.h"

namespace kudu {
namespace cfile {

using std::string;

static const int kBufSize = 1024*1024;

Status DumpIterator(const CFileReader& reader,
                    CFileIterator* it,
                    std::ostream* out,
                    const DumpIteratorOptions& opts,
                    int indent) {

  Arena arena(8192, 8*1024*1024);
  uint8_t buf[kBufSize];
  const TypeInfo *type = reader.type_info();
  size_t max_rows = kBufSize/type->size();
  uint8_t nulls[BitmapSize(max_rows)];
  ColumnBlock cb(type, reader.is_nullable() ? nulls : NULL, buf, max_rows, &arena);

  string strbuf;
  uint64_t count = 0;
  while (it->HasNext()) {
    size_t n = opts.nrows == 0 ? max_rows : std::min(max_rows, opts.nrows - count);
    if (n == 0) break;

    RETURN_NOT_OK(it->CopyNextValues(&n, &cb));

    if (opts.print_rows) {
      if (reader.is_nullable()) {
        for (size_t i = 0; i < n; i++) {
          strbuf.append(indent, ' ');
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
          strbuf.append(indent, ' ');
          type->AppendDebugStringForValue(cb.cell_ptr(i), &strbuf);
          strbuf.push_back('\n');
        }
      }
      *out << strbuf;
      strbuf.clear();
    }
    arena.Reset();
    count += n;
  }

  VLOG(1) << "Dumped " << count << " rows";

  return Status::OK();
}

ReaderOptions::ReaderOptions()
  : parent_mem_tracker(MemTracker::GetRootTracker()) {
}

} // namespace cfile
} // namespace kudu
