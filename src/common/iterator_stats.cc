// Copyright (c) 2014, Cloudera, inc.

#include "common/iterator_stats.h"

#include "gutil/stringprintf.h"
#include "util/status.h"

namespace kudu {

using std::string;

IteratorStats::IteratorStats()
    : data_blocks_read_from_disk(0),
      rows_read_from_disk(0) {
}

string IteratorStats::ToString() const {
  return StringPrintf("data_blocks_read_from_disk=%d rows_read_from_disk=%ld",
                      data_blocks_read_from_disk, rows_read_from_disk);
}

void IteratorStats::AddStats(const IteratorStats& other) {
  data_blocks_read_from_disk += other.data_blocks_read_from_disk;
  rows_read_from_disk += other.rows_read_from_disk;
}

} // namespace kudu
