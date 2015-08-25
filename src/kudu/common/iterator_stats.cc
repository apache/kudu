// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/common/iterator_stats.h"

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {

using std::string;
using strings::Substitute;

IteratorStats::IteratorStats()
    : data_blocks_read_from_disk(0),
      bytes_read_from_disk(0),
      cells_read_from_disk(0) {
}

string IteratorStats::ToString() const {
  return Substitute("data_blocks_read_from_disk=$0 "
                    "bytes_read_from_disk=$1 "
                    "cells_read_from_disk=$2",
                    data_blocks_read_from_disk,
                    bytes_read_from_disk,
                    cells_read_from_disk);
}

void IteratorStats::AddStats(const IteratorStats& other) {
  data_blocks_read_from_disk += other.data_blocks_read_from_disk;
  bytes_read_from_disk += other.bytes_read_from_disk;
  cells_read_from_disk += other.cells_read_from_disk;
  DCheckNonNegative();
}

void IteratorStats::SubtractStats(const IteratorStats& other) {
  data_blocks_read_from_disk -= other.data_blocks_read_from_disk;
  bytes_read_from_disk -= other.bytes_read_from_disk;
  cells_read_from_disk -= other.cells_read_from_disk;
  DCheckNonNegative();
}

void IteratorStats::DCheckNonNegative() const {
  DCHECK_GE(data_blocks_read_from_disk, 0);
  DCHECK_GE(bytes_read_from_disk, 0);
  DCHECK_GE(cells_read_from_disk, 0);
}


} // namespace kudu
