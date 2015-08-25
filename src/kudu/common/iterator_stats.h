// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_COMMON_ITERATOR_STATS_H
#define KUDU_COMMON_ITERATOR_STATS_H

#include <stdint.h>
#include <string>

namespace kudu {

struct IteratorStats {
  IteratorStats();

  std::string ToString() const;

  // The number of data blocks read from disk (or cache) by the iterator.
  int64_t data_blocks_read_from_disk;

  // The number of bytes read from disk (or cache) by the iterator.
  int64_t bytes_read_from_disk;

  // The number of cells which were read from disk --  regardless of whether
  // they were decoded/materialized.
  int64_t cells_read_from_disk;

  // Add statistics contained 'other' to this object (for each field
  // in this object, increment it by the value of the equivalent field
  // in 'other').
  void AddStats(const IteratorStats& other);

  // Same, except subtract.
  void SubtractStats(const IteratorStats& other);

 private:
  // DCHECK that all of the stats are non-negative. This is a no-op in
  // release builds.
  void DCheckNonNegative() const;
};

} // namespace kudu

#endif
