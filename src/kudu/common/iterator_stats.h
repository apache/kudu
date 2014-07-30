// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_COMMON_ITERATOR_STATS_H
#define KUDU_COMMON_ITERATOR_STATS_H

#include <stdint.h>
#include <string>

namespace kudu {

struct IteratorStats {
  IteratorStats();

  std::string ToString() const;

  // The number of data blocks read from disk by the iterator.
  uint32_t data_blocks_read_from_disk;

  // The number of rows which were read from disk --  regardless of whether
  // they were decoded/materialized.
  uint64_t rows_read_from_disk;

  // Add statistics contained 'other' to this object (for each field
  // in this object, increment it by the value of the equivalent field
  // in 'other').
  void AddStats(const IteratorStats& other);

  // TODO: flesh this out with index blocks, number of bytes read,
  // rows
};

} // namespace kudu

#endif
