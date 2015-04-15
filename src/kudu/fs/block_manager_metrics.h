// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_FS_BLOCK_MANAGER_METRICS_H
#define KUDU_FS_BLOCK_MANAGER_METRICS_H

#include <stdint.h>

#include "kudu/gutil/ref_counted.h"

namespace kudu {

class Counter;
template<class T>
class AtomicGauge;
class MetricEntity;

namespace fs {
namespace internal {

struct BlockManagerMetrics {
  explicit BlockManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  scoped_refptr<AtomicGauge<uint64_t> > blocks_open_reading;
  scoped_refptr<AtomicGauge<uint64_t> > blocks_open_writing;

  scoped_refptr<Counter> total_readable_blocks;
  scoped_refptr<Counter> total_writable_blocks;
  scoped_refptr<Counter> total_bytes_read;
  scoped_refptr<Counter> total_bytes_written;
};

} // namespace internal
} // namespace fs
} // namespace kudu

#endif // KUDU_FS_BLOCK_MANAGER_METRICS_H
