// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_FS_BLOCK_MANAGER_METRICS_H
#define KUDU_FS_BLOCK_MANAGER_METRICS_H

#include <stdint.h>

namespace kudu {

class Counter;
template<class T>
class AtomicGauge;
class MetricContext;

namespace fs {
namespace internal {

struct BlockManagerMetrics {
  explicit BlockManagerMetrics(const MetricContext& metric_ctx);

  AtomicGauge<uint64_t>* blocks_open_reading;
  AtomicGauge<uint64_t>* blocks_open_writing;

  Counter* total_readable_blocks;
  Counter* total_writable_blocks;
  Counter* total_bytes_read;
  Counter* total_bytes_written;
};

} // namespace internal
} // namespace fs
} // namespace kudu

#endif // KUDU_FS_BLOCK_MANAGER_METRICS_H
