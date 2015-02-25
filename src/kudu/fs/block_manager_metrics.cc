// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/block_manager_metrics.h"

#include "kudu/util/metrics.h"

namespace kudu {
namespace fs {
namespace internal {

METRIC_DEFINE_gauge_uint64(blocks_open_reading, kudu::MetricUnit::kBlocks,
                           "Number of data blocks currently open for reading");

METRIC_DEFINE_gauge_uint64(blocks_open_writing, kudu::MetricUnit::kBlocks,
                           "Number of data blocks currently open for writing");

METRIC_DEFINE_counter(total_writable_blocks, kudu::MetricUnit::kBlocks,
                      "Number of data blocks opened for writing since service start");

METRIC_DEFINE_counter(total_readable_blocks, kudu::MetricUnit::kBlocks,
                      "Number of data blocks opened for reading since service start");

METRIC_DEFINE_counter(total_bytes_written, kudu::MetricUnit::kBytes,
                      "Number of bytes of block data written since service start");

METRIC_DEFINE_counter(total_bytes_read, kudu::MetricUnit::kBytes,
                      "Number of bytes of block data read since service start");

#define MINIT(x) x(METRIC_##x.Instantiate(metric_ctx))
#define GINIT(x) x(AtomicGauge<uint64_t>::Instantiate(METRIC_##x, metric_ctx))
BlockManagerMetrics::BlockManagerMetrics(const MetricContext& metric_ctx)
  : GINIT(blocks_open_reading),
    GINIT(blocks_open_writing),
    MINIT(total_readable_blocks),
    MINIT(total_writable_blocks),
    MINIT(total_bytes_read),
    MINIT(total_bytes_written) {
}
#undef GINIT
#undef MINIT

} // namespace internal
} // namespace fs
} // namespace kudu
