// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/server/tcmalloc_metrics.h"

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <google/malloc_extension.h>

#include "kudu/util/metrics.h"

#ifndef TCMALLOC_ENABLED
#define TCM_ASAN_MSG " (Disabled - no tcmalloc in this build)"
#else
#define TCM_ASAN_MSG
#endif

// As of this writing, we expose all of the un-deprecated tcmalloc status metrics listed at:
// http://gperftools.googlecode.com/svn/trunk/doc/tcmalloc.html

METRIC_DEFINE_gauge_uint64(generic_current_allocated_bytes, kudu::MetricUnit::kBytes,
    "Number of bytes used by the application. This will not typically match the memory "
    "use reported by the OS, because it does not include TCMalloc overhead or memory "
    "fragmentation." TCM_ASAN_MSG);

METRIC_DEFINE_gauge_uint64(generic_heap_size, kudu::MetricUnit::kBytes,
    "Bytes of system memory reserved by TCMalloc." TCM_ASAN_MSG);

METRIC_DEFINE_gauge_uint64(tcmalloc_pageheap_free_bytes, kudu::MetricUnit::kBytes,
    "Number of bytes in free, mapped pages in page heap. These bytes can be used to "
    "fulfill allocation requests. They always count towards virtual memory usage, and "
    "unless the underlying memory is swapped out by the OS, they also count towards "
    "physical memory usage." TCM_ASAN_MSG);

METRIC_DEFINE_gauge_uint64(tcmalloc_pageheap_unmapped_bytes, kudu::MetricUnit::kBytes,
    "Number of bytes in free, unmapped pages in page heap. These are bytes that have "
    "been released back to the OS, possibly by one of the MallocExtension \"Release\" "
    "calls. They can be used to fulfill allocation requests, but typically incur a page "
    "fault. They always count towards virtual memory usage, and depending on the OS, "
    "typically do not count towards physical memory usage." TCM_ASAN_MSG);

METRIC_DEFINE_gauge_uint64(tcmalloc_max_total_thread_cache_bytes, kudu::MetricUnit::kBytes,
    "A limit to how much memory TCMalloc dedicates for small objects. Higher numbers "
    "trade off more memory use for -- in some situations -- improved efficiency." TCM_ASAN_MSG);

METRIC_DEFINE_gauge_uint64(tcmalloc_current_total_thread_cache_bytes, kudu::MetricUnit::kBytes,
    "A measure of some of the memory TCMalloc is using (for small objects)." TCM_ASAN_MSG);

#undef TCM_ASAN_MSG

namespace kudu {
namespace tcmalloc {

static size_t GetTCMallocPropValue(const char* prop) {
  size_t value = 0;
#ifdef TCMALLOC_ENABLED
  if (!MallocExtension::instance()->GetNumericProperty(prop, &value)) {
    LOG(DFATAL) << "Failed to get value of numeric tcmalloc property: " << prop;
  }
#endif
  return value;
}

void RegisterMetrics(MetricRegistry* registry) {
  MetricContext ctx(registry, "tcmalloc");
  METRIC_generic_current_allocated_bytes.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "generic.current_allocated_bytes"));
  METRIC_generic_heap_size.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "generic.heap_size"));
  METRIC_tcmalloc_pageheap_free_bytes.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "tcmalloc.pageheap_free_bytes"));
  METRIC_tcmalloc_pageheap_unmapped_bytes.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "tcmalloc.pageheap_unmapped_bytes"));
  METRIC_tcmalloc_max_total_thread_cache_bytes.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "tcmalloc.max_total_thread_cache_bytes"));
  METRIC_tcmalloc_current_total_thread_cache_bytes.InstantiateFunctionGauge(ctx,
      boost::bind(GetTCMallocPropValue, "tcmalloc.current_total_thread_cache_bytes"));
}

} // namespace tcmalloc
} // namespace kudu
