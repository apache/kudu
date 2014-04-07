// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TEST_GRAPH_COLLECTOR_H
#define KUDU_TEST_GRAPH_COLLECTOR_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include <string>

#include "gutil/ref_counted.h"
#include "gutil/macros.h"
#include "gutil/walltime.h"
#include "util/countdown_latch.h"
#include "util/faststring.h"
#include "util/pthread_spinlock.h"
#include "util/thread.h"

namespace kudu {

using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;

class TimeSeries {
 public:
  void AddValue(double val);
  void SetValue(double val);

  double value() const;

 private:
  friend class TimeSeriesCollector;

  DISALLOW_COPY_AND_ASSIGN(TimeSeries);

  TimeSeries() :
    val_(0)
  {}

  mutable PThreadSpinLock lock_;
  double val_;
};

class TimeSeriesCollector {
 public:
  explicit TimeSeriesCollector(const string &scope) :
    scope_(scope),
    exit_latch_(0),
    started_(false)
  {}

  ~TimeSeriesCollector();

  shared_ptr<TimeSeries> GetTimeSeries(const string &key);
  void StartDumperThread();
  void StopDumperThread();

 private:
  DISALLOW_COPY_AND_ASSIGN(TimeSeriesCollector);

  void DumperThread();
  void BuildMetricsString(WallTime time_since_start, faststring *dst_buf) const;

  string scope_;

  typedef unordered_map<string, shared_ptr<TimeSeries> > SeriesMap;
  SeriesMap series_map_;
  mutable boost::mutex series_lock_;

  scoped_refptr<kudu::Thread> dumper_thread_;

  // Latch used to stop the dumper_thread_. When the thread is started,
  // this is set to 1, and when the thread should exit, it is counted down.
  CountDownLatch exit_latch_;

  bool started_;
};

} // namespace kudu
#endif
