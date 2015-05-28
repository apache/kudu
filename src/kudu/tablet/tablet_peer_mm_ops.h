// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_TABLET_TABLET_PEER_MM_OPS_H_
#define KUDU_TABLET_TABLET_PEER_MM_OPS_H_

#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/util/stopwatch.h"

namespace kudu {

class Histogram;
template<class T>
class AtomicGauge;

namespace tablet {

// Maintenance op for MRS flush. Only one can happen at a time.
class FlushMRSOp : public MaintenanceOp {
 public:
  explicit FlushMRSOp(TabletPeer* tablet_peer)
    : MaintenanceOp(StringPrintf("FlushMRSOp(%s)", tablet_peer->tablet()->tablet_id().c_str()),
                    MaintenanceOp::HIGH_IO_USAGE),
      tablet_peer_(tablet_peer) {
    time_since_flush_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE;

  virtual bool Prepare() OVERRIDE;

  virtual void Perform() OVERRIDE;

  virtual scoped_refptr<Histogram> DurationHistogram() const OVERRIDE;

  virtual scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const OVERRIDE;

 private:
  // Lock protecting time_since_flush_.
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;

  TabletPeer *const tablet_peer_;
};

// Maintenance op for DMS flush.
// Reports stats for all the DMS this tablet contains but only flushes one in Perform().
class FlushDeltaMemStoresOp : public MaintenanceOp {
 public:
  explicit FlushDeltaMemStoresOp(TabletPeer* tablet_peer)
    : MaintenanceOp(StringPrintf("FlushDeltaMemStoresOp(%s)",
                                 tablet_peer->tablet()->tablet_id().c_str()),
                    MaintenanceOp::HIGH_IO_USAGE),
      tablet_peer_(tablet_peer) {
    time_since_flush_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE;

  virtual bool Prepare() OVERRIDE {
    return true;
  }

  virtual void Perform() OVERRIDE;

  virtual scoped_refptr<Histogram> DurationHistogram() const OVERRIDE;

  virtual scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const OVERRIDE;

 private:
  // Lock protecting time_since_flush_
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;

  TabletPeer *const tablet_peer_;
};

// Maintenance task that runs log GC. Reports log retention that represents the amount of data
// that can be GC'd.
//
// Only one LogGC op can run at a time.
class LogGCOp : public MaintenanceOp {
 public:
  explicit LogGCOp(TabletPeer* tablet_peer);

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE;

  virtual bool Prepare() OVERRIDE;

  virtual void Perform() OVERRIDE;

  virtual scoped_refptr<Histogram> DurationHistogram() const OVERRIDE;

  virtual scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const OVERRIDE;

 private:
  TabletPeer *const tablet_peer_;
  scoped_refptr<Histogram> log_gc_duration_;
  scoped_refptr<AtomicGauge<uint32_t> > log_gc_running_;
  mutable Semaphore sem_;
};

} // namespace tablet
} // namespace kudu

#endif /* KUDU_TABLET_TABLET_PEER_MM_OPS_H_ */
