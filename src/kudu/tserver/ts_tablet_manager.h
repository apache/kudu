// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_TS_TABLET_MANAGER_H
#define KUDU_TSERVER_TS_TABLET_MANAGER_H

#include <boost/thread/locks.hpp>
#include <gtest/gtest_prod.h>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/tablet_peer_lookup.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

class FsManager;
class Schema;

namespace master {
class ReportedTabletPB;
class TabletReportPB;
} // namespace master

namespace metadata {
class QuorumPB;
} // namespace metadata

namespace tablet {
class TabletMasterBlockPB;
class TabletMetadata;
class TabletPeer;
class TabletStatusPB;
class TabletStatusListener;
}

namespace tserver {
class TabletServer;

typedef std::tr1::unordered_set<std::string> CreatesInProgressSet;

// Keeps track of the tablets hosted on the tablet server side.
//
// TODO: will also be responsible for keeping the local metadata about
// which tablets are hosted on this server persistent on disk, as well
// as re-opening all the tablets at startup, etc.
class TSTabletManager : public tserver::TabletPeerLookupIf {
 public:
  // Construct the tablet manager.
  // 'fs_manager' must remain valid until this object is destructed.
  TSTabletManager(FsManager* fs_manager,
                  TabletServer* server,
                  const MetricContext& metric_ctx);

  virtual ~TSTabletManager();

  // Load all master blocks from disk, and open their respective tablets.
  // Upon return of this method all existing tablets are registered, but
  // the bootstrap is performed asynchronously.
  Status Init();

  // Waits for all the bootstraps to complete.
  // Returns Status::OK if all tablets bootstrapped successfully. If
  // the bootstrap of any tablet failed returns the failure reason for
  // the first tablet whose bootstrap failed.
  Status WaitForAllBootstrapsToFinish();

  // Shut down all of the tablets, gracefully flushing before shutdown.
  void Shutdown();

  // Create a new tablet and register it with the tablet manager. The new tablet
  // is persisted on disk and opened before this method returns.
  //
  // If tablet_peer is non-NULL, the newly created tablet will be returned.
  //
  // If another tablet already exists with this ID, logs a DFATAL
  // and returns a bad Status.
  Status CreateNewTablet(const std::string& table_id,
                         const std::string& tablet_id,
                         const std::string& start_key,
                         const std::string& end_key,
                         const std::string& table_name,
                         const Schema& schema,
                         metadata::QuorumPB quorum,
                         scoped_refptr<tablet::TabletPeer>* tablet_peer);

  // Delete the specified tablet.
  // TODO: Remove it from disk
  Status DeleteTablet(const scoped_refptr<tablet::TabletPeer>& tablet_peer);

  // Lookup the given tablet peer by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const std::string& tablet_id,
                    scoped_refptr<tablet::TabletPeer>* tablet_peer) const;

  // Same as LookupTablet but doesn't acquired the shared lock.
  bool LookupTabletUnlocked(const std::string& tablet_id,
                            scoped_refptr<tablet::TabletPeer>* tablet_peer) const;

  virtual Status GetTabletPeer(const std::string& tablet_id,
                               scoped_refptr<tablet::TabletPeer>* tablet_peer) const
                               OVERRIDE;

  virtual const NodeInstancePB& NodeInstance() const OVERRIDE;

  // Generate an incremental tablet report.
  //
  // This will report any tablets which have changed since the last acknowleged
  // tablet report. Once the report is successfully transferred, call
  // MarkTabletReportAcknowledged() to clear the incremental state. Otherwise, the
  // next tablet report will continue to include the same tablets until one
  // is acknowleged.
  //
  // This is thread-safe to call along with tablet modification, but not safe
  // to call from multiple threads at the same time.
  void GenerateIncrementalTabletReport(master::TabletReportPB* report);

  // Generate a full tablet report and reset any incremental state tracking.
  void GenerateFullTabletReport(master::TabletReportPB* report);

  // Mark that the master successfully received and processed the given
  // tablet report. This uses the report sequence number to "un-dirty" any
  // tablets which have not changed since the acknowledged report.
  void MarkTabletReportAcknowledged(const master::TabletReportPB& report);

  // Get all of the tablets currently hosted on this server.
  void GetTabletPeers(std::vector<scoped_refptr<tablet::TabletPeer> >* tablet_peers) const;

  // Marks tablet with 'tablet_id' dirty.
  // Used for state changes outside of the control of TsTabletManager, such as consensus role
  // changes.
  void MarkTabletDirty(tablet::TabletPeer* tablet_peer);

  Status RunAllLogGC();

 private:
  FRIEND_TEST(TsTabletManagerTest, TestPersistBlocks);

  // Each tablet report is assigned a sequence number, so that subsequent
  // tablet reports only need to re-report those tablets which have
  // changed since the last report. Each tablet tracks the sequence
  // number at which it became dirty.
  struct TabletReportState {
    uint32_t change_seq;
  };
  typedef std::tr1::unordered_map<std::string, TabletReportState> DirtyMap;

  // Write the given master block onto the file system.
  Status PersistMasterBlock(const tablet::TabletMasterBlockPB& pb);

  // Load the given tablet's master block from the file system.
  Status LoadMasterBlock(const std::string& tablet_id, tablet::TabletMasterBlockPB* block);

  // Open a tablet meta from the local file system by loading its master block.
  Status OpenTabletMeta(const std::string& tablet_id,
                        scoped_refptr<tablet::TabletMetadata>* metadata);

  // Open a tablet whose metadata has already been loaded/created.
  // This method does not return anything as it can be run asynchronously.
  // Upon completion of this method the tablet should be initialized and running.
  // If something wrong happened on bootstrap/initialization the relevant error
  // will be set on TabletPeer along with the state set to FAILED.
  // NOTE: The tablet must be registered prior to calling this method.
  void OpenTablet(const scoped_refptr<tablet::TabletMetadata>& meta);

  // Open a tablet whose metadata has already been loaded.
  void BootstrapAndInitTablet(const scoped_refptr<tablet::TabletMetadata>& meta,
                              scoped_refptr<tablet::TabletPeer>* peer);

  // Add the tablet to the tablet map.
  void RegisterTablet(const std::string& tablet_id,
                      const scoped_refptr<tablet::TabletPeer>& tablet_peer);

  // Helper to generate the report for a single tablet.
  void CreateReportedTabletPB(const std::string& tablet_id,
                              const scoped_refptr<tablet::TabletPeer>& tablet_peer,
                              master::ReportedTabletPB* reported_tablet);

  // Mark that the provided TabletPeer's state has changed. That should be taken into
  // account in the next report.
  //
  // NOTE: requires that the caller holds the lock.
  void MarkDirtyUnlocked(tablet::TabletPeer* tablet_peer);

  TSTabletManagerStatePB state() const {
    boost::shared_lock<rw_spinlock> lock(lock_);
    return state_;
  }

  FsManager* fs_manager_;

  TabletServer* server_;

  typedef std::tr1::unordered_map<std::string, scoped_refptr<tablet::TabletPeer> > TabletMap;

  // Lock protecting tablet_map_, dirty_tablets_, state_, and
  // creates_in_progress_.
  mutable rw_spinlock lock_;

  // Map from tablet ID to tablet
  TabletMap tablet_map_;

  // Set of tablet ids whose creation is in-progress
  CreatesInProgressSet creates_in_progress_;

  // Tablets to include in the next incremental tablet report.
  // When a tablet is added/removed/added locally and needs to be
  // reported to the master, an entry is added to this map.
  DirtyMap dirty_tablets_;

  // Next tablet report seqno.
  int32_t next_report_seq_;

  MetricContext metric_ctx_;

  TSTabletManagerStatePB state_;

  // Thread pool used to open the tablets async, whether bootstrap is required or not.
  gscoped_ptr<ThreadPool> open_tablet_pool_;

  // Executors for apply transactions, shared between all tablets.
  gscoped_ptr<ThreadPool> leader_apply_pool_;
  gscoped_ptr<ThreadPool> replica_apply_pool_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TS_TABLET_MANAGER_H */
