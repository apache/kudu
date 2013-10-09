// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TS_TABLET_MANAGER_H
#define KUDU_TSERVER_TS_TABLET_MANAGER_H

#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>

#include "gutil/macros.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

class FsManager;
class Schema;

namespace master {
class TabletReportPB;
} // namespace master

namespace metadata {
class TabletMasterBlockPB;
class TabletMetadata;
} // namespace metadata

namespace tablet {
class TabletPeer;
}

namespace tserver {

// Keeps track of the tablets hosted on the tablet server side.
//
// TODO: will also be responsible for keeping the local metadata about
// which tablets are hosted on this server persistent on disk, as well
// as re-opening all the tablets at startup, etc.
class TSTabletManager {
 public:
  // Construct the tablet manager.
  // 'fs_manager' must remain valid until this object is destructed.
  explicit TSTabletManager(FsManager* fs_manager);
  ~TSTabletManager();

  // Load all master blocks from disk, and open their respective tablets.
  Status Init();

  // Shut down all of the tablets, gracefully flushing before shutdown.
  void Shutdown();

  // Create a new tablet and register it with the tablet manager. The new tablet
  // is persisted on disk and opened before this method returns.
  //
  // If tablet_peer is non-NULL, the newly created tablet will be returned.
  //
  // If another tablet already exists with this ID, logs a DFATAL
  // and returns a bad Status.
  Status CreateNewTablet(const std::string& tablet_id,
                         const std::string& start_key, const std::string& end_key,
                         const Schema& schema,
                         std::tr1::shared_ptr<tablet::TabletPeer>* tablet_peer);

  // Lookup the given tablet peer by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const std::string& tablet_id,
                    std::tr1::shared_ptr<tablet::TabletPeer>* tablet_peer) const;

  // Generate an incremental tablet report.
  //
  // This will report any tablets which have changed since the last acknowleged
  // tablet report. Once the report is successfully transferred, call
  // AcknowledgeTabletReport() to clear the incremental state. Otherwise, the
  // next tablet report will continue to include the same tablets until one
  // is acknowleged.
  //
  // This is thread-safe to call along with tablet modification, but not safe
  // to call from multiple threads at the same time.
  Status GenerateTabletReport(master::TabletReportPB* report);

  // Mark that the master successfully received and processed the given
  // tablet report. This uses the report sequence number to "un-dirty" any
  // tablets which have not changed since the acknowledged report.
  Status AcknowledgeTabletReport(const master::TabletReportPB& report);

  // Generate a full tablet report and reset any incremental state tracking.
  Status GenerateFullTabletReport(master::TabletReportPB* report);

 private:
  FRIEND_TEST(TsTabletManagerTest, TestPersistBlocks);

  // Write the given master block onto the file system.
  Status PersistMasterBlock(const metadata::TabletMasterBlockPB& pb);

  // Load the given tablet's master block from the file system.
  Status LoadMasterBlock(const string& tablet_id, metadata::TabletMasterBlockPB* block);

  // Open a tablet from the local file system, by first loading its master block,
  // and then opening the tablet itself.
  Status OpenTablet(const std::string& tablet_id);

  // Open a tablet whose metadata has already been loaded.
  Status OpenTablet(gscoped_ptr<metadata::TabletMetadata> meta,
                    std::tr1::shared_ptr<tablet::TabletPeer>* peer);

  // Add the tablet to the tablet map.
  void RegisterTablet(const std::tr1::shared_ptr<tablet::TabletPeer>& tablet_peer);

  // Mark that the given tablet ID is dirty and needs to be included in the next
  // tablet report. This can be used for tablets which are still live as well as
  // those which were removed.
  //
  // NOTE: requires that the caller holds the exclusive lock.
  void MarkDirtyUnlocked(const std::string& tablet_id);

  FsManager* fs_manager_;

  typedef std::tr1::unordered_map<std::string, std::tr1::shared_ptr<tablet::TabletPeer> > TabletMap;

  // Lock protecting tablet_map_ and dirty_tablets_
  mutable rw_spinlock lock_;

  // Map from tablet ID to tablet
  TabletMap tablet_map_;

  // When a tablet is added/removed/added locally and needs to be
  // reported to the master, an entry is added to this map. Each
  // tablet report is assigned a sequence number, so that subsequent
  // tablet reports only need to re-report those tablets which have
  // changed since the last report. Each tablet tracks the sequence
  // number at which it became dirty.
  struct TabletReportState {
    uint32_t change_seq_;
  };
  typedef std::tr1::unordered_map<std::string, TabletReportState> DirtyMap;
  DirtyMap dirty_tablets_;
  int32_t next_report_seq_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TS_TABLET_MANAGER_H */
