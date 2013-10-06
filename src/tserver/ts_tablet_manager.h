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

  FsManager* fs_manager_;

  typedef std::tr1::unordered_map<std::string, std::tr1::shared_ptr<tablet::TabletPeer> > TabletMap;

  // Lock protecting tablet_map_
  mutable rw_spinlock lock_;

  // Map from tablet ID to tablet
  TabletMap tablet_map_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TS_TABLET_MANAGER_H */
