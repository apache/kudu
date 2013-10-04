// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TS_TABLET_MANAGER_H
#define KUDU_TSERVER_TS_TABLET_MANAGER_H

#include <string>
#include <tr1/memory>

#include "gutil/macros.h"
#include "util/status.h"

namespace kudu {

class FsManager;

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

  Status Init();

  // Shut down all of the tablets, gracefully flushing before shutdown.
  void Shutdown();

  // Register the given tablet peer to be managed by this tablet server.
  // TODO: this will probably die, in favor of a CreateTablet method --
  // it doesn't really make sense for an external entity to create new tablets.
  void RegisterTablet(const std::tr1::shared_ptr<tablet::TabletPeer>& tablet_peer);

  // Lookup the given tablet peer by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const std::string& tablet_id,
                    std::tr1::shared_ptr<tablet::TabletPeer>* tablet_peer) const;

 private:
  FsManager* fs_manager_;

  // TODO: replace with a map
  std::tr1::shared_ptr<tablet::TabletPeer> tablet_peer_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TS_TABLET_MANAGER_H */
