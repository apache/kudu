// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_MASTER_TS_MANAGER_H
#define KUDU_MASTER_TS_MANAGER_H

#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class NodeInstancePB;

namespace master {

class TSDescriptor;
class TSRegistrationPB;

typedef std::vector<std::tr1::shared_ptr<TSDescriptor> > TSDescriptorVector;

// Tracks the servers that the master has heard from, along with their
// last heartbeat, etc.
//
// Note that TSDescriptors are never deleted, even if the TS crashes
// and has not heartbeated in quite a while. This makes it simpler to
// keep references to TSDescriptors elsewhere in the master without
// fear of lifecycle problems. Dead servers are "dead, but not forgotten"
// (they live on in the heart of the master).
//
// This class is thread-safe.
class TSManager {
 public:
  TSManager();
  virtual ~TSManager();

  // Lookup the tablet server descriptor for the given instance identifier.
  // If the TS has never registered, or this instance doesn't match the
  // current instance ID for the TS, then a NotFound status is returned.
  // Otherwise, *desc is set and OK is returned.
  Status LookupTS(const NodeInstancePB& instance,
                  std::tr1::shared_ptr<TSDescriptor>* desc);

  // Lookup the tablet server descriptor for the given UUID.
  // Returns false if the TS has never registered.
  // Otherwise, *desc is set and returns true.
  bool LookupTSByUUID(const std::string& uuid,
                        std::tr1::shared_ptr<TSDescriptor>* desc);

  // Register or re-register a tablet server with the manager.
  //
  // If successful, *desc reset to the registered descriptor.
  Status RegisterTS(const NodeInstancePB& instance,
                    const TSRegistrationPB& registration,
                    std::tr1::shared_ptr<TSDescriptor>* desc);

  // Return all of the currently registered TS descriptors into the provided
  // list.
  void GetAllDescriptors(std::vector<std::tr1::shared_ptr<TSDescriptor> >* descs) const;

  // Get the TS count.
  int GetCount() const;

 private:
  mutable rw_spinlock lock_;

  std::tr1::unordered_map<std::string, std::tr1::shared_ptr<TSDescriptor> > servers_by_id_;

  DISALLOW_COPY_AND_ASSIGN(TSManager);
};

} // namespace master
} // namespace kudu

#endif
