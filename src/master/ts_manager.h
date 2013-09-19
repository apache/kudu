// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TS_MANAGER_H
#define KUDU_MASTER_TS_MANAGER_H

#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/status.h"

namespace kudu {

class NodeInstancePB;

namespace master {

class TSDescriptor;
class TSRegistrationPB;

// Tracks the servers that the master has heard from, along with their
// last heartbeat, etc.
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

  // Register or re-register a tablet server with the manager.
  //
  // If successful, *desc reset to the registered descriptor.
  Status RegisterTS(const NodeInstancePB& instance,
                    const TSRegistrationPB& registration,
                    std::tr1::shared_ptr<TSDescriptor>* desc);

  // Return all of the currently registered TS descriptors into the provided
  // list.
  void GetAllDescriptors(std::vector<std::tr1::shared_ptr<TSDescriptor> >* descs) const;

 private:
  mutable rw_spinlock lock_;

  std::tr1::unordered_map<std::string, std::tr1::shared_ptr<TSDescriptor> > servers_by_id_;

  DISALLOW_COPY_AND_ASSIGN(TSManager);
};

} // namespace master
} // namespace kudu

#endif
