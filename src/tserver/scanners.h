// Copyright(c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TSERVER_SCANNERS_H
#define KUDU_TSERVER_SCANNERS_H

#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>

#include <boost/thread/shared_mutex.hpp>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "util/auto_release_pool.h"
#include "util/monotime.h"
#include "server/oid_generator.h"

namespace kudu {

class RowwiseIterator;

namespace tserver {

class Scanner;
typedef std::tr1::shared_ptr<Scanner> SharedScanner;

// Manages the live scanners within a Tablet Server.
//
// When a scanner is created by a client, it is assigned a unique scanner ID.
// The client may then use this ID to fetch more rows from the scanner
// or close it.
//
// Since scanners keep resources on the server, the manager periodically
// removes any scanners which have not been accessed since a configurable TTL.
class ScannerManager {
 public:
  ScannerManager();
  ~ScannerManager();

  // Create a new scanner with a unique ID, inserting it into the map.
  void NewScanner(SharedScanner* scanner);

  // Lookup the given scanner by its ID.
  // Returns true if the scanner is found successfully.
  bool LookupScanner(const std::string& scanner_id, SharedScanner* scanner);

  // Unregister the given scanner by its ID.
  // Returns true if unregistered successfully.
  bool UnregisterScanner(const std::string& scanner_id);

  // TODO: add method to iterate through scanners and remove any which
  // are past their TTL

 private:
  // The amount of time that any given scanner should live after its
  // last access.
  MonoDelta scanner_ttl_;

  // Lock protecting the scanner map
  boost::shared_mutex lock_;

  // Map of the currently active scanners.
  typedef std::tr1::unordered_map<std::string, SharedScanner> ScannerMap;
  ScannerMap scanners_by_id_;

  // Generator for scanner IDs.
  ObjectIdGenerator oid_generator_;

  DISALLOW_COPY_AND_ASSIGN(ScannerManager);
};

// An open scanner on the server side.
class Scanner {
 public:
  Scanner(const std::string& id);
  ~Scanner();

  // Attach an actual iterator to this Scanner.
  void Init(gscoped_ptr<RowwiseIterator> iter);

  RowwiseIterator* iter() {
    return DCHECK_NOTNULL(iter_.get());
  }

  // Update the last-access time to the current time,
  // delaying the expiration of the Scanner for another TTL
  // period.
  void UpdateAccessTime();

  // Return the auto-release pool which will be freed when this scanner
  // closes. This can be used as a storage area for the ScanSpec and any
  // associated data (eg storage for its predicates).
  AutoReleasePool* autorelease_pool() {
    return &autorelease_pool_;
  }

  const std::string& id() const { return id_; }

 private:
  friend class ScannerManager;

  // The unique ID of this scanner.
  const std::string id_;

  // TODO: keep information on the scanner's "owner" -- IP address,
  // user information, etc.

  // The last time that the scanner was accessed.
  MonoTime last_access_time_;

  gscoped_ptr<RowwiseIterator> iter_;

  AutoReleasePool autorelease_pool_;

  DISALLOW_COPY_AND_ASSIGN(Scanner);
};


} // namespace tserver
} // namespace kudu

#endif
