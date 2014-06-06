// Copyright(c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TSERVER_SCANNERS_H
#define KUDU_TSERVER_SCANNERS_H

#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

#include <boost/thread/shared_mutex.hpp>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "util/auto_release_pool.h"
#include "util/monotime.h"
#include "server/oid_generator.h"

namespace kudu {

class RowwiseIterator;
class ScanSpec;
class Schema;
struct IteratorStats;

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
  void NewScanner(const std::string& tablet_id,
                  const std::string& requestor_string,
                  SharedScanner* scanner);

  // Lookup the given scanner by its ID.
  // Returns true if the scanner is found successfully.
  bool LookupScanner(const std::string& scanner_id, SharedScanner* scanner);

  // Unregister the given scanner by its ID.
  // Returns true if unregistered successfully.
  bool UnregisterScanner(const std::string& scanner_id);

  // Return the number of scanners currently active.
  size_t CountActiveScanners() const;

  // List all active scanners.
  void ListScanners(std::vector<SharedScanner>* scanners);

  // TODO: add method to iterate through scanners and remove any which
  // are past their TTL

 private:
  typedef std::pair<std::string, SharedScanner> ScannerMapEntry;

  // The amount of time that any given scanner should live after its
  // last access.
  MonoDelta scanner_ttl_;

  // Lock protecting the scanner map
  mutable boost::shared_mutex lock_;

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
  explicit Scanner(const std::string& id,
                   const std::string& tablet_id,
                   const std::string& requestor_string);
  ~Scanner();

  // Attach an actual iterator and a ScanSpec to this Scanner.
  // Takes ownership of 'iter' and 'spec'.
  void Init(gscoped_ptr<RowwiseIterator> iter,
            gscoped_ptr<ScanSpec> spec);

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

  // Return the ScanSpec associated with this Scanner.
  const ScanSpec& spec() const;

  const std::string& tablet_id() const { return tablet_id_; }

  const std::string& requestor_string() const { return requestor_string_; }

  // Return the last time this scan was updated.
  const MonoTime& last_access_time() const { return last_access_time_; }

  // Returns the time this scan was started.
  const MonoTime& start_time() const { return start_time_; }

  // Associate a projection schema with the Scanner. The scanner takes
  // ownership of 'client_projection_schema'.
  //
  // Note: 'client_projection_schema' is set if the client's
  // projection is a subset of the iterator's schema -- the iterator's
  // schema needs to include all columns that have predicates, whereas
  // the client may not want to project all of them.
  void set_client_projection_schema(gscoped_ptr<Schema> client_projection_schema) {
    client_projection_schema_.swap(client_projection_schema);
  }

  // Returns request's projection schema if it differs from the schema
  // used by the iterator (which must contain all columns used as
  // predicates). Returns NULL if the iterator's schema is the same as
  // the projection schema.
  // See the note about 'set_client_projection_schema' above.
  const Schema* client_projection_schema() const { return client_projection_schema_.get(); }

  // Get per-column stats for each iterator.
  void GetIteratorStats(std::vector<IteratorStats>* stats) const;

 private:
  friend class ScannerManager;

  // The unique ID of this scanner.
  const std::string id_;

  // Tablet associated with the scanner.
  const std::string tablet_id_;

  // Information about the requestor. Populated from
  // RpcContext::requestor_string().
  const std::string requestor_string_;

  // The last time that the scanner was accessed.
  MonoTime last_access_time_;

  // The time the scanner was started.
  const MonoTime start_time_;

  // The spec used by 'iter_'
  gscoped_ptr<ScanSpec> spec_;

  // Stores the request's projection schema, if it differs from the
  // schema used by the iterator.
  gscoped_ptr<Schema> client_projection_schema_;

  gscoped_ptr<RowwiseIterator> iter_;

  AutoReleasePool autorelease_pool_;

  DISALLOW_COPY_AND_ASSIGN(Scanner);
};


} // namespace tserver
} // namespace kudu

#endif
