// Copyright(c) 2013, Cloudera, inc.
// All rights reserved.
#include "tserver/scanners.h"

#include "gutil/map-util.h"
#include <gflags/gflags.h>
#include <tr1/memory>

DEFINE_int32(tablet_server_scanner_ttl_seconds, 60,
             "Number of seconds of inactivity allowed for a scanner"
             "before it may be expired");

namespace kudu {
namespace tserver {

ScannerManager::ScannerManager()
  : scanner_ttl_(MonoDelta::FromSeconds(
                   FLAGS_tablet_server_scanner_ttl_seconds)) {
}

ScannerManager::~ScannerManager() {
}

void ScannerManager::NewScanner(SharedScanner* scanner) {
  // Keep trying to generate a unique ID until we get one.
  bool success = false;
  while (!success) {
    // TODO(security): are these UUIDs predictable? If so, we should
    // probably generate random numbers instead, since we can safely
    // just retry until we avoid a collission.
    string id = oid_generator_.Next();
    scanner->reset(new Scanner(id));

    boost::lock_guard<boost::shared_mutex> l(lock_);
    success = InsertIfNotPresent(&scanners_by_id_, id, *scanner);
  }
}

bool ScannerManager::LookupScanner(const string& scanner_id, SharedScanner* scanner) {
  boost::shared_lock<boost::shared_mutex> l(lock_);
  return FindCopy(scanners_by_id_, scanner_id, scanner);
}

bool ScannerManager::UnregisterScanner(const string& scanner_id) {
  boost::lock_guard<boost::shared_mutex> l(lock_);
  return scanners_by_id_.erase(scanner_id) > 0;
}

Scanner::Scanner(const string& id)
  : id_(id),
    last_access_time_(MonoTime::Now(MonoTime::COARSE)) {
}

} // namespace tserver
} // namespace kudu
