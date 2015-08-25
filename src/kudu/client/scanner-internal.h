// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_SCANNER_INTERNAL_H
#define KUDU_CLIENT_SCANNER_INTERNAL_H

#include <set>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/client/client.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/predicate_encoder.h"
#include "kudu/tserver/tserver_service.proxy.h"

namespace kudu {

namespace client {

class KuduScanner::Data {
 public:
  explicit Data(KuduTable* table);
  ~Data();

  Status CheckForErrors();

  // Copies a predicate lower or upper bound from 'bound_src' into
  // 'bound_dst'.
  void CopyPredicateBound(const ColumnSchema& col,
                          const void* bound_src, std::string* bound_dst);

  // Called when KuduScanner::NextBatch or KuduScanner::Data::OpenTablet result in an RPC or
  // server error. Returns the error status if the call cannot be retried.
  //
  // The number of parameters reflects the complexity of handling retries.
  // We must respect the overall scan 'deadline', as well as the 'blacklist' of servers
  // experiencing transient failures. See the implementation for more details.
  Status CanBeRetried(const bool isNewScan,
                      const Status& rpc_status,
                      const Status& server_status,
                      const MonoTime& actual_deadline,
                      const MonoTime& deadline,
                      const std::vector<internal::RemoteTabletServer*>& candidates,
                      std::set<std::string>* blacklist);

  // Open a tablet.
  // The deadline is the time budget for this operation.
  // The blacklist is used to temporarily filter out nodes that are experiencing transient errors.
  // This blacklist may be modified by the callee.
  Status OpenTablet(const std::string& partition_key,
                    const MonoTime& deadline,
                    std::set<std::string>* blacklist);

  // Extracts data from the last scan response and adds them to 'rows'.
  Status ExtractRows(std::vector<KuduRowResult>* rows);

  // Static implementation of ExtractRows. This is used by some external
  // tools.
  static Status ExtractRows(const rpc::RpcController& controller,
                            const Schema* projection,
                            tserver::ScanResponsePB* resp,
                            std::vector<KuduRowResult>* rows);

  // Returns whether there exist more tablets we should scan.
  //
  // Note: there may not be any actual matching rows in subsequent tablets,
  // but we won't know until we scan them.
  bool MoreTablets() const;

  // Possible scan requests.
  enum RequestType {
    // A new scan of a particular tablet.
    NEW,

    // A continuation of an existing scan (to read more rows).
    CONTINUE,

    // A close of a partially-completed scan. Complete scans are closed
    // automatically by the tablet server.
    CLOSE
  };

  // Modifies fields in 'next_req_' in preparation for a new request.
  void PrepareRequest(RequestType state);

  // Returns the size of a row for the given projection 'proj'.
  static size_t CalculateProjectedRowSize(const Schema& proj);

  bool open_;
  bool data_in_open_;
  bool has_batch_size_bytes_;
  uint32 batch_size_bytes_;
  KuduClient::ReplicaSelection selection_;

  ReadMode read_mode_;
  bool is_fault_tolerant_;
  int64_t snapshot_timestamp_;

  // The encoded last primary key from the most recent tablet scan response.
  std::string last_primary_key_;

  internal::RemoteTabletServer* ts_;
  // The proxy can be derived from the RemoteTabletServer, but this involves retaking the
  // meta cache lock. Keeping our own shared_ptr avoids this overhead.
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  // The next scan request to be sent. This is cached as a field
  // since most scan requests will share the scanner ID with the previous
  // request.
  tserver::ScanRequestPB next_req_;

  // The last response received from the server. Cached for buffer reuse.
  tserver::ScanResponsePB last_response_;

  // RPC controller for the last in-flight RPC.
  rpc::RpcController controller_;

  // The table we're scanning.
  KuduTable* table_;

  // The projection schema used in the scan.
  const Schema* projection_;

  Arena arena_;
  AutoReleasePool pool_;

  // Machinery to store and encode raw column range predicates into
  // encoded keys.
  ScanSpec spec_;
  RangePredicateEncoder spec_encoder_;

  // The tablet we're scanning.
  scoped_refptr<internal::RemoteTablet> remote_;

  // Timeout for scanner RPCs.
  MonoDelta timeout_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
