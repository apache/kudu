// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_SCANNER_INTERNAL_H
#define KUDU_CLIENT_SCANNER_INTERNAL_H

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

  Status OpenTablet(const Slice& key);

  // Extracts data from the last scan response and adds them to 'rows'.
  Status ExtractRows(std::vector<KuduRowResult>* rows);

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
  size_t CalculateProjectedRowSize(const Schema& proj);

  bool open_;
  bool data_in_open_;
  bool has_batch_size_bytes_;
  uint32 batch_size_bytes_;
  KuduClient::ReplicaSelection selection_;

  ReadMode read_mode_;
  int64_t snapshot_timestamp_;

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

  // The projection schema used in the scan, and the expected size (in
  // bytes) per projected row.
  const Schema* projection_;
  size_t projected_row_size_;

  // Machinery to store and encode raw column range predicates into
  // encoded keys.
  ScanSpec spec_;
  RangePredicateEncoder spec_encoder_;

  // Key range we're scanning (optional). Extracted from column range
  // predicates during Open.
  //
  // Memory is owned by 'spec_encoder_'.
  const EncodedKey* start_key_;
  const EncodedKey* end_key_;

  // The tablet we're scanning.
  scoped_refptr<internal::RemoteTablet> remote_;

  enum { kRpcTimeoutMillis = 5000 };

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
