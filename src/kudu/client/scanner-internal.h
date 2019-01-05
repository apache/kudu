// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_CLIENT_SCANNER_INTERNAL_H
#define KUDU_CLIENT_SCANNER_INTERNAL_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/resource_metrics.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_configuration.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partition_pruner.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoTime;
class Schema;

namespace tserver {
class TabletServerServiceProxy;
} // tserver

namespace client {

class KuduSchema;

namespace internal {
class RemoteTablet;
class RemoteTabletServer;
} // namespace internal

// The result of KuduScanner::Data::AnalyzeResponse.
//
// This provides a more specific enum for handling the possible error conditions in a Scan
// RPC.
struct ScanRpcStatus {
  enum Result {
    OK,

    // The request was malformed (e.g. bad schema, etc).
    INVALID_REQUEST,

    // The server received the request but it was not ready to serve it right
    // away. It might happen that the server was too busy and did not have
    // necessary resources or information to serve the request but it
    // anticipates it should be ready to serve the request really soon, so it's
    // worth retrying the request at a later time.
    SERVICE_UNAVAILABLE,

    // The deadline for the whole batch was exceeded.
    OVERALL_DEADLINE_EXCEEDED,

    // The deadline for an individual RPC was exceeded, but we have more time left to try
    // on other hosts.
    RPC_DEADLINE_EXCEEDED,

    // The authentication token supplied by the client is invalid. Most likely,
    // the token has expired.
    RPC_INVALID_AUTHENTICATION_TOKEN,

    // The requestor was not authorized to make the request.
    SCAN_NOT_AUTHORIZED,

    // Another RPC-system error (e.g. NetworkError because the TS was down).
    RPC_ERROR,

    // The scanner on the server side expired.
    SCANNER_EXPIRED,

    // The destination tablet was not running (e.g. in the process of bootstrapping).
    TABLET_NOT_RUNNING,

    // The destination tablet does not exist (e.g. because the replica was deleted).
    TABLET_NOT_FOUND,

    // Some other unknown tablet server error. This indicates that the TS was running
    // but some problem occurred other than the ones enumerated above.
    OTHER_TS_ERROR
  };

  Result result;
  Status status;
};

class KuduScanner::Data {
 public:

  explicit Data(KuduTable* table);
  ~Data();

  // Calculates a deadline and sends the next RPC for this scanner. The deadline for the
  // RPC is calculated based on whether 'allow_time_for_failover' is true. If true,
  // the deadline used for the RPC will be shortened so that, on timeout, there will
  // be enough time for another attempt to a different server. If false, then the RPC
  // will use 'overall_deadline' as its deadline.
  //
  // The RPC and TS proxy should already have been prepared in next_req_, proxy_, etc.
  ScanRpcStatus SendScanRpc(const MonoTime& overall_deadline, bool allow_time_for_failover);

  // Called when KuduScanner::NextBatch or KuduScanner::Data::OpenTablet result in an RPC or
  // server error.
  //
  // If the provided 'err' indicates the error was retryable, then returns Status::OK()
  // and potentially inserts the current server into 'blacklist' if the retry should be
  // made on a different replica. If the current server seems healthy, but the scanner expired,
  // sets 'needs_reopen' to true to indicate that the client should re-open a new scanner.
  //
  // If 'needs_reopen' is nullptr, then it is not set.
  //
  // This function may also sleep in case the error suggests that backoff is necessary.
  Status HandleError(const ScanRpcStatus& err,
                     const MonoTime& deadline,
                     std::set<std::string>* blacklist,
                     bool* needs_reopen);

  // Opens the next tablet in the scan, or returns Status::NotFound if there are
  // no more tablets to scan.
  //
  // The deadline is the time budget for this operation.
  // The blacklist is used to temporarily filter out nodes that are experiencing transient errors.
  // This blacklist may be modified by the callee.
  Status OpenNextTablet(const MonoTime& deadline, std::set<std::string>* blacklist);

  // Open the current tablet in the scan again.
  // See OpenNextTablet for options.
  Status ReopenCurrentTablet(const MonoTime& deadline, std::set<std::string>* blacklist);

  // Open the tablet to scan.
  Status OpenTablet(const std::string& partition_key,
                    const MonoTime& deadline,
                    std::set<std::string>* blacklist);

  Status KeepAlive();

  // Returns whether there may exist more tablets to scan.
  //
  // This method does not take into account any non-covered range partitions
  // that may exist in the table, so it should only be used as a hint.
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

  // Update 'last_error_' if need be. Should be invoked whenever a
  // non-fatal (i.e. retriable) scan error is encountered.
  void UpdateLastError(const Status& error);

  const ScanConfiguration& configuration() const {
    return configuration_;
  }

  ScanConfiguration* mutable_configuration() {
    return &configuration_;
  }

  ScanConfiguration configuration_;

  bool open_;
  bool data_in_open_;

  // Set to true if the scan is known to be empty based on predicates and
  // primary key bounds.
  bool short_circuit_;

  // The encoded last primary key from the most recent tablet scan response.
  std::string last_primary_key_;

  internal::RemoteTabletServer* ts_;

  // The proxy can be derived from the RemoteTabletServer, but this involves retaking the
  // meta cache lock. Keeping our own shared_ptr avoids this overhead.
  std::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  // The next scan request to be sent. This is cached as a field
  // since most scan requests will share the scanner ID with the previous
  // request.
  tserver::ScanRequestPB next_req_;

  // The last response received from the server. Cached for buffer reuse.
  tserver::ScanResponsePB last_response_;

  // RPC controller for the last in-flight RPC.
  rpc::RpcController controller_;

  // The table we're scanning.
  sp::shared_ptr<KuduTable> table_;

  PartitionPruner partition_pruner_;

  // The tablet we're scanning.
  scoped_refptr<internal::RemoteTablet> remote_;

  // Number of attempts since the last successful scan.
  int scan_attempts_;

  // Number of rows already returned.
  int64_t num_rows_returned_;

  // The deprecated "NextBatch(vector<KuduRowResult>*) API requires some local
  // storage for the actual row data. If that API is used, this member keeps the
  // actual storage for the batch that is returned.
  KuduScanBatch batch_for_old_api_;

  // The latest error experienced by this scan that provoked a retry. If the
  // scan times out, this error will be incorporated into the status that is
  // passed back to the client.
  //
  // TODO: This and the overall scan retry logic duplicates much of RpcRetrier.
  Status last_error_;

  // The scanner's cumulative resource metrics since the scan was started.
  ResourceMetrics resource_metrics_;

  // Returns a text description of the scan suitable for debug printing.
  //
  // This method will not return sensitive predicate information, so it's
  // suitable for use in client-side logging (as opposed to Scanner::ToString).
  std::string DebugString() const;

 private:
  // Analyze the response of the last Scan RPC made by this scanner.
  //
  // The error handling of a scan RPC is fairly complex, since we have to handle
  // some errors which happen at the network layer, some which happen generically
  // at the RPC layer, and some which are scanner-specific. This function consolidates
  // the various different error situations into a single enum code and Status.
  //
  // 'rpc_status':       the Status directly returned by the RPC Scan() method.
  // 'overall_deadline': the user-provided deadline for the scanner batch
  // 'rpc_deadline':     the deadline that was used for this specific RPC, which
  //                     might be earlier than the overall deadline, in order to
  //                     leave more time for further retries on other hosts.
  ScanRpcStatus AnalyzeResponse(const Status& rpc_status,
                                const MonoTime& overall_deadline,
                                const MonoTime& rpc_deadline);

  // Add additional details to the status message, such as number of retries,
  // original cause of the error, etc. Returns a cloned object.
  Status EnrichStatusMessage(Status s) const;

  void UpdateResourceMetrics();

  DISALLOW_COPY_AND_ASSIGN(Data);
};

class KuduScanBatch::Data {
 public:
  Data();
  ~Data();

  Status Reset(rpc::RpcController* controller,
               const Schema* projection,
               const KuduSchema* client_projection,
               uint64_t row_format_flags,
               std::unique_ptr<RowwiseRowBlockPB> resp_data);

  int num_rows() const {
    return resp_data_.num_rows();
  }

  KuduRowResult row(int idx) {
    DCHECK_EQ(row_format_flags_, KuduScanner::NO_FLAGS)
        << "Cannot decode individual rows. Row format flags were set: "
        << row_format_flags_;
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, num_rows());
    if (direct_data_.empty()) {
      return KuduRowResult(projection_, nullptr);
    }
    int offset = idx * projected_row_size_;
    return KuduRowResult(projection_, &direct_data_[offset]);
  }

  void ExtractRows(std::vector<KuduScanBatch::RowPtr>* rows);

  void Clear();

  // Returns the size of a row for the given projection 'proj'.
  static size_t CalculateProjectedRowSize(const Schema& proj);

  // The RPC controller for the RPC which returned this batch.
  // Holding on to the controller ensures we hold on to the indirect data
  // which contains the rows.
  rpc::RpcController controller_;

  // The PB which contains the "direct data" slice.
  RowwiseRowBlockPB resp_data_;

  // Slices into the direct and indirect row data, whose lifetime is ensured
  // by the members above.
  Slice direct_data_, indirect_data_;

  // The projection being scanned.
  const Schema* projection_;
  // The KuduSchema version of 'projection_'
  const KuduSchema* client_projection_;

  // The row format flags that were passed to the KuduScanner.
  // See: KuduScanner::SetRowFormatFlags()
  uint64_t row_format_flags_;

  // The number of bytes of direct data for each row.
  size_t projected_row_size_;
};

} // namespace client
} // namespace kudu

#endif
