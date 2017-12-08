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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_copy.proxy.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {

class KuduPartialRow;
class TimeSeries;

namespace rpc {
class Messenger;
class RpcController;
} // namespace rpc

namespace tserver {

class TabletServerTestBase : public KuduTest {
 public:
  typedef std::pair<int32_t, int32_t> KeyValue;

  TabletServerTestBase();

  // Starts the tablet server, override to start it later.
  void SetUp() override;

  virtual void StartTabletServer(int num_data_dirs);

  Status WaitForTabletRunning(const char *tablet_id);

  void UpdateTestRowRemote(int64_t row_idx,
                           int32_t new_val,
                           TimeSeries* ts = nullptr);

  void ResetClientProxies();

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRowsDirect(int64_t start_row, uint64_t num_rows);

  // Inserts 'num_rows' test rows remotely into the tablet (i.e via RPC)
  // Rows are grouped in batches of 'count'/'num_batches' size.
  // Batch size defaults to 1.
  void InsertTestRowsRemote(int64_t first_row,
                            uint64_t count,
                            uint64_t num_batches = -1,
                            TabletServerServiceProxy* proxy = nullptr,
                            std::string tablet_id = kTabletId,
                            std::vector<uint64_t>* write_timestamps_collector = nullptr,
                            TimeSeries* ts = nullptr,
                            bool string_field_defined = true);

  // Delete specified test row range.
  void DeleteTestRowsRemote(int64_t first_row,
                            uint64_t count,
                            TabletServerServiceProxy* proxy = nullptr,
                            std::string tablet_id = kTabletId);

  void BuildTestRow(int index, KuduPartialRow* row);

  void DrainScannerToStrings(const std::string& scanner_id,
                             const Schema& projection,
                             std::vector<std::string>* results,
                             TabletServerServiceProxy* proxy = nullptr,
                             uint32_t call_seq_id = 1);

  void StringifyRowsFromResponse(const Schema& projection,
                                 const rpc::RpcController& rpc,
                                 ScanResponsePB* resp,
                                 std::vector<std::string>* results);

  void ShutdownTablet();

  Status ShutdownAndRebuildTablet(int num_data_dirs = 1);

  // Verifies that a set of expected rows (key, value) is present in the tablet.
  void VerifyRows(const Schema& schema, const std::vector<KeyValue>& expected);

  // Verifies that a simple scan request fails with the specified error code/message.
  void VerifyScanRequestFailure(const Schema& projection,
                                TabletServerErrorPB::Code expected_code,
                                const char *expected_message);

  // Open a new scanner which scans all of the columns in the table.
  void OpenScannerWithAllColumns(ScanResponsePB* resp,
                                 ReadMode read_mode = READ_LATEST);

 protected:
  static const char* kTableId;
  static const char* kTabletId;

  const Schema schema_;
  Schema key_schema_;
  std::unique_ptr<RowBuilder> rb_;

  std::shared_ptr<rpc::Messenger> client_messenger_;

  std::unique_ptr<MiniTabletServer> mini_server_;
  scoped_refptr<tablet::TabletReplica> tablet_replica_;
  std::unique_ptr<TabletCopyServiceProxy> tablet_copy_proxy_;
  std::unique_ptr<TabletServerServiceProxy> proxy_;
  std::unique_ptr<TabletServerAdminServiceProxy> admin_proxy_;
  std::unique_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;
  std::unique_ptr<server::GenericServiceProxy> generic_proxy_;

  MetricRegistry ts_test_metric_registry_;
  scoped_refptr<MetricEntity> ts_test_metric_entity_;

  void* shared_region_;
};

} // namespace tserver
} // namespace kudu
