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
#include "kudu/tserver/tablet_copy-test-base.h"

#include <gflags/gflags.h>
#include <limits>
#include <thread>
#include <vector>

#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/transfer.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/crc.h"
#include "kudu/util/env_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

#define ASSERT_REMOTE_ERROR(status, err, code, str) \
    ASSERT_NO_FATAL_FAILURE(AssertRemoteError(status, err, code, str))

DECLARE_uint64(tablet_copy_idle_timeout_ms);
DECLARE_uint64(tablet_copy_timeout_poll_period_ms);

namespace kudu {
namespace tserver {

using consensus::MaximumOpId;
using consensus::MinimumOpId;
using consensus::OpIdEquals;
using env_util::ReadFully;
using log::ReadableLogSegment;
using rpc::ErrorStatusPB;
using rpc::RpcController;
using std::thread;
using std::vector;

class TabletCopyServiceTest : public TabletCopyTest {
 public:
  TabletCopyServiceTest() {
    // Poll for session expiration every 10 ms for the session timeout test.
    FLAGS_tablet_copy_timeout_poll_period_ms = 10;
  }

 protected:
  void SetUp() OVERRIDE {
    TabletCopyTest::SetUp();
    tablet_copy_proxy_.reset(
        new TabletCopyServiceProxy(
            client_messenger_, mini_server_->bound_rpc_addr(),
            mini_server_->bound_rpc_addr().host()));
  }

  Status DoBeginTabletCopySession(const string& tablet_id,
                                       const string& requestor_uuid,
                                       BeginTabletCopySessionResponsePB* resp,
                                       RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    BeginTabletCopySessionRequestPB req;
    req.set_tablet_id(tablet_id);
    req.set_requestor_uuid(requestor_uuid);
    return UnwindRemoteError(
        tablet_copy_proxy_->BeginTabletCopySession(req, resp, controller), controller);
  }

  Status DoBeginValidTabletCopySession(string* session_id,
                                            tablet::TabletSuperBlockPB* superblock = nullptr,
                                            uint64_t* idle_timeout_millis = nullptr,
                                            vector<uint64_t>* sequence_numbers = nullptr) {
    BeginTabletCopySessionResponsePB resp;
    RpcController controller;
    RETURN_NOT_OK(DoBeginTabletCopySession(GetTabletId(), GetLocalUUID(), &resp, &controller));
    *session_id = resp.session_id();
    if (superblock) {
      *superblock = resp.superblock();
    }
    if (idle_timeout_millis) {
      *idle_timeout_millis = resp.session_idle_timeout_millis();
    }
    if (sequence_numbers) {
      sequence_numbers->assign(resp.wal_segment_seqnos().begin(), resp.wal_segment_seqnos().end());
    }
    return Status::OK();
  }

  Status DoCheckSessionActive(const string& session_id,
                              CheckTabletCopySessionActiveResponsePB* resp,
                              RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    CheckTabletCopySessionActiveRequestPB req;
    req.set_session_id(session_id);
    return UnwindRemoteError(
        tablet_copy_proxy_->CheckSessionActive(req, resp, controller), controller);
  }

  Status DoFetchData(const string& session_id, const DataIdPB& data_id,
                     uint64_t* offset, int64_t* max_length,
                     FetchDataResponsePB* resp,
                     RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    FetchDataRequestPB req;
    req.set_session_id(session_id);
    req.mutable_data_id()->CopyFrom(data_id);
    if (offset) {
      req.set_offset(*offset);
    }
    if (max_length) {
      req.set_max_length(*max_length);
    }
    return UnwindRemoteError(
        tablet_copy_proxy_->FetchData(req, resp, controller), controller);
  }

  Status DoEndTabletCopySession(const string& session_id, bool is_success,
                                     const Status* error_msg,
                                     EndTabletCopySessionResponsePB* resp,
                                     RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    EndTabletCopySessionRequestPB req;
    req.set_session_id(session_id);
    req.set_is_success(is_success);
    if (error_msg) {
      StatusToPB(*error_msg, req.mutable_error());
    }
    return UnwindRemoteError(
        tablet_copy_proxy_->EndTabletCopySession(req, resp, controller), controller);
  }

  // Decode the remote error into a Status object.
  Status ExtractRemoteError(const ErrorStatusPB* remote_error) {
    const TabletCopyErrorPB& error =
        remote_error->GetExtension(TabletCopyErrorPB::tablet_copy_error_ext);
    return StatusFromPB(error.status());
  }

  // Enhance a RemoteError Status message with additional details from the remote.
  Status UnwindRemoteError(Status status, const RpcController* controller) {
    if (!status.IsRemoteError() ||
        controller->error_response()->code() != ErrorStatusPB::ERROR_APPLICATION) {
      return status;
    }
    Status remote_error = ExtractRemoteError(controller->error_response());
    return status.CloneAndPrepend(remote_error.ToString());
  }

  void AssertRemoteError(Status status, const ErrorStatusPB* remote_error,
                         const TabletCopyErrorPB::Code app_code,
                         const string& status_code_string) {
    ASSERT_TRUE(status.IsRemoteError()) << "Unexpected status code: " << status.ToString()
                                        << ", app code: "
                                        << TabletCopyErrorPB::Code_Name(app_code)
                                        << ", status code string: " << status_code_string;
    const Status app_status = ExtractRemoteError(remote_error);
    const TabletCopyErrorPB& error =
        remote_error->GetExtension(TabletCopyErrorPB::tablet_copy_error_ext);
    ASSERT_EQ(app_code, error.code()) << SecureShortDebugString(error);
    ASSERT_EQ(status_code_string, app_status.CodeAsString()) << app_status.ToString();
    LOG(INFO) << app_status.ToString();
  }

  // Return BlockId in format suitable for a FetchData() call.
  static DataIdPB AsDataTypeId(const BlockId& block_id) {
    DataIdPB data_id;
    data_id.set_type(DataIdPB::BLOCK);
    block_id.CopyToPB(data_id.mutable_block_id());
    return data_id;
  }

  gscoped_ptr<TabletCopyServiceProxy> tablet_copy_proxy_;
};

// Test beginning and ending a tablet copy session.
TEST_F(TabletCopyServiceTest, TestSimpleBeginEndSession) {
  string session_id;
  tablet::TabletSuperBlockPB superblock;
  uint64_t idle_timeout_millis;
  vector<uint64_t> segment_seqnos;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id,
                                               &superblock,
                                               &idle_timeout_millis,
                                               &segment_seqnos));
  // Basic validation of returned params.
  ASSERT_FALSE(session_id.empty());
  ASSERT_EQ(FLAGS_tablet_copy_idle_timeout_ms, idle_timeout_millis);
  ASSERT_TRUE(superblock.IsInitialized());
  // We should have number of segments = number of rolls + 1 (due to the active segment).
  ASSERT_EQ(kNumLogRolls + 1, segment_seqnos.size());

  EndTabletCopySessionResponsePB resp;
  RpcController controller;
  ASSERT_OK(DoEndTabletCopySession(session_id, true, nullptr, &resp, &controller));
}

// Test starting two sessions. The current implementation will silently only create one.
TEST_F(TabletCopyServiceTest, TestBeginTwice) {
  // Second time through should silently succeed.
  for (int i = 0; i < 2; i++) {
    string session_id;
    ASSERT_OK(DoBeginValidTabletCopySession(&session_id));
    ASSERT_FALSE(session_id.empty());
  }
}

// Regression test for KUDU-1436: race conditions if multiple requests
// to begin the same tablet copy session arrive at more or less the
// same time.
TEST_F(TabletCopyServiceTest, TestBeginConcurrently) {
  const int kNumThreads = 5;
  vector<thread> threads;
  vector<tablet::TabletSuperBlockPB> sblocks(kNumThreads);
  for (int i = 0 ; i < kNumThreads; i++) {
    threads.emplace_back([this, &sblocks, i]{
        string session_id;
        CHECK_OK(DoBeginValidTabletCopySession(&session_id, &sblocks[i]));
        CHECK(!session_id.empty());
      });
  }
  for (auto& t : threads) {
    t.join();
  }
  // Verify that all threads got the same result.
  for (int i = 1; i < threads.size(); i++) {
    ASSERT_EQ(SecureDebugString(sblocks[i]), SecureDebugString(sblocks[0]));
  }
}

// Test bad session id error condition.
TEST_F(TabletCopyServiceTest, TestInvalidSessionId) {
  vector<string> bad_session_ids;
  bad_session_ids.push_back("hodor");
  bad_session_ids.push_back(GetLocalUUID());

  // Fetch a block for a non-existent session.
  for (const string& session_id : bad_session_ids) {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::BLOCK);
    data_id.mutable_block_id()->set_id(1);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(), TabletCopyErrorPB::NO_SESSION,
                        Status::NotFound("").CodeAsString());
  }

  // End a non-existent session.
  for (const string& session_id : bad_session_ids) {
    EndTabletCopySessionResponsePB resp;
    RpcController controller;
    Status status = DoEndTabletCopySession(session_id, true, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(), TabletCopyErrorPB::NO_SESSION,
                        Status::NotFound("").CodeAsString());
  }
}

// Test bad tablet id error condition.
TEST_F(TabletCopyServiceTest, TestInvalidTabletId) {
  BeginTabletCopySessionResponsePB resp;
  RpcController controller;
  Status status =
      DoBeginTabletCopySession("some-unknown-tablet", GetLocalUUID(), &resp, &controller);
  ASSERT_REMOTE_ERROR(status, controller.error_response(), TabletCopyErrorPB::TABLET_NOT_FOUND,
                      Status::NotFound("").CodeAsString());
}

// Test DataIdPB validation.
TEST_F(TabletCopyServiceTest, TestInvalidBlockOrOpId) {
  string session_id;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id));

  // Invalid BlockId.
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::BLOCK);
    data_id.mutable_block_id()->set_id(1);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        TabletCopyErrorPB::BLOCK_NOT_FOUND,
                        Status::NotFound("").CodeAsString());
  }

  // Invalid Segment Sequence Number for log fetch.
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    data_id.set_wal_segment_seqno(31337);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        TabletCopyErrorPB::WAL_SEGMENT_NOT_FOUND,
                        Status::NotFound("").CodeAsString());
  }

  // Empty data type with BlockId.
  // The server will reject the request since we are missing the required 'type' field.
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.mutable_block_id()->set_id(1);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_TRUE(status.IsRemoteError()) << status.ToString();
    ASSERT_STR_CONTAINS(status.ToString(),
                        "Invalid argument: invalid parameter for call "
                        "kudu.tserver.TabletCopyService.FetchData: "
                        "missing fields: data_id.type");
  }

  // Empty data type id (no BlockId, no Segment Sequence Number);
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST,
                        Status::InvalidArgument("").CodeAsString());
  }

  // Both BlockId and Segment Sequence Number in the same "union" PB (illegal).
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::BLOCK);
    data_id.mutable_block_id()->set_id(1);
    data_id.set_wal_segment_seqno(0);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST,
                        Status::InvalidArgument("").CodeAsString());
  }
}

// Test invalid file offset error condition.
TEST_F(TabletCopyServiceTest, TestFetchInvalidBlockOffset) {
  string session_id;
  tablet::TabletSuperBlockPB superblock;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id, &superblock));

  FetchDataResponsePB resp;
  RpcController controller;
  // Impossible offset.
  uint64_t offset = std::numeric_limits<uint64_t>::max();
  Status status = DoFetchData(session_id, AsDataTypeId(FirstColumnBlockId(superblock)),
                              &offset, nullptr, &resp, &controller);
  ASSERT_REMOTE_ERROR(status, controller.error_response(),
                      TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST,
                      Status::InvalidArgument("").CodeAsString());
}

// Test that we are able to fetch an entire block.
TEST_F(TabletCopyServiceTest, TestFetchBlockAtOnce) {
  string session_id;
  tablet::TabletSuperBlockPB superblock;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id, &superblock));

  // Local.
  BlockId block_id = FirstColumnBlockId(superblock);
  Slice local_data;
  faststring scratch;
  ASSERT_OK(ReadLocalBlockFile(mini_server_->server()->fs_manager(), block_id,
                               &scratch, &local_data));

  // Remote.
  FetchDataResponsePB resp;
  RpcController controller;
  ASSERT_OK(DoFetchData(session_id, AsDataTypeId(block_id), nullptr, nullptr, &resp, &controller));

  AssertDataEqual(local_data.data(), local_data.size(), resp.chunk());
}

// Test that we are able to incrementally fetch blocks.
TEST_F(TabletCopyServiceTest, TestFetchBlockIncrementally) {
  string session_id;
  tablet::TabletSuperBlockPB superblock;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id, &superblock));

  BlockId block_id = FirstColumnBlockId(superblock);
  Slice local_data;
  faststring scratch;
  ASSERT_OK(ReadLocalBlockFile(mini_server_->server()->fs_manager(), block_id,
                               &scratch, &local_data));

  // Grab the remote data in several chunks.
  int64_t block_size = local_data.size();
  int64_t max_chunk_size = block_size / 5;
  uint64_t offset = 0;
  while (offset < block_size) {
    FetchDataResponsePB resp;
    RpcController controller;
    ASSERT_OK(DoFetchData(session_id, AsDataTypeId(block_id),
                                 &offset, &max_chunk_size, &resp, &controller));
    int64_t returned_bytes = resp.chunk().data().size();
    ASSERT_LE(returned_bytes, max_chunk_size);
    AssertDataEqual(local_data.data() + offset, returned_bytes, resp.chunk());
    offset += returned_bytes;
  }
}

// Test that we are able to fetch log segments.
TEST_F(TabletCopyServiceTest, TestFetchLog) {
  string session_id;
  tablet::TabletSuperBlockPB superblock;
  uint64_t idle_timeout_millis;
  vector<uint64_t> segment_seqnos;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id,
                                               &superblock,
                                               &idle_timeout_millis,
                                               &segment_seqnos));

  ASSERT_EQ(kNumLogRolls + 1, segment_seqnos.size());
  uint64_t seg_seqno = *segment_seqnos.begin();

  // Fetch the remote data.
  FetchDataResponsePB resp;
  RpcController controller;
  DataIdPB data_id;
  data_id.set_type(DataIdPB::LOG_SEGMENT);
  data_id.set_wal_segment_seqno(seg_seqno);
  ASSERT_OK(DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller));

  // Fetch the local data.
  log::SegmentSequence local_segments;
  ASSERT_OK(tablet_peer_->log()->reader()->GetSegmentsSnapshot(&local_segments));

  uint64_t first_seg_seqno = (*local_segments.begin())->header().sequence_number();


  ASSERT_EQ(seg_seqno, first_seg_seqno)
      << "Expected equal sequence numbers: " << seg_seqno
      << " and " << first_seg_seqno;
  const scoped_refptr<ReadableLogSegment>& segment = local_segments[0];
  faststring scratch;
  int64_t size = segment->file_size();
  scratch.resize(size);
  Slice slice;
  ASSERT_OK(ReadFully(segment->readable_file().get(), 0, size, &slice, scratch.data()));

  AssertDataEqual(slice.data(), slice.size(), resp.chunk());
}

// Test that the tablet copy session timeout works properly.
TEST_F(TabletCopyServiceTest, TestSessionTimeout) {
  // This flag should be seen by the service due to TSO.
  // We have also reduced the timeout polling frequency in SetUp().
  FLAGS_tablet_copy_idle_timeout_ms = 1; // Expire the session almost immediately.

  // Start session.
  string session_id;
  ASSERT_OK(DoBeginValidTabletCopySession(&session_id));

  MonoTime start_time = MonoTime::Now();
  CheckTabletCopySessionActiveResponsePB resp;

  do {
    RpcController controller;
    ASSERT_OK(DoCheckSessionActive(session_id, &resp, &controller));
    if (!resp.session_is_active()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1)); // 1 ms
  } while (MonoTime::Now().GetDeltaSince(start_time).ToSeconds() < 10);

  ASSERT_FALSE(resp.session_is_active()) << "Tablet Copy session did not time out!";
}

} // namespace tserver
} // namespace kudu
