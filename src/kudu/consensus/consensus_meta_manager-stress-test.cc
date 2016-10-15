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

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/stl_logging.h>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/barrier.h"
#include "kudu/util/locks.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::lock_guard;
using std::string;
using std::thread;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace consensus {

static constexpr const int64_t kInitialTerm = 1;

using LockTable = unordered_map<string, string>;

// Multithreaded stress tests for the cmeta manager.
class ConsensusMetadataManagerStressTest : public KuduTest {
 public:
  ConsensusMetadataManagerStressTest()
      : rng_(SeedRandom()),
        fs_manager_(env_, GetTestPath("fs_root")),
        cmeta_manager_(new ConsensusMetadataManager(&fs_manager_)) {
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test configuration.
    config_.set_opid_index(kInvalidOpIdIndex);
    RaftPeerPB* peer = config_.add_peers();
    peer->set_permanent_uuid(fs_manager_.uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

 protected:
  enum OpType {
    kCreate,
    kLoad,
    kDelete,
    kNumOpTypes, // Must come last.
  };

  ThreadSafeRandom rng_;
  FsManager fs_manager_;
  scoped_refptr<ConsensusMetadataManager> cmeta_manager_;
  RaftConfigPB config_;

  // Lock used by tests.
  simple_spinlock lock_;
};

// Concurrency test to check whether TSAN will flag unsafe concurrent
// operations for simultaneous access to the cmeta manager by different threads
// on different tablet ids. For a given tablet id, a lock table is used as
// external synchronization to ensure exclusive access by a single thread.
TEST_F(ConsensusMetadataManagerStressTest, CreateLoadDeleteTSANTest) {
  static const int kNumTablets = 26;
  static const int kNumThreads = 8;
  static const int kNumOpsPerThread = 1000;

  // Set of tablets we are operating on.
  vector<string> tablet_ids;

  // Map of tablet_id -> cmeta existence.
  unordered_map<string, bool> tablet_cmeta_exists;

  // Each entry in 'lock_table' protects each value of
  // 'tablet_cmeta_exists[tablet_id]'. We never resize 'tablet_cmeta_exists'.
  LockTable lock_table;

  for (int i = 0; i < kNumTablets; i++) {
    string tablet_id = string(1, 'a' + i);
    // None of the cmetas have been created yet.
    InsertOrDie(&tablet_cmeta_exists, tablet_id, false);
    tablet_ids.push_back(std::move(tablet_id));
  }

  // Eventually exit if the test hangs.
  alarm(60);
  auto c = MakeScopedCleanup([&] { alarm(0); });

  atomic<int64_t> ops_performed(0);
  Barrier barrier(kNumThreads);
  vector<thread> threads;
  for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
    threads.emplace_back([&] {
      barrier.Wait();
      for (int op_num = 0; op_num < kNumOpsPerThread; op_num++) {
        const string& tablet_id = tablet_ids[rng_.Uniform(kNumTablets)];
        auto unlocker = MakeScopedCleanup([&] {
          lock_guard<simple_spinlock> l(lock_);
          CHECK(lock_table.erase(tablet_id));
        });
        // Acquire lock in lock table or bail.
        {
          // 'lock_' protects 'lock_table'.
          lock_guard<simple_spinlock> l(lock_);
          if (ContainsKey(lock_table, tablet_id)) {
            // Another thread has access to this tablet id. Bail.
            unlocker.cancel(); // Don't unlock what we didn't lock.
            continue;
          }
          InsertOrDie(&lock_table, tablet_id, "lock for test");
        }
        OpType type = static_cast<OpType>(rng_.Uniform(kNumOpTypes));
        switch (type) {
          case kCreate: {
            Status s = cmeta_manager_->Create(tablet_id, config_, kInitialTerm);
            if (tablet_cmeta_exists[tablet_id]) {
              CHECK(s.IsAlreadyPresent()) << s.ToString();
            } else {
              CHECK(s.ok()) << s.ToString();
              ops_performed.fetch_add(1, std::memory_order_relaxed);
            }
            tablet_cmeta_exists[tablet_id] = true;
            break;
          }
          case kLoad: {
            scoped_refptr<ConsensusMetadata> cmeta;
            Status s = cmeta_manager_->Load(tablet_id, &cmeta);
            if (tablet_cmeta_exists[tablet_id]) {
              CHECK(s.ok()) << s.ToString();
              ops_performed.fetch_add(1, std::memory_order_relaxed);
            } else {
              CHECK(s.IsNotFound()) << tablet_id << ": " << s.ToString();
            }
            // Load() does not change 'tablet_cmeta_exists' status.
            break;
          }
          case kDelete: {
            Status s = cmeta_manager_->Delete(tablet_id);
            if (tablet_cmeta_exists[tablet_id]) {
              CHECK(s.ok()) << s.ToString();
              ops_performed.fetch_add(1, std::memory_order_relaxed);
            } else {
              CHECK(s.IsNotFound()) << s.ToString();
            }
            tablet_cmeta_exists[tablet_id] = false;
            break;
          }
          default: LOG(FATAL) << type; break;
        }
      }
    });
  }

  for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
    threads[thread_num].join();
  }

  LOG(INFO) << "Ops performed: " << ops_performed.load(std::memory_order_relaxed);
}

} // namespace consensus
} // namespace kudu
