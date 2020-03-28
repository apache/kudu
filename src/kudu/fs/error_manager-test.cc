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

#include "kudu/fs/error_manager.h"

#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

using std::map;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

const int kVecSize = 10;

namespace kudu {
namespace fs {

// These tests are designed to ensure callback serialization by using callbacks
// that update a single vector.
class FsErrorManagerTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    em_.reset(new FsErrorManager());
    test_vec_.resize(kVecSize, -1);
  }

  // Returns a stringified version of the single vector that each thread is
  // updating.
  string test_vec_string() {
    return JoinInts(test_vec_, " ");
  }

  // Sleeps for a random amount of time, up to 500ms.
  void SleepForRand() {
    SleepFor(MonoDelta::FromMilliseconds(rand() % 500));
  }

  // Returns the index of the first instance of 'k' in test_vec_.
  int FindFirst(int k) {
    int first = -1;
    for (int i = 0; i < test_vec_.size(); i++) {
      if (test_vec_[i] == k) {
        first = i;
        break;
      }
    }
    return first;
  }

  // Writes i to the first available (-1) entry in test_vec_ after sleeping a
  // random amount of time. If multiple calls to this are running at the same
  // time, it is likely that some will write to the same entry.
  //
  // NOTE: this can be curried into an ErrorNotificationCb.
  void SleepAndWriteFirstEmptyCb(int i, const string& /* s */) {
    DFAKE_SCOPED_LOCK(fake_lock_);
    int first_available = FindFirst(-1);
    SleepForRand();
    if (first_available == -1) {
      LOG(INFO) << "No available entries!";
      return;
    }
    test_vec_[first_available] = i;
  }

  // Returns a map between each unique value in test_vec_ and the indices
  // within test_vec_ at which the value is located.
  map<int, set<int>> GetPositions() {
    map<int, set<int>> positions;
    for (int i = 0; i < test_vec_.size(); i++) {
      positions[test_vec_[i]].insert(i);
    }
    return positions;
  }

  FsErrorManager* em() const { return em_.get(); }

 protected:
  // The single vector that the error notification callbacks will all write to.
  vector<int> test_vec_;

 private:
  unique_ptr<FsErrorManager> em_;

  // Fake lock used to ensure threads don't run error-handling at the same time.
  DFAKE_MUTEX(fake_lock_);
};

// Tests the basic functionality (i.e. registering, unregistering, calling
// callbacks) of the error manager.
TEST_F(FsErrorManagerTest, TestBasicRegistration) {
  // Before registering anything, there should be all '-1's in test_vec_.
  ASSERT_EQ(-1, FindFirst(ErrorHandlerType::DISK_ERROR));
  ASSERT_EQ(-1, FindFirst(ErrorHandlerType::NO_AVAILABLE_DISKS));

  // Register a callback to update the first '-1' entry in test_vec_ to '0'
  // after waiting a random amount of time.
  em()->SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR, [this](const string& uuid) {
        this->SleepAndWriteFirstEmptyCb(ErrorHandlerType::DISK_ERROR, uuid);
      });
  em()->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, "");
  ASSERT_EQ(0, FindFirst(ErrorHandlerType::DISK_ERROR));

  // Running callbacks that haven't been registered should do nothing.
  em()->RunErrorNotificationCb(ErrorHandlerType::NO_AVAILABLE_DISKS, "");
  ASSERT_EQ(0, FindFirst(ErrorHandlerType::DISK_ERROR));
  ASSERT_EQ(-1, FindFirst(ErrorHandlerType::NO_AVAILABLE_DISKS));

  // Now register another callback.
  em()->SetErrorNotificationCb(
      ErrorHandlerType::NO_AVAILABLE_DISKS, [this](const string& uuid) {
        this->SleepAndWriteFirstEmptyCb(ErrorHandlerType::NO_AVAILABLE_DISKS, uuid);
      });
  em()->RunErrorNotificationCb(ErrorHandlerType::NO_AVAILABLE_DISKS, "");
  ASSERT_EQ(1, FindFirst(ErrorHandlerType::NO_AVAILABLE_DISKS));

  // Now unregister one of the callbacks. This should not affect the other.
  em()->UnsetErrorNotificationCb(ErrorHandlerType::DISK_ERROR);
  em()->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, "");
  em()->RunErrorNotificationCb(ErrorHandlerType::NO_AVAILABLE_DISKS, "");

  LOG(INFO) << "Final state of the vector: " << test_vec_string();
  map<int, set<int>> positions = GetPositions();
  set<int> disk_set = { 0 };        // The first entry should be DISK...
  set<int> tablet_set = { 1, 2 };   // ...followed by NO_AVAILABLE_DISKS, NO_AVAILABLE_DISKS.
  ASSERT_EQ(disk_set, FindOrDie(positions, ErrorHandlerType::DISK_ERROR));
  ASSERT_EQ(tablet_set, FindOrDie(positions, ErrorHandlerType::NO_AVAILABLE_DISKS));
}

// Test that the callbacks get run serially.
TEST_F(FsErrorManagerTest, TestSerialization) {
  em()->SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR, [this](const string& uuid) {
        this->SleepAndWriteFirstEmptyCb(ErrorHandlerType::DISK_ERROR, uuid);
      });
  em()->SetErrorNotificationCb(
      ErrorHandlerType::NO_AVAILABLE_DISKS, [this](const string& uuid) {
        this->SleepAndWriteFirstEmptyCb(ErrorHandlerType::NO_AVAILABLE_DISKS, uuid);
      });

  // Swap back and forth between error-handler type.
  const auto IntToEnum = [&] (int i) {
    return i % 2 == 0 ? ErrorHandlerType::DISK_ERROR : ErrorHandlerType::NO_AVAILABLE_DISKS;
  };

  vector<thread> cb_threads;
  for (int i = 0; i < kVecSize; i++) {
    // Each call will update the first available entry in test_vec_ after
    // waiting a random amount of time. Without proper serialization, these
    // could end up writing to the same entry.
    cb_threads.emplace_back([&,i] {
      em()->RunErrorNotificationCb(IntToEnum(i), "");
    });
  }
  for (auto& t : cb_threads) {
    t.join();
  }
  LOG(INFO) << "Final state of the vector: " << test_vec_string();

  // Because the callbacks ran serially, all threads should have been able to
  // get a unique slot in test_vec_, and thus, all entries should be filled.
  // Note that the order of the calls does not matter, only the fact that they
  // ran serially.
  CHECK_EQ(-1, FindFirst(-1));
}

}  // namespace fs
}  // namespace kudu
