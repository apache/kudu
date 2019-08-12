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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

namespace master {
class TabletReportPB;
}

namespace tserver {

class TabletServer;

// Component of the Tablet Server which is responsible for heartbeating to all
// of the masters.
class Heartbeater {
 public:
  Heartbeater(UnorderedHostPortSet master_addrs, TabletServer* server);

  // Start heartbeating to every master.
  Status Start();

  // Stop heartbeating to every master.
  Status Stop();

  // Trigger heartbeats as soon as possible, even if the normal
  // heartbeat interval has not expired.
  void TriggerASAP();

  // Mark the given tablets as dirty, or do nothing if they are already dirty.
  //
  // Tablet dirtiness is tracked separately for each master. Dirty tablets are
  // included in the heartbeat's tablet report, and only marked not dirty once
  // the report has been acknowledged by the master.
  void MarkTabletsDirty(const std::vector<std::string>& tablet_ids, const std::string& reason);

  ~Heartbeater();

  // Methods for manually manipulating tablet reports, intended for testing.
  // The generate methods return one report per master.
  std::vector<master::TabletReportPB> GenerateIncrementalTabletReportsForTests();
  std::vector<master::TabletReportPB> GenerateFullTabletReportsForTests();
  void MarkTabletReportsAcknowledgedForTests(
      const std::vector<master::TabletReportPB>& reports);

 private:
  class Thread;

  FRIEND_TEST(TsTabletManagerITest, TestDeduplicateMasterAddrsForHeartbeaters);

  std::vector<std::unique_ptr<Thread>> threads_;
  DISALLOW_COPY_AND_ASSIGN(Heartbeater);
};

} // namespace tserver
} // namespace kudu
