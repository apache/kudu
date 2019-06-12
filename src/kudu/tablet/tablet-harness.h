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
#ifndef KUDU_TABLET_TABLET_REPLICA_HARNESS_H
#define KUDU_TABLET_TABLET_REPLICA_HARNESS_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/env.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

// Creates a default partition schema and partition for a table.
//
// The provided schema must include column IDs.
//
// The partition schema will have no hash components, and a single range
// component over the primary key columns. The partition will cover the
// entire partition-key space.
static std::pair<PartitionSchema, Partition> CreateDefaultPartition(const Schema& schema) {
  // Create a default partition schema.
  PartitionSchema partition_schema;
  CHECK_OK(PartitionSchema::FromPB(PartitionSchemaPB(), schema, &partition_schema));

  // Create the tablet partitions.
  std::vector<Partition> partitions;
  CHECK_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  CHECK_EQ(1, partitions.size());
  return std::make_pair(partition_schema, partitions[0]);
}

class TabletHarness {
 public:
  struct Options {
    enum ClockType {
      HYBRID_CLOCK,
      LOGICAL_CLOCK
    };
    explicit Options(std::string root_dir)
        : env(Env::Default()),
          tablet_id("test_tablet_id"),
          root_dir(std::move(root_dir)),
          enable_metrics(true),
          clock_type(LOGICAL_CLOCK) {}

    Env* env;
    std::string tablet_id;
    std::string root_dir;
    bool enable_metrics;
    ClockType clock_type;
  };

  TabletHarness(const Schema& schema, Options options)
      : options_(std::move(options)), schema_(schema) {}

  Status Create(bool first_time) {
    std::pair<PartitionSchema, Partition> partition(CreateDefaultPartition(schema_));

    // Build the Tablet
    fs_manager_.reset(new FsManager(options_.env, options_.root_dir));
    if (first_time) {
      RETURN_NOT_OK(fs_manager_->CreateInitialFileSystemLayout());
    }
    RETURN_NOT_OK(fs_manager_->Open());

    scoped_refptr<TabletMetadata> metadata;
    RETURN_NOT_OK(TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                               options_.tablet_id,
                                               "KuduTableTest",
                                               "KuduTableTestId",
                                               schema_,
                                               partition.first,
                                               partition.second,
                                               TABLET_DATA_READY,
                                               /*tombstone_last_logged_opid=*/ boost::none,
                                               /*extra_config=*/ boost::none,
                                               /*dimension_label=*/ boost::none,
                                               &metadata));
    if (options_.enable_metrics) {
      metrics_registry_.reset(new MetricRegistry());
    }

    if (options_.clock_type == Options::LOGICAL_CLOCK) {
      clock_.reset(clock::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp));
    } else {
      clock_.reset(new clock::HybridClock());
      RETURN_NOT_OK(clock_->Init());
    }
    tablet_.reset(new Tablet(metadata,
                             clock_,
                             std::shared_ptr<MemTracker>(),
                             metrics_registry_.get(),
                             make_scoped_refptr(new log::LogAnchorRegistry())));
    return Status::OK();
  }

  Status Open() {
    RETURN_NOT_OK(tablet_->Open());
    return tablet_->MarkFinishedBootstrapping();
  }

  clock::Clock* clock() const {
    return clock_.get();
  }

  const std::shared_ptr<Tablet>& tablet() {
    return tablet_;
  }

  Tablet* mutable_tablet() {
    return tablet_.get();
  }

  FsManager* fs_manager() {
    return fs_manager_.get();
  }

  MetricRegistry* metrics_registry() {
    return metrics_registry_.get();
  }

 private:
  Options options_;

  gscoped_ptr<MetricRegistry> metrics_registry_;

  scoped_refptr<clock::Clock> clock_;
  Schema schema_;
  gscoped_ptr<FsManager> fs_manager_;
  std::shared_ptr<Tablet> tablet_;
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_REPLICA_HARNESS_H */
