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

#include "kudu/tools/master_rebuilder.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_options.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

DECLARE_int32(default_num_replicas);

using kudu::master::Master;
using kudu::master::MasterOptions;
using kudu::master::SysCatalogTable;
using kudu::master::SysTablesEntryPB;
using kudu::master::SysTabletsEntryPB;
using kudu::master::TableInfo;
using kudu::master::TableMetadataLock;
using kudu::master::TabletInfo;
using kudu::master::TabletMetadataGroupLock;
using kudu::master::TabletMetadataLock;
using kudu::tserver::ListTabletsResponsePB;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {
Status NoOpCb() {
  return Status::OK();
}
} // anonymous namespace

MasterRebuilder::MasterRebuilder(vector<string> tserver_addrs)
    : state_(State::NOT_DONE),
      tserver_addrs_(std::move(tserver_addrs)) {
}

const RebuildReport& MasterRebuilder::GetRebuildReport() const {
  CHECK_EQ(State::DONE, state_);
  return rebuild_report_;
}

Status MasterRebuilder::RebuildMaster() {
  CHECK_EQ(State::NOT_DONE, state_);

  int bad_tservers = 0;
  for (const auto& tserver_addr : tserver_addrs_) {
    std::unique_ptr<tserver::TabletServerServiceProxy> proxy;
    vector<ListTabletsResponsePB::StatusAndSchemaPB> replicas;
    Status s = BuildProxy(tserver_addr, tserver::TabletServer::kDefaultPort, &proxy).AndThen([&]() {
      return GetReplicas(proxy.get(), &replicas);
    });
    rebuild_report_.tservers.emplace_back(tserver_addr, s);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("Failed to gather metadata from tablet server $0: $1",
                                 tserver_addr, s.ToString());
      bad_tservers++;
      continue;
    }

    for (const auto& replica : replicas) {
      const auto& tablet_status_pb = replica.tablet_status();
      const auto& state_pb = tablet_status_pb.state();
      const auto& state_str = TabletStatePB_Name(state_pb);
      const auto& tablet_id = tablet_status_pb.tablet_id();
      const auto& table_name = tablet_status_pb.table_name();
      switch (state_pb) {
        case tablet::STOPPING:
        case tablet::STOPPED:
        case tablet::SHUTDOWN:
        case tablet::FAILED:
          LOG(INFO) << Substitute("Skipping replica of tablet $0 of table $1 on tablet "
                                  "server $2 in state $3", tablet_id, table_name, tserver_addr,
                                  state_str);
          continue;
        default:
          break;
      }
      Status s = CheckTableAndTabletConsistency(replica);
      if (!s.ok()) {
        LOG(WARNING) << Substitute("Failed to process metadata for replica of tablet $0 "
                                   "of table $1 on tablet server $2 in state $3: $4",
                                   tablet_id, table_name, tserver_addr, state_str, s.ToString());
      }
      InsertOrDieNoPrint(&rebuild_report_.replicas,
                         std::make_tuple(table_name, tablet_id, tserver_addr), s);
    }
  }

  // Check how many tablet servers we got metadata from. We can still continue
  // as long as one reported. If not all tablet servers returned info, our
  // reconstructed syscatalog might be missing tables and tablets, or it might
  // have everything.
  if (bad_tservers > 0) {
    LOG(WARNING) << Substitute("Failed to gather metadata from all tablet servers: "
                               "$0 of $1 tablet server(s) had errors",
                               bad_tservers, tserver_addrs_.size());
  }
  if (bad_tservers == tserver_addrs_.size()) {
    return Status::ServiceUnavailable("unable to gather any tablet server metadata");
  }

  // Now that we've assembled all the metadata, we can write to a syscatalog table.
  RETURN_NOT_OK(WriteSysCatalog());

  state_ = State::DONE;
  return Status::OK();
}

Status MasterRebuilder::CheckTableAndTabletConsistency(
    const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  const string& table_name = replica.tablet_status().table_name();
  const string& tablet_id = replica.tablet_status().tablet_id();

  if (!ContainsKey(tables_by_name_, table_name)) {
    CreateTable(replica);
  } else {
    RETURN_NOT_OK(CheckTableConsistency(replica));
  }

  if (!ContainsKey(tablets_by_id_, tablet_id)) {
    CreateTablet(replica);
  }
  return CheckTabletConsistency(replica);
}

void MasterRebuilder::CreateTable(const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  scoped_refptr<TableInfo> table(new TableInfo(oid_generator_.Next()));
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB* table_metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  const string& table_name = replica.tablet_status().table_name();
  table_metadata->set_name(table_name);

  // We can't tell the schema version from ListTablets.
  // If there's multiple schemas reported by the replicas (i.e. if an alter
  // table is in progress), we'll fail to recover some replicas and only
  // partially recover the table.
  table_metadata->set_version(0);

  // We can't tell the replication factor from ListTablets.
  // We'll guess the default replication factor because it's safe and almost
  // always correct.
  // TODO(awong): there's probably a better heuristic based on the number of
  // replicas reported by the tablet servers.
  table_metadata->set_num_replicas(FLAGS_default_num_replicas);
  table_metadata->mutable_schema()->CopyFrom(replica.schema());
  table_metadata->mutable_partition_schema()->CopyFrom(replica.partition_schema());

  // We can't tell what the next column id should be, so we guess something so
  // large we're almost certainly OK.
  table_metadata->set_next_column_id(std::numeric_limits<int32_t>::max() / 2);
  table_metadata->set_state(SysTablesEntryPB::RUNNING);
  table_metadata->set_state_msg("reconstructed by MasterRebuilder");
  table->mutable_metadata()->CommitMutation();
  InsertOrDie(&tables_by_name_, table_name, table);
}

Status MasterRebuilder::CheckTableConsistency(
    const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  const string& tablet_id = replica.tablet_status().tablet_id();
  const string& table_name = replica.tablet_status().table_name();
  scoped_refptr<TableInfo> table = FindOrDie(tables_by_name_, table_name);

  TableMetadataLock l_table(table.get(), LockMode::READ);
  const SysTablesEntryPB& metadata = table->metadata().state().pb;

  // Check the schemas match.
  Schema schema_from_table;
  Schema schema_from_replica;
  RETURN_NOT_OK(SchemaFromPB(metadata.schema(), &schema_from_table));
  RETURN_NOT_OK(SchemaFromPB(replica.schema(), &schema_from_replica));
  if (schema_from_table != schema_from_replica) {
    LOG(WARNING) << Substitute("Schema mismatch for tablet $0 of table $1", tablet_id, table_name);
    LOG(WARNING) << Substitute("Table schema: $0", schema_from_table.ToString());
    LOG(WARNING) << Substitute("Mismatched schema: $0", schema_from_replica.ToString());
    return Status::Corruption("inconsistent replica: schema mismatch");
  }

  // Check the partition schemas match.
  PartitionSchema pschema_from_table;
  PartitionSchema pschema_from_replica;
  RETURN_NOT_OK(PartitionSchema::FromPB(metadata.partition_schema(),
                                        schema_from_table,
                                        &pschema_from_table));
  RETURN_NOT_OK(PartitionSchema::FromPB(replica.partition_schema(),
                                        schema_from_replica,
                                        &pschema_from_replica));
  if (pschema_from_table != pschema_from_replica) {
    LOG(WARNING) << Substitute("Partition schema mismatch for tablet $0 of table $1",
                               tablet_id, table_name);
    LOG(WARNING) << Substitute("First seen partition schema: $0",
                               pschema_from_table.DebugString(schema_from_table));
    LOG(WARNING) << Substitute("Mismatched partition schema $0",
                               pschema_from_replica.DebugString(schema_from_replica));
    return Status::Corruption("inconsistent replica: partition schema mismatch");
  }
  return Status::OK();
}

void MasterRebuilder::CreateTablet(const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  const string& table_name = replica.tablet_status().table_name();
  const string& tablet_id = replica.tablet_status().tablet_id();
  scoped_refptr<TableInfo> table = FindOrDie(tables_by_name_, table_name);
  scoped_refptr<TabletInfo> tablet(new TabletInfo(table, tablet_id));

  tablet->mutable_metadata()->StartMutation();
  SysTabletsEntryPB* metadata = &tablet->mutable_metadata()->mutable_dirty()->pb;
  metadata->mutable_partition()->CopyFrom(replica.tablet_status().partition());

  // Setting the term to the minimum and an invalid opid ensures that, when a
  // master loads the reconstructed syscatalog table and receives tablet
  // reports, it will adopt the consensus state from the reports.
  metadata->mutable_consensus_state()->set_current_term(consensus::kMinimumTerm);
  metadata->mutable_consensus_state()->mutable_committed_config()
      ->set_opid_index(consensus::kInvalidOpIdIndex);
  metadata->set_state(SysTabletsEntryPB::RUNNING);
  metadata->set_state_msg("reconstructed by MasterRebuilder");
  metadata->set_table_id(table->id());
  tablet->mutable_metadata()->CommitMutation();
  InsertOrDie(&tablets_by_id_, tablet_id, tablet);

  TableMetadataLock l_table(table.get(), LockMode::WRITE);
  TabletMetadataLock l_tablet(tablet.get(), LockMode::READ);
  table->AddRemoveTablets({ tablet }, {});
}

Status MasterRebuilder::CheckTabletConsistency(
    const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  const string& tablet_id = replica.tablet_status().tablet_id();
  const string& table_name = replica.tablet_status().table_name();
  scoped_refptr<TabletInfo> tablet = FindOrDie(tablets_by_id_, tablet_id);

  TabletMetadataLock l_tablet(tablet.get(), LockMode::READ);
  const SysTabletsEntryPB& metadata = tablet->metadata().state().pb;

  // Check the partitions match.
  // We do not check the schemas and partition schemas match because they are
  // already checked against the table's.
  Partition partition_from_tablet;
  Partition partition_from_replica;
  Partition::FromPB(metadata.partition(), &partition_from_tablet);
  Partition::FromPB(replica.tablet_status().partition(), &partition_from_replica);
  if (partition_from_tablet != partition_from_replica) {
    LOG(WARNING) << Substitute("Partition mismatch for tablet $0 of table $1",
                               tablet_id, table_name);
    LOG(WARNING) << Substitute("First seen partition: $0",
                               metadata.partition().DebugString());
    LOG(WARNING) << Substitute("Mismatched partition $0",
                               replica.tablet_status().partition().DebugString());;
    return Status::Corruption("inconsistent replica: partition mismatch");
  }

  return Status::OK();
}

Status MasterRebuilder::WriteSysCatalog() {
  // Start up the master and syscatalog.
  MasterOptions opts;
  Master master(opts);
  RETURN_NOT_OK(master.Init());
  SysCatalogTable sys_catalog(&master, &NoOpCb);
  SCOPED_CLEANUP({
    sys_catalog.Shutdown();
    master.Shutdown();
  });
  // TODO(awong): we could be smarter about cleaning up on failure, either by
  // initially placing data into some temp directories and moving at the end,
  // or by deleting all blocks, WALs, and metadata on error.
  Status s = sys_catalog.CreateNew(master.fs_manager());
  if (s.IsAlreadyPresent()) {
    // Adjust the error message to be clearer when running on non-empty dirs.
    s = s.CloneAndPrepend("the specified fs directories must be empty");
  }
  RETURN_NOT_OK(s);

  // Table-by-table, organize the metadata and write it to the syscatalog.
  vector<scoped_refptr<TabletInfo>> tablets;
  const auto kLeaderTimeout = MonoDelta::FromSeconds(10);
  RETURN_NOT_OK(sys_catalog.tablet_replica()->consensus()->WaitUntilLeader(kLeaderTimeout));
  for (const auto& table_entry : tables_by_name_) {
    const auto& table = table_entry.second;
    table->GetAllTablets(&tablets);
    TableMetadataLock l_table(table.get(), LockMode::WRITE);
    TabletMetadataGroupLock l_tablets(LockMode::RELEASED);
    l_tablets.AddMutableInfos(tablets);
    l_tablets.Lock(LockMode::WRITE);
    SysCatalogTable::Actions actions;
    actions.table_to_add = table;
    actions.tablets_to_add = tablets;
    RETURN_NOT_OK_PREPEND(sys_catalog.Write(actions),
                          Substitute("unable to write metadata for table $0 to sys_catalog",
                                     table_entry.first));
  }
  return Status::OK();
}

} // namespace tools
} // namespace kudu
