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
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <gflags/gflags.h>
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
#include "kudu/gutil/strings/split.h"
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
DEFINE_uint32(default_schema_version, 0, "The table schema version assigned to tables if one "
              "cannot be determined automatically. When the tablet server version is < 1.16, a "
              "viable value can be determined manually using "
              "'kudu pbc dump tablet-meta/<tablet-id>' on each server and taking the max value "
              "found across all tablets. In tablet server versions >= 1.16, Kudu will determine "
              "the proper value for each tablet automatically.");
DECLARE_string(tables);

using kudu::master::Master;
using kudu::master::MasterOptions;
using kudu::master::SysCatalogTable;
using kudu::master::SysTablesEntryPB;
using kudu::master::SysTabletsEntryPB;
using kudu::master::TableInfo;
using kudu::master::TableInfoLoader;
using kudu::master::TableMetadataLock;
using kudu::master::TabletInfo;
using kudu::master::TabletInfoLoader;
using kudu::master::TabletMetadataGroupLock;
using kudu::master::TabletMetadataLock;
using kudu::tserver::ListTabletsResponsePB;
using std::map;
using std::set;
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

static Status DoWrite(SysCatalogTable* sys_catalog,
                      const map<string, scoped_refptr<master::TableInfo>>& table_by_name,
                      SysCatalogTable::SysCatalogOperation operation,
                      map<string, vector<scoped_refptr<TabletInfo>>>* tablet_by_name = nullptr) {
  if (table_by_name.empty()) return Status::OK();
  const auto kLeaderTimeout = MonoDelta::FromSeconds(10);
  RETURN_NOT_OK(sys_catalog->tablet_replica()->consensus()->WaitUntilLeader(kLeaderTimeout));
  for (const auto& table_entry : table_by_name) {
    const auto& table = table_entry.second;
    vector<scoped_refptr<TabletInfo>> tablets;
    if (tablet_by_name == nullptr ||
        tablet_by_name->empty()) {
      table->GetAllTablets(&tablets);
    } else {
      if (ContainsKey(*tablet_by_name, table_entry.first))
        tablets = (*tablet_by_name)[table_entry.first];
    }
    TableMetadataLock l_table(table.get(), LockMode::WRITE);
    TabletMetadataGroupLock l_tablets(LockMode::RELEASED);
    l_tablets.AddMutableInfos(tablets);
    l_tablets.Lock(LockMode::WRITE);
    SysCatalogTable::Actions actions;
    switch (operation) {
      case SysCatalogTable::SysCatalogOperation::ADD:
      {
        actions.table_to_add = table;
        actions.tablets_to_add = tablets;
        break;
      }
      case SysCatalogTable::SysCatalogOperation::UPDATE:
      {
        actions.table_to_update = table;
        actions.tablets_to_update = tablets;
        break;
      }
      case SysCatalogTable::SysCatalogOperation::DELETE:
      {
        actions.table_to_delete = table;
        actions.tablets_to_delete = tablets;
        break;
      }
      default:
        return Status::InvalidArgument(Substitute("Operation:$ is not supported.",
                                                  operation));
    }
    RETURN_NOT_OK_PREPEND(sys_catalog->Write(actions),
                          Substitute("unable to write metadata for table $0 to sys_catalog",
                                     table_entry.first));
  }
  return Status::OK();
}

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
  const set<string>& filter_tables = strings::Split(FLAGS_tables, ",",
                                                    strings::SkipWhitespace());
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
      if (!filter_tables.empty() && !ContainsKey(filter_tables, table_name)) continue;
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
        // TODO(yingchun): should abort rebuilding master if any fatal error happened.
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
  if (!filter_tables.empty()) {
    RETURN_NOT_OK(UpsertSysCatalog());
  } else {
    // Now that we've assembled all the metadata, we can write to a syscatalog table.
    RETURN_NOT_OK(WriteSysCatalog());
  }
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
  SysTablesEntryPB* metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  const string& table_name = replica.tablet_status().table_name();
  metadata->set_name(table_name);
  if (!replica.has_schema_version()) {
    metadata->set_version(FLAGS_default_schema_version);
  } else {
    metadata->set_version(replica.schema_version());
  }

  // We can't tell the replication factor from ListTablets.
  // We'll guess the default replication factor because it's safe and almost
  // always correct.
  // TODO(awong): there's probably a better heuristic based on the number of
  // replicas reported by the tablet servers.
  metadata->set_num_replicas(FLAGS_default_num_replicas);
  metadata->mutable_schema()->CopyFrom(replica.schema());
  metadata->mutable_partition_schema()->CopyFrom(replica.partition_schema());

  int32_t max_column_id = 0;
  for (const auto& column : replica.schema().columns()) {
    if (column.has_id() && max_column_id < column.id()) {
      max_column_id = column.id();
    }
  }
  metadata->set_next_column_id(max_column_id + 1);
  metadata->set_state(SysTablesEntryPB::RUNNING);
  metadata->set_state_msg("reconstructed by MasterRebuilder");
  table->mutable_metadata()->CommitMutation();
  InsertOrDie(&tables_by_name_, table_name, table);
}

Status MasterRebuilder::CheckTableConsistency(
    const ListTabletsResponsePB::StatusAndSchemaPB& replica) {
  const string& tablet_id = replica.tablet_status().tablet_id();
  const string& table_name = replica.tablet_status().table_name();
  Schema schema_from_replica;
  RETURN_NOT_OK(SchemaFromPB(replica.schema(), &schema_from_replica));

  scoped_refptr<TableInfo> table = FindOrDie(tables_by_name_, table_name);
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB* metadata = &table->mutable_metadata()->mutable_dirty()->pb;

  // Update next_column_id if needed.
  if (schema_from_replica.max_col_id() + 1 > metadata->next_column_id()) {
    metadata->set_next_column_id(schema_from_replica.max_col_id() + 1);
  }

  // Update schema if needed.
  // Suppose the schema with a higher version would be newer, we will replace
  // the schema_version, SchemaPB and PartitionSchemaPB.
  if (replica.has_schema_version() &&
      replica.schema_version() > metadata->version()) {
    metadata->set_version(replica.schema_version());
    metadata->mutable_schema()->CopyFrom(replica.schema());
    metadata->mutable_partition_schema()->CopyFrom(replica.partition_schema());
  }

  // Obtain Schema and PartitionSchema before CommitMutation, since
  // CommitMutation will reset metadata.
  int table_version = metadata->version();
  Schema schema_from_table;
  RETURN_NOT_OK(SchemaFromPB(metadata->schema(), &schema_from_table));
  PartitionSchema pschema_from_table;
  RETURN_NOT_OK(PartitionSchema::FromPB(
      metadata->partition_schema(), schema_from_table, &pschema_from_table));
  table->mutable_metadata()->CommitMutation();

  // Check for the consistency.
  // Only matched when they have the same version.
  if (replica.has_schema_version() &&
      replica.schema_version() != table_version) {
    LOG(WARNING) << Substitute(
        "Ignoring mismatched schema version for tablet $0 of table $1", tablet_id, table_name);
    LOG(WARNING) << Substitute("Table schema version: $0", table_version);
    LOG(WARNING) << Substitute("Mismatched schema version: $0", replica.schema_version());
    return Status::OK();
  }

  string error_message;
  // Check the schemas match.
  if (schema_from_table != schema_from_replica) {
    error_message = "schema mismatch";
    LOG(WARNING) << Substitute(
        "Schema mismatch for tablet $0 of table $1", tablet_id, table_name);
    LOG(WARNING) << Substitute("Table schema: $0", schema_from_table.ToString());
    LOG(WARNING) << Substitute("Mismatched schema: $0", schema_from_replica.ToString());
  }

  // Check the partition schemas match.
  PartitionSchema pschema_from_replica;
  RETURN_NOT_OK(PartitionSchema::FromPB(
      replica.partition_schema(), schema_from_replica, &pschema_from_replica));
  if (pschema_from_table != pschema_from_replica) {
    if (!error_message.empty()) {
      error_message += ", ";
    }
    error_message += "partition schema mismatch";
    LOG(WARNING) << Substitute(
        "Partition schema mismatch for tablet $0 of table $1", tablet_id, table_name);
    LOG(WARNING) << Substitute("First seen partition schema: $0",
                               pschema_from_table.DebugString(schema_from_table));
    LOG(WARNING) << Substitute("Mismatched partition schema $0",
                               pschema_from_replica.DebugString(schema_from_replica));
  }

  if (!error_message.empty()) {
    return Status::Corruption(Substitute("inconsistent replica: $0", error_message));
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
                               replica.tablet_status().partition().DebugString());
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
  RETURN_NOT_OK(DoWrite(&sys_catalog, tables_by_name_,
                        SysCatalogTable::SysCatalogOperation::ADD));
  return Status::OK();
}

Status MasterRebuilder::UpsertSysCatalog() {
  MasterOptions opts;
  Master master(opts);

  RETURN_NOT_OK(master.Init());
  SysCatalogTable sys_catalog(&master, &NoOpCb);
  RETURN_NOT_OK(sys_catalog.Load(master.fs_manager()));
  SCOPED_CLEANUP({
    sys_catalog.Shutdown();
    master.Shutdown();
  });
  // Get all table in syscatalog.
  TableInfoLoader table_info_loader;
  sys_catalog.VisitTables(&table_info_loader);
  map<string, scoped_refptr<master::TableInfo>> table_by_name_in_syscatalog;
  for (const auto& table : table_info_loader.tables) {
    table->metadata().ReadLock();
    InsertOrDie(&table_by_name_in_syscatalog, table->metadata().state().name(),
                table);
    table->metadata().ReadUnlock();
  }
  // Get all tablets in master.
  TabletInfoLoader tablet_info_loader;
  sys_catalog.VisitTablets(&tablet_info_loader);

  string new_table_name;
  map<string, scoped_refptr<master::TableInfo>> table_by_name_to_delete;
  map<string, vector<scoped_refptr<TabletInfo>>> tablet_by_name_to_delete;
  map<string, scoped_refptr<master::TableInfo>> table_by_name_to_add;
  for (const auto& table_entry : tables_by_name_) {
    table_entry.second->metadata().ReadLock();
    new_table_name = table_entry.second->metadata().state().name();
    table_entry.second->metadata().ReadUnlock();
    if (ContainsKey(table_by_name_in_syscatalog, new_table_name)) {
      InsertOrDie(&table_by_name_to_delete, new_table_name,
                  table_by_name_in_syscatalog[new_table_name]);
      // Get all tablets for this table.
      vector<scoped_refptr<TabletInfo>> tablets;
      for (const auto& tablet : tablet_info_loader.tablets) {
        tablet->metadata().ReadLock();
        if (tablet->metadata().state().pb.table_id() ==
            table_by_name_in_syscatalog[new_table_name]->id())
          tablets.push_back(tablet);
        tablet->metadata().ReadUnlock();
      }
      InsertOrDie(&tablet_by_name_to_delete, new_table_name, tablets);
    }
    InsertOrDie(&table_by_name_to_add, table_entry.first, table_entry.second);
  }

  // If a table found in master, delete it then add it.
  RETURN_NOT_OK(DoWrite(&sys_catalog, table_by_name_to_delete,
                        SysCatalogTable::SysCatalogOperation::DELETE,
                        &tablet_by_name_to_delete));
  RETURN_NOT_OK(DoWrite(&sys_catalog, table_by_name_to_add,
                        SysCatalogTable::SysCatalogOperation::ADD));
  return Status::OK();
}
} // namespace tools
} // namespace kudu
