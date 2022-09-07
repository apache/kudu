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

#include "kudu/client/scan_token-internal.h"

#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/port.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/replica-internal.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/tablet-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/key_range.h"
#include "kudu/common/partition.h"
#include "kudu/common/partition_pruner.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::string;
using std::map;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using master::GetTableLocationsResponsePB;
using master::TableIdentifierPB;
using master::TabletLocationsPB;
using master::TSInfoPB;
using security::SignedTokenPB;

namespace client {

using internal::MetaCache;
using internal::MetaCacheEntry;
using internal::RemoteReplica;
using internal::RemoteTabletServer;

KuduScanToken::Data::Data(KuduTable* table,
                          ScanTokenPB message,
                          unique_ptr<KuduTablet> tablet)
    : table_(table),
      message_(std::move(message)),
      tablet_(std::move(tablet)) {
}

Status KuduScanToken::Data::IntoKuduScanner(KuduScanner** scanner) const {
  return PBIntoScanner(table_->client(), message_, scanner);
}

Status KuduScanToken::Data::Serialize(string* buf) const {
  if (!message_.SerializeToString(buf)) {
    return Status::Corruption("unable to serialize scan token");
  }
  return Status::OK();
}

Status KuduScanToken::Data::DeserializeIntoScanner(KuduClient* client,
                                                   const std::string& serialized_token,
                                                   KuduScanner** scanner) {
  ScanTokenPB message;
  if (!message.ParseFromString(serialized_token)) {
    return Status::Corruption("unable to deserialize scan token");
  }
  return PBIntoScanner(client, message, scanner);
}

Status KuduScanToken::Data::PBIntoScanner(KuduClient* client,
                                          const ScanTokenPB& message,
                                          KuduScanner** scanner) {
  for (int32_t feature : message.feature_flags()) {
    if (!ScanTokenPB::Feature_IsValid(feature) || feature == ScanTokenPB::Unknown) {
      return Status::NotSupported(
          "scan token requires features not supported by this client version");
    }
  }

  // Use the table metadata from the scan token if it exists,
  // otherwise call OpenTable to get the metadata from the master.
  sp::shared_ptr<KuduTable> table;
  if (message.has_table_metadata()) {
    const TableMetadataPB& metadata = message.table_metadata();
    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(metadata.schema(), &schema));
    KuduSchema kudu_schema(schema);
    PartitionSchema partition_schema;
    RETURN_NOT_OK(PartitionSchema::FromPB(metadata.partition_schema(), schema,
                                          &partition_schema));
    map<string, string> extra_configs(metadata.extra_configs().begin(),
        metadata.extra_configs().end());
    table.reset(new KuduTable(client->shared_from_this(), metadata.table_name(),
        metadata.table_id(), metadata.num_replicas(), metadata.owner(), metadata.comment(),
        kudu_schema, partition_schema, extra_configs));
  } else {
    TableIdentifierPB table_identifier;
    if (message.has_table_id()) {
      table_identifier.set_table_id(message.table_id());
    }
    if (message.has_table_name()) {
      table_identifier.set_table_name(message.table_name());
    }
    RETURN_NOT_OK(client->data_->OpenTable(client, table_identifier, &table));
  }

  // Prime the client tablet location cache if no entry is already present.
  if (message.has_tablet_metadata()) {
    const TabletMetadataPB& tablet_metadata = message.tablet_metadata();
    Partition partition;
    Partition::FromPB(tablet_metadata.partition(), &partition);
    MetaCacheEntry entry;
    if (!client->data_->meta_cache_->LookupEntryByKeyFastPath(table.get(),
        partition.begin(), &entry)) {
      // Generate a fake GetTableLocationsResponsePB to pass to the client
      // meta cache in order to "inject" the tablet metadata into the client.
      GetTableLocationsResponsePB mock_resp;
      mock_resp.set_ttl_millis(tablet_metadata.ttl_millis());

      // Populate the locations.
      TabletLocationsPB locations_pb;
      locations_pb.set_tablet_id(tablet_metadata.tablet_id());
      PartitionPB partition_pb;
      partition.ToPB(&partition_pb);
      *locations_pb.mutable_partition() = std::move(partition_pb);
      for (const TabletMetadataPB::ReplicaMetadataPB& replica_meta : tablet_metadata.replicas()) {
        TabletLocationsPB::InternedReplicaPB replica_pb;
        replica_pb.set_ts_info_idx(replica_meta.ts_idx());
        replica_pb.set_role(replica_meta.role());
        if (replica_meta.has_dimension_label()) {
          replica_pb.set_dimension_label(replica_meta.dimension_label());
        }
        *locations_pb.add_interned_replicas() = std::move(replica_pb);
      }
      *mock_resp.add_tablet_locations() = std::move(locations_pb);

      // Populate the servers.
      for (const ServerMetadataPB& server_meta : tablet_metadata.tablet_servers()) {
        TSInfoPB server_pb;
        server_pb.set_permanent_uuid(server_meta.uuid());
        server_pb.set_location(server_meta.location());
        for (const HostPortPB& host_port :server_meta.rpc_addresses()) {
          *server_pb.add_rpc_addresses() = host_port;
        }
        *mock_resp.add_ts_infos() = std::move(server_pb);
      }

      client->data_->meta_cache_->ProcessGetTableLocationsResponse(
          table.get(), partition.begin(), true, mock_resp, &entry, 1);
    }
  }

  if (message.has_authz_token()) {
    client->data_->StoreAuthzToken(table->id(), message.authz_token());
  }

  Schema* schema = table->schema().schema_;

  unique_ptr<KuduScanner> scan_builder(new KuduScanner(table.get()));

  vector<int> column_indexes;
  if (!message.projected_column_idx().empty()) {
    for (const int column_idx : message.projected_column_idx()) {
      column_indexes.push_back(column_idx);
    }
  } else {
    for (const ColumnSchemaPB& column : message.projected_columns()) {
      int column_idx = schema->find_column(column.name());
      if (column_idx == Schema::kColumnNotFound) {
        return Status::IllegalState("unknown column in scan token", column.name());
      }
      DataType expected_type = schema->column(column_idx).type_info()->type();
      if (column.type() != expected_type) {
        return Status::IllegalState(Substitute(
            "invalid type $0 for column '$1' in scan token, expected: $2",
            DataType_Name(column.type()), column.name(), DataType_Name(expected_type)));
      }
      bool expected_is_nullable = schema->column(column_idx).is_nullable();
      if (column.is_nullable() != expected_is_nullable) {
        return Status::IllegalState(Substitute(
            "invalid nullability for column '$0' in scan token, expected: $1",
            column.name(), expected_is_nullable ? "NULLABLE" : "NOT NULL"));
      }
      column_indexes.push_back(column_idx);
    }
  }
  RETURN_NOT_OK(scan_builder->SetProjectedColumnIndexes(column_indexes));

  ScanConfiguration* configuration = scan_builder->data_->mutable_configuration();
  for (const ColumnPredicatePB& pb : message.column_predicates()) {
    std::optional<ColumnPredicate> predicate;
    RETURN_NOT_OK(ColumnPredicateFromPB(*schema, configuration->arena(), pb, &predicate));
    configuration->AddConjunctPredicate(std::move(*predicate));
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  if (message.has_lower_bound_primary_key()) {
    RETURN_NOT_OK(scan_builder->AddLowerBoundRaw(message.lower_bound_primary_key()));
  }
  if (message.has_upper_bound_primary_key()) {
    RETURN_NOT_OK(scan_builder->AddExclusiveUpperBoundRaw(message.upper_bound_primary_key()));
  }
#pragma GCC diagnostic pop

  if (message.has_lower_bound_partition_key()) {
    RETURN_NOT_OK(scan_builder->AddLowerBoundPartitionKeyRaw(message.lower_bound_partition_key()));
  }
  if (message.has_upper_bound_partition_key()) {
    RETURN_NOT_OK(scan_builder->AddExclusiveUpperBoundPartitionKeyRaw(
          message.upper_bound_partition_key()));
  }

  if (message.has_limit()) {
    // TODO(KUDU-16)
  }

  if (message.has_read_mode()) {
    switch (message.read_mode()) {
      case ReadMode::READ_LATEST:
        RETURN_NOT_OK(scan_builder->SetReadMode(KuduScanner::READ_LATEST));
        break;
      case ReadMode::READ_AT_SNAPSHOT:
        RETURN_NOT_OK(scan_builder->SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
        break;
      case ReadMode::READ_YOUR_WRITES:
        RETURN_NOT_OK(scan_builder->SetReadMode(KuduScanner::READ_YOUR_WRITES));
        break;
      default:
        return Status::InvalidArgument("scan token has unrecognized read mode");
    }
  }

  if (message.fault_tolerant()) {
    RETURN_NOT_OK(scan_builder->SetFaultTolerant());
  }

  if (message.has_snap_start_timestamp() && message.has_snap_timestamp()) {
    RETURN_NOT_OK(scan_builder->SetDiffScan(message.snap_start_timestamp(),
                                            message.snap_timestamp()));
  } else if (message.has_snap_timestamp()) {
    RETURN_NOT_OK(scan_builder->SetSnapshotRaw(message.snap_timestamp()));
  }

  RETURN_NOT_OK(scan_builder->SetCacheBlocks(message.cache_blocks()));

  // Since the latest observed timestamp from the given client might be
  // more recent than the one when the token is created, the performance
  // of the scan could be affected if using READ_YOUR_WRITES mode.
  //
  // We choose to keep it this way because the code path is simpler.
  // Beside, in practice it's very rarely the case that an active client
  // is permanently being written to and read from (using scan tokens).
  //
  // However it is worth to note that this is a possible optimization, if
  // we ever notice READ_YOUR_WRITES read stalling with scan tokens.
  if (message.has_propagated_timestamp()) {
    client->data_->UpdateLatestObservedTimestamp(message.propagated_timestamp());
  }

  if (message.has_batch_size_bytes()) {
    RETURN_NOT_OK(scan_builder->SetBatchSizeBytes(message.batch_size_bytes()));
  }

  if (message.has_scan_request_timeout_ms()) {
    scan_builder->SetTimeoutMillis(message.scan_request_timeout_ms());
  }

  *scanner = scan_builder.release();
  return Status::OK();
}

KuduScanTokenBuilder::Data::Data(KuduTable* table)
    : configuration_(table) {
}

Status KuduScanTokenBuilder::Data::Build(vector<KuduScanToken*>* tokens) {
  KuduTable* table = configuration_.table_;
  KuduClient* client = table->client();
  configuration_.OptimizeScanSpec();

  if (configuration_.spec().CanShortCircuit()) {
    return Status::OK();
  }

  ScanTokenPB pb;

  if (include_table_metadata_) {
    // Set the table metadata so that a call to the master is not needed when
    // deserializing the token into a scanner.
    TableMetadataPB table_pb;
    table_pb.set_table_id(table->id());
    table_pb.set_table_name(table->name());
    table_pb.set_num_replicas(table->num_replicas());
    table_pb.set_owner(table->owner());
    table_pb.set_comment(table->comment());
    SchemaPB schema_pb;
    RETURN_NOT_OK(SchemaToPB(KuduSchema::ToSchema(table->schema()), &schema_pb));
    *table_pb.mutable_schema() = std::move(schema_pb);
    PartitionSchemaPB partition_schema_pb;
    RETURN_NOT_OK(table->partition_schema().ToPB(KuduSchema::ToSchema(table->schema()),
                                                 &partition_schema_pb));
    table_pb.mutable_partition_schema()->CopyFrom(partition_schema_pb);
    table_pb.mutable_extra_configs()->insert(table->extra_configs().begin(),
                                             table->extra_configs().end());
    *pb.mutable_table_metadata() = std::move(table_pb);

    // Only include the authz token if the table metadata is included.
    // It is returned in the required GetTableSchema request otherwise.
    SignedTokenPB authz_token;
    bool found_authz_token = client->data_->FetchCachedAuthzToken(table->id(), &authz_token);
    if (found_authz_token) {
      *pb.mutable_authz_token() = std::move(authz_token);
    }
  } else {
    // If we add the table metadata, we don't need to set the old table id
    // and table name. It is expected that the creation and use of a scan token
    // will be on the same or compatible versions.
    pb.set_table_id(table->id());
    pb.set_table_name(table->name());
  }

  if (include_table_metadata_) {
    for (const ColumnSchema& col : configuration_.projection()->columns()) {
      int column_idx;
      table->schema().schema_->FindColumn(col.name(), &column_idx);
      pb.mutable_projected_column_idx()->Add(column_idx);
    }
  } else {
    RETURN_NOT_OK(SchemaToColumnPBs(*configuration_.projection(), pb.mutable_projected_columns(),
        SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));
  }

  if (configuration_.spec().lower_bound_key()) {
    pb.mutable_lower_bound_primary_key()->assign(
      reinterpret_cast<const char*>(configuration_.spec().lower_bound_key()->encoded_key().data()),
      configuration_.spec().lower_bound_key()->encoded_key().size());
  } else {
    pb.clear_lower_bound_primary_key();
  }
  if (configuration_.spec().exclusive_upper_bound_key()) {
    pb.mutable_upper_bound_primary_key()->assign(reinterpret_cast<const char*>(
          configuration_.spec().exclusive_upper_bound_key()->encoded_key().data()),
      configuration_.spec().exclusive_upper_bound_key()->encoded_key().size());
  } else {
    pb.clear_upper_bound_primary_key();
  }

  for (const auto& predicate_pair : configuration_.spec().predicates()) {
    ColumnPredicateToPB(predicate_pair.second, pb.add_column_predicates());
  }

  const KuduScanner::ReadMode read_mode = configuration_.read_mode();
  switch (read_mode) {
    case KuduScanner::READ_LATEST:
      pb.set_read_mode(kudu::READ_LATEST);
      if (configuration_.has_snapshot_timestamp()) {
        return Status::InvalidArgument("Snapshot timestamp should only be configured "
                                       "for READ_AT_SNAPSHOT scan mode.");
      }
      break;
    case KuduScanner::READ_AT_SNAPSHOT:
      pb.set_read_mode(kudu::READ_AT_SNAPSHOT);
      if (configuration_.has_start_timestamp()) {
        pb.set_snap_start_timestamp(configuration_.start_timestamp());
      }
      if (configuration_.has_snapshot_timestamp()) {
        pb.set_snap_timestamp(configuration_.snapshot_timestamp());
      }
      break;
    case KuduScanner::READ_YOUR_WRITES:
      pb.set_read_mode(kudu::READ_YOUR_WRITES);
      if (configuration_.has_snapshot_timestamp()) {
        return Status::InvalidArgument("Snapshot timestamp should only be configured "
                                       "for READ_AT_SNAPSHOT scan mode.");
      }
      break;
    default:
      LOG(FATAL) << Substitute("$0: unexpected read mode", read_mode);
  }

  pb.set_cache_blocks(configuration_.spec().cache_blocks());
  pb.set_fault_tolerant(configuration_.is_fault_tolerant());
  pb.set_propagated_timestamp(client->GetLatestObservedTimestamp());
  pb.set_scan_request_timeout_ms(configuration_.timeout().ToMilliseconds());

  if (configuration_.has_batch_size_bytes()) {
    pb.set_batch_size_bytes(configuration_.batch_size_bytes());
  }

  PartitionPruner pruner;
  vector<MetaCache::RangeWithRemoteTablet> range_tablets;
  pruner.Init(*table->schema().schema_, table->partition_schema(), configuration_.spec());
  while (pruner.HasMorePartitionKeyRanges()) {
    PartitionKey key_range;
    vector<MetaCache::RangeWithRemoteTablet> tmp_range_tablets;
    const auto& partition_key = pruner.NextPartitionKey();
    Status s = client->data_->meta_cache_->GetTableKeyRanges(
        table,
        partition_key,
        MetaCache::LookupType::kLowerBound,
        split_size_bytes_,
        client->default_rpc_timeout(),
        &tmp_range_tablets);

    if (s.IsNotFound()) {
      pruner.RemovePartitionKeyRange({});
      continue;
    }
    RETURN_NOT_OK(s);

    if (tmp_range_tablets.empty()) {
      pruner.RemovePartitionKeyRange(partition_key);
    } else {
      // If split_size_bytes_ set to zero, we just do search in meta cache.
      // Check if the meta cache returned a tablet covering a partition key range past
      // what we asked for. This can happen if the requested partition key falls
      // in a non-covered range. In this case we can potentially prune the tablet.
      if (split_size_bytes_ == 0 &&
          partition_key < tmp_range_tablets.back().remote_tablet->partition().begin() &&
          pruner.ShouldPrune(tmp_range_tablets.back().remote_tablet->partition())) {
        pruner.RemovePartitionKeyRange(tmp_range_tablets.back().remote_tablet->partition().end());
        continue;
      }
      for (auto& range_tablet : tmp_range_tablets) {
        range_tablets.push_back(range_tablet);
      }
      pruner.RemovePartitionKeyRange(tmp_range_tablets.back().remote_tablet->partition().end());
    }
  }

  for (const auto& range_tablet : range_tablets) {
    const auto& range = range_tablet.key_range;
    const auto& tablet = range_tablet.remote_tablet;

    vector<internal::RemoteReplica> replicas;
    tablet->GetRemoteReplicas(&replicas);

    vector<const KuduReplica*> client_replicas;
    ElementDeleter deleter(&client_replicas);

    // Convert the replicas from their internal format to something appropriate
    // for clients.
    for (const auto& r : replicas) {
      vector<HostPort> host_ports;
      r.ts->GetHostPorts(&host_ports);
      if (host_ports.empty()) {
        return Status::IllegalState(Substitute(
            "No host found for tablet server $0", r.ts->ToString()));
      }
      unique_ptr<KuduTabletServer> client_ts(new KuduTabletServer);
      client_ts->data_ = new KuduTabletServer::Data(r.ts->permanent_uuid(),
                                                    host_ports[0],
                                                    r.ts->location());
      bool is_leader = r.role == consensus::RaftPeerPB::LEADER;
      bool is_voter = is_leader || r.role == consensus::RaftPeerPB::FOLLOWER;
      unique_ptr<KuduReplica> client_replica(new KuduReplica);
      client_replica->data_ = new KuduReplica::Data(is_leader, is_voter,
                                                    std::move(client_ts));
      client_replicas.push_back(client_replica.release());
    }

    unique_ptr<KuduTablet> client_tablet(new KuduTablet);
    client_tablet->data_ = new KuduTablet::Data(tablet->tablet_id(),
                                                std::move(client_replicas));
    client_replicas.clear();

    // Create the scan token itself.
    ScanTokenPB message;
    message.CopyFrom(pb);
    message.set_lower_bound_partition_key(tablet->partition().begin().ToString());
    message.set_upper_bound_partition_key(tablet->partition().end().ToString());
    if (!range.start_primary_key().empty() && split_size_bytes_) {
      message.clear_lower_bound_primary_key();
      message.set_lower_bound_primary_key(range.start_primary_key());
    }
    if (!range.stop_primary_key().empty() && split_size_bytes_) {
      message.clear_upper_bound_primary_key();
      message.set_upper_bound_primary_key(range.stop_primary_key());
    }


    // Set the tablet metadata so that a call to the master is not needed to
    // locate the tablet to scan when opening the scanner.
    if (include_tablet_metadata_) {
      internal::MetaCacheEntry entry;
      if (client->data_->meta_cache_->LookupEntryByKeyFastPath(table,
          tablet->partition().begin(), &entry)) {
        if (!entry.is_non_covered_range() && !entry.stale()) {
          TabletMetadataPB tablet_pb;
          tablet_pb.set_tablet_id(entry.tablet()->tablet_id());
          PartitionPB partition_pb;
          entry.tablet()->partition().ToPB(&partition_pb);
          *tablet_pb.mutable_partition() = std::move(partition_pb);
          MonoDelta ttl = entry.expiration_time() - MonoTime::Now();
          tablet_pb.set_ttl_millis(ttl.ToMilliseconds());

          // Build the list of server metadata.
          vector<RemoteTabletServer*> servers;
          map<string, int> server_index_map;
          entry.tablet()->GetRemoteTabletServers(&servers);
          for (int i = 0; i < servers.size(); i++) {
            RemoteTabletServer* server = servers[i];
            ServerMetadataPB server_pb;
            server_pb.set_uuid(server->permanent_uuid());
            server_pb.set_location(server->location());
            vector<HostPort> host_ports;
            server->GetHostPorts(&host_ports);
            for (const HostPort& host_port : host_ports) {
              *server_pb.add_rpc_addresses() = HostPortToPB(host_port);
              server_index_map[host_port.ToString()] = i;
            }
            *tablet_pb.add_tablet_servers() = std::move(server_pb);
          }

          // Build the list of replica metadata.
          vector<RemoteReplica> replicas;
          entry.tablet()->GetRemoteReplicas(&replicas);
          for (const RemoteReplica& replica : replicas) {
            vector<HostPort> host_ports;
            replica.ts->GetHostPorts(&host_ports);
            int server_index = server_index_map[host_ports[0].ToString()];
            TabletMetadataPB::ReplicaMetadataPB replica_pb;
            replica_pb.set_role(replica.role);
            replica_pb.set_ts_idx(server_index);
            *tablet_pb.add_replicas() = std::move(replica_pb);
          }

          *message.mutable_tablet_metadata() = std::move(tablet_pb);
        }
      }
    }

    unique_ptr<KuduScanToken> client_scan_token(new KuduScanToken);
    client_scan_token->data_ =
        new KuduScanToken::Data(table,
                                std::move(message),
                                std::move(client_tablet));
    tokens->push_back(client_scan_token.release());
  }

  return Status::OK();
}

} // namespace client
} // namespace kudu
