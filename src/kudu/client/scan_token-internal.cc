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

#include <boost/optional.hpp>
#include <vector>
#include <string>
#include <memory>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

KuduScanToken::Data::Data(KuduTable* table,
                          ScanTokenPB message,
                          vector<KuduTabletServer*> tablet_servers)
    : table_(table),
      message_(std::move(message)),
      tablet_servers_(std::move(tablet_servers)) {
}

KuduScanToken::Data::~Data() {
  ElementDeleter deleter(&tablet_servers_);
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

  sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(message.table_name(), &table));
  Schema* schema = table->schema().schema_;

  unique_ptr<KuduScanner> scan_builder(new KuduScanner(table.get()));

  vector<int> column_indexes;
  for (const ColumnSchemaPB& column : message.projected_columns()) {
    int columnIdx = schema->find_column(column.name());
    if (columnIdx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("unknown column in scan token", column.name());
    }
    DataType expectedType = schema->column(columnIdx).type_info()->type();
    if (column.type() != expectedType) {
      return Status::InvalidArgument(strings::Substitute(
            "invalid type $0 for column '$1' in scan token, expected: $2",
            column.type(), column.name(), expectedType));
    }
    column_indexes.push_back(columnIdx);
  }
  RETURN_NOT_OK(scan_builder->SetProjectedColumnIndexes(column_indexes));

  ScanConfiguration* configuration = scan_builder->data_->mutable_configuration();
  for (const ColumnPredicatePB& pb : message.column_predicates()) {
    boost::optional<ColumnPredicate> predicate;
    RETURN_NOT_OK(ColumnPredicateFromPB(*schema, configuration->arena(), pb, &predicate));
    configuration->AddConjunctPredicate(std::move(*predicate));
  }

  if (message.has_lower_bound_primary_key()) {
    RETURN_NOT_OK(scan_builder->AddLowerBoundRaw(message.lower_bound_primary_key()));
  }
  if (message.has_upper_bound_primary_key()) {
    RETURN_NOT_OK(scan_builder->AddExclusiveUpperBoundRaw(message.upper_bound_primary_key()));
  }

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
      case ReadMode::READ_LATEST: {
        RETURN_NOT_OK(scan_builder->SetReadMode(KuduScanner::READ_LATEST));
        break;
      }
      case ReadMode::READ_AT_SNAPSHOT: {
        RETURN_NOT_OK(scan_builder->SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
        break;
      }
      default: return Status::InvalidArgument("scan token has unrecognized read mode");
    }
  }

  if (message.fault_tolerant()) {
    RETURN_NOT_OK(scan_builder->SetFaultTolerant());
  }

  if (message.has_snap_timestamp()) {
    RETURN_NOT_OK(scan_builder->SetSnapshotRaw(message.snap_timestamp()));
  }

  if (message.has_propagated_timestamp()) {
    // TODO(KUDU-420)
  }

  RETURN_NOT_OK(scan_builder->SetCacheBlocks(message.cache_blocks()));

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

  ScanTokenPB pb;

  pb.set_table_name(table->name());
  RETURN_NOT_OK(SchemaToColumnPBs(*configuration_.projection(), pb.mutable_projected_columns(),
                                  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));

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

  switch (configuration_.read_mode()) {
    case KuduScanner::READ_LATEST: pb.set_read_mode(kudu::READ_LATEST); break;
    case KuduScanner::READ_AT_SNAPSHOT: pb.set_read_mode(kudu::READ_AT_SNAPSHOT); break;
    default: LOG(FATAL) << "Unexpected read mode.";
  }

  if (configuration_.snapshot_timestamp() != ScanConfiguration::kNoTimestamp) {
    if (PREDICT_FALSE(configuration_.read_mode() != KuduScanner::READ_AT_SNAPSHOT)) {
      LOG(WARNING) << "Scan token snapshot timestamp set but read mode was READ_LATEST."
                      " Ignoring timestamp.";
    } else {
      pb.set_snap_timestamp(configuration_.snapshot_timestamp());
    }
  }

  pb.set_cache_blocks(configuration_.spec().cache_blocks());
  pb.set_fault_tolerant(configuration_.is_fault_tolerant());

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(client->default_admin_operation_timeout());

  PartitionPruner pruner;
  pruner.Init(*table->schema().schema_, table->partition_schema(), configuration_.spec());
  while (pruner.HasMorePartitionKeyRanges()) {
    scoped_refptr<internal::RemoteTablet> tablet;
    Synchronizer sync;
    client->data_->meta_cache_->LookupTabletByKey(table,
                                                  pruner.NextPartitionKey(),
                                                  deadline,
                                                  &tablet,
                                                  sync.AsStatusCallback());
    RETURN_NOT_OK(sync.Wait());
    CHECK(tablet);

    vector<internal::RemoteTabletServer*> remote_tablet_servers;
    tablet->GetRemoteTabletServers(&remote_tablet_servers);

    vector<KuduTabletServer*> tablet_servers;
    ElementDeleter deleter(&tablet_servers);

    for (internal::RemoteTabletServer* remote_tablet_server : remote_tablet_servers) {
      vector<HostPort> host_ports;
      remote_tablet_server->GetHostPorts(&host_ports);
      if (host_ports.empty()) {
        return Status::IllegalState(strings::Substitute("No host found for tablet server $0",
                                                        remote_tablet_server->ToString()));
      }
      KuduTabletServer* tablet_server = new KuduTabletServer;
      tablet_server->data_ = new KuduTabletServer::Data(remote_tablet_server->permanent_uuid(),
                                                        host_ports[0].host());
      tablet_servers.push_back(tablet_server);
    }
    ScanTokenPB message;
    message.CopyFrom(pb);
    message.set_lower_bound_partition_key(tablet->partition().partition_key_start());
    message.set_upper_bound_partition_key(tablet->partition().partition_key_end());
    tokens->push_back(new KuduScanToken(new KuduScanToken::Data(table,
                                                                std::move(message),
                                                                std::move(tablet_servers))));
    pruner.RemovePartitionKeyRange(tablet->partition().partition_key_end());
  }
  return Status::OK();
}

} // namespace client
} // namespace kudu
