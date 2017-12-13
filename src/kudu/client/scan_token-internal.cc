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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/replica-internal.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/tablet-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
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
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {

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

  sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(message.table_name(), &table));
  Schema* schema = table->schema().schema_;

  unique_ptr<KuduScanner> scan_builder(new KuduScanner(table.get()));

  vector<int> column_indexes;
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

  if (message.has_snap_timestamp()) {
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

  if (configuration_.has_batch_size_bytes()) {
    pb.set_batch_size_bytes(configuration_.batch_size_bytes());
  }

  MonoTime deadline = MonoTime::Now() + client->default_admin_operation_timeout();

  PartitionPruner pruner;
  pruner.Init(*table->schema().schema_, table->partition_schema(), configuration_.spec());
  while (pruner.HasMorePartitionKeyRanges()) {
    scoped_refptr<internal::RemoteTablet> tablet;
    Synchronizer sync;
    const string& partition_key = pruner.NextPartitionKey();
    client->data_->meta_cache_->LookupTabletByKeyOrNext(table,
                                                        partition_key,
                                                        deadline,
                                                        &tablet,
                                                        sync.AsStatusCallback(),
                                                        internal::kFetchTabletsPerRangeLookup);
    Status s = sync.Wait();
    if (s.IsNotFound()) {
      // No more tablets in the table.
      pruner.RemovePartitionKeyRange("");
      continue;
    } else {
      RETURN_NOT_OK(s);
    }

    // Check if the meta cache returned a tablet covering a partition key range past
    // what we asked for. This can happen if the requested partition key falls
    // in a non-covered range. In this case we can potentially prune the tablet.
    if (partition_key < tablet->partition().partition_key_start() &&
        pruner.ShouldPrune(tablet->partition())) {
      pruner.RemovePartitionKeyRange(tablet->partition().partition_key_end());
      continue;
    }

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
                                                    host_ports[0]);
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
    message.set_lower_bound_partition_key(
        tablet->partition().partition_key_start());
    message.set_upper_bound_partition_key(
        tablet->partition().partition_key_end());
    unique_ptr<KuduScanToken> client_scan_token(new KuduScanToken);
    client_scan_token->data_ =
        new KuduScanToken::Data(table,
                                std::move(message),
                                std::move(client_tablet));
    tokens->push_back(client_scan_token.release());
    pruner.RemovePartitionKeyRange(tablet->partition().partition_key_end());
  }
  return Status::OK();
}

} // namespace client
} // namespace kudu
