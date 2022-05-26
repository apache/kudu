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

#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/catalog_manager.h" // IWYU pragma: keep
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/status.h"

namespace kudu {

namespace tools {
// Object for accumulating information about the rebuilding process.
struct RebuildReport {
  // List of (address, status) for each tablet server contacted.
  std::vector<std::pair<std::string, Status>> tservers;

  // Map (table name, tablet id, tserver address) -> status for each replica processed.
  std::map<std::tuple<std::string, std::string, std::string>, Status> replicas;
};

// Class that reconstructs a syscatalog table from tablet servers using the
// metadata returned by ListTablets.
//
// In many cases, it can create a syscatalog table that a master can use to
// restore a cluster after a catastrophic failure of the previous masters.
// It makes a best effort to restore as much as possible. It has the following
// limitations:
// - Security metadata like cryptographic keys are not rebuilt. Tablet servers
//   and clients must be restarted before starting the new master in order to
//   communicate with the new master.
// - Table IDs are known only by the masters. Reconstructed tables will have
//   new IDs.
// - If the table was in the process of being created, it may not be possible to
//   restore the table and it may be stuck in a half-created state.
// - If a table was in the process of being deleted, it may be partially
//   restored and the delete request may need to be sent again.
// - If an alter table was in progress when the masters were lost, it may not
//   be possible to restore the table.
// - If all replicas of a tablet are missing, it may not be able to recover the
//   table fully. Moreover, the MasterRebuilder cannot detect that a tablet is
//   missing.
// - It's not possible to determine the replication factor of a table from tablet
//   server metadata. The MasterRebuilder sets the replication factor of each
//   table to --default_num_replicas instead.
// - It's not possible to determine the next column id for a table from tablet
//   server metadata. Instead, the MasterRebuilder sets the next column id to
//   a very large number to prevent potential conflict.
// - Table metadata like comments, owners, and configurations are not stored on
//   tablet servers and are thus not restored.
class MasterRebuilder {
 public:
  explicit MasterRebuilder(std::vector<std::string> tserver_addrs);
  ~MasterRebuilder() = default;

  // Returns the RebuildReport for this MasterRebuilder.
  //
  // RebuildMaster() must succeed before calling this method.
  const RebuildReport& GetRebuildReport() const;

  // Gathers metadata from tablet servers and builds a syscatalog table from it.
  Status RebuildMaster();

 private:
  enum State {
    NOT_DONE,
    DONE,
  };

  // Examine the metadata of a tablet replica and merge it with the metadata
  // from previously processed replicas.
  Status CheckTableAndTabletConsistency(
      const tserver::ListTabletsResponsePB::StatusAndSchemaPB& replica);

  // Create the metadata for a new table or tablet from one its replicas.
  void CreateTable(const tserver::ListTabletsResponsePB::StatusAndSchemaPB& replica);
  void CreateTablet(const tserver::ListTabletsResponsePB::StatusAndSchemaPB& replica);

  // Check that a replica's metadata is consistent with previously-gathered
  // metadata for its table or tablet, returning an error if the input
  // 'replica' is inconsistent with existing metadata collected for the same
  // tablet or table.
  // Before checking inconsistent, table's next_column_id will be updated if
  // found a larger one in 'replica'.
  Status CheckTableConsistency(const tserver::ListTabletsResponsePB::StatusAndSchemaPB& replica);
  Status CheckTabletConsistency(const tserver::ListTabletsResponsePB::StatusAndSchemaPB& replica);

  // Write the syscatalog table based on the collated tablet server metadata.
  Status WriteSysCatalog();

  // Update or write the syscatalog table based on the collated tablet server metadata.
  Status UpsertSysCatalog();

  State state_;

  // Addresses of the tablet servers used for the reconstruction.
  const std::vector<std::string> tserver_addrs_;

  // Data structures used to organize metadata from tablet servers until it can
  // be written to the syscatalog.
  std::map<std::string, scoped_refptr<master::TableInfo>> tables_by_name_;
  std::map<std::string, scoped_refptr<master::TabletInfo>> tablets_by_id_;

  // Accumulator for the results of various rebuilding processes.
  RebuildReport rebuild_report_;

  // Generator for table ids, since table ids are lost if the masters are lost.
  ObjectIdGenerator oid_generator_;

  DISALLOW_COPY_AND_ASSIGN(MasterRebuilder);
};
} // namespace tools
} // namespace kudu
