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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tablet/cfile_set.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_bool(dump_data, false,
            "Dump the data for each column in the rowset.");
DEFINE_bool(metadata_only, false,
            "Only dump the block metadata when printing blocks.");
DEFINE_int64(nrows, 0, "Number of rows to dump");
DEFINE_bool(list_detail, false,
            "Print partition info for the local replicas");
DEFINE_int64(rowset_index, -1,
             "Index of the rowset in local replica, default value(-1) "
             "will dump all the rowsets of the local replica");
DEFINE_bool(clean_unsafe, false,
            "Delete the local replica completely, not leaving a tombstone. "
            "This is not guaranteed to be safe because it also removes the "
            "consensus metadata (including Raft voting record) for the "
            "specified tablet, which violates the Raft vote durability requirements.");

namespace kudu {
namespace tools {

using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::DumpIterator;
using cfile::ReaderOptions;
using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::OpId;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using fs::ReadableBlock;
using log::LogEntryPB;
using log::LogEntryReader;
using log::LogIndex;
using log::LogReader;
using log::ReadableLogSegment;
using log::SegmentSequence;
using rpc::Messenger;
using rpc::MessengerBuilder;
using std::cout;
using std::endl;
using std::list;
using std::map;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;
using tablet::CFileSet;
using tablet::DeltaFileReader;
using tablet::DeltaIterator;
using tablet::DeltaKeyAndUpdate;
using tablet::DeltaType;
using tablet::MvccSnapshot;
using tablet::RowSetMetadata;
using tablet::TabletMetadata;
using tablet::TabletDataState;
using tserver::TabletCopyClient;
using tserver::TSTabletManager;

namespace {

const char* const kSeparatorLine =
    "----------------------------------------------------------------------\n";

const char* const kTermArg = "term";

const char* const kTabletIdGlobArg = "tablet_id_pattern";
const char* const kTabletIdGlobArgDesc = "Tablet identifier pattern. "
    "This argument supports basic glob syntax: '*' matches 0 or more wildcard "
    "characters.";

string Indent(int indent) {
  return string(indent, ' ');
}

Status FsInit(unique_ptr<FsManager>* fs_manager) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  unique_ptr<FsManager> fs_ptr(new FsManager(Env::Default(), fs_opts));
  RETURN_NOT_OK(fs_ptr->Open());
  fs_manager->swap(fs_ptr);
  return Status::OK();
}

// Parses a colon-delimited string containing a hostname or IP address and port
// into its respective parts. For example, "localhost:12345" parses into
// hostname=localhost, and port=12345.
//
// Does not allow a port with value 0.
Status ParseHostPortString(const string& hostport_str, HostPort* hostport) {
  HostPort hp;
  Status s = hp.ParseString(hostport_str, 0);
  if (!s.ok()) {
    return s.CloneAndPrepend(Substitute(
        "error while parsing peer '$0'", hostport_str));
  }
  if (hp.port() == 0) {
    return Status::InvalidArgument(
        Substitute("peer '$0' has port of 0", hostport_str));
  }
  *hostport = hp;
  return Status::OK();
}

// Find the last replicated OpId for the tablet_id from the WAL.
Status FindLastLoggedOpId(FsManager* fs, const string& tablet_id,
                          OpId* last_logged_opid) {
  shared_ptr<LogReader> reader;
  RETURN_NOT_OK(LogReader::Open(fs, scoped_refptr<log::LogIndex>(), tablet_id,
                                scoped_refptr<MetricEntity>(), &reader));
  SegmentSequence segs;
  RETURN_NOT_OK(reader->GetSegmentsSnapshot(&segs));
  // Reverse iterate the segments to find the 'last replicated' entry quickly.
  // Note that we still read the entries within a segment in sequential
  // fashion, so the last entry within the first 'found' segment will
  // give us the last_logged_opid.
  vector<scoped_refptr<ReadableLogSegment>>::reverse_iterator seg;
  bool found = false;
  for (seg = segs.rbegin(); seg != segs.rend(); ++seg) {
    LogEntryReader reader(seg->get());
    while (true) {
      LogEntryPB entry;
      Status s = reader.ReadNextEntry(&entry);
      if (s.IsEndOfFile()) break;
      RETURN_NOT_OK_PREPEND(s, "Error in log segment");
      if (entry.type() != log::REPLICATE) continue;
      *last_logged_opid = entry.replicate().id();
      found = true;
    }
    if (found) return Status::OK();
  }
  return Status::NotFound("No entries found in the write-ahead log");
}

// Parses a colon-delimited string containing a uuid, hostname or IP address,
// and port into its respective parts. For example,
// "1c7f19e7ecad4f918c0d3d23180fdb18:localhost:12345" parses into
// uuid=1c7f19e7ecad4f918c0d3d23180fdb18, hostname=localhost, and port=12345.
Status ParsePeerString(const string& peer_str,
                       string* uuid,
                       HostPort* hostport) {
  string::size_type first_colon_idx = peer_str.find(":");
  if (first_colon_idx == string::npos) {
    return Status::InvalidArgument(Substitute("bad peer '$0'", peer_str));
  }
  string hostport_str = peer_str.substr(first_colon_idx + 1);
  RETURN_NOT_OK(ParseHostPortString(hostport_str, hostport));
  *uuid = peer_str.substr(0, first_colon_idx);
  return Status::OK();
}

Status PrintReplicaUuids(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(
      new ConsensusMetadataManager(fs_manager.get()));

  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  // Load the cmeta file and print all peer uuids.
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager->Load(tablet_id, &cmeta));
  cout << JoinMapped(cmeta->CommittedConfig().peers(),
                     [](const RaftPeerPB& p){ return p.permanent_uuid(); },
                     " ") << endl;
  return Status::OK();
}

Status BackupConsensusMetadata(FsManager* fs_manager,
                               const string& tablet_id) {
  Env* env = fs_manager->env();
  string cmeta_filename = fs_manager->GetConsensusMetadataPath(tablet_id);
  string backup_filename = Substitute("$0.pre_rewrite.$1", cmeta_filename, env->NowMicros());
  WritableFileOptions opts;
  opts.mode = Env::CREATE_NON_EXISTING;
  opts.sync_on_close = true;
  RETURN_NOT_OK(env_util::CopyFile(env, cmeta_filename, backup_filename, opts));
  LOG(INFO) << "Backed up old consensus metadata to " << backup_filename;
  return Status::OK();
}

Status RewriteRaftConfig(const RunnerContext& context) {
  // Parse tablet ID argument.
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  if (tablet_id != master::SysCatalogTable::kSysCatalogTabletId) {
    LOG(WARNING) << "Master will not notice rewritten Raft config of regular "
                 << "tablets. A regular Raft config change must occur.";
  }

  // Parse peer arguments.
  vector<pair<string, HostPort>> peers;
  for (const auto& arg : context.variadic_args) {
    pair<string, HostPort> parsed_peer;
    RETURN_NOT_OK(ParsePeerString(arg,
                                  &parsed_peer.first, &parsed_peer.second));
    peers.push_back(parsed_peer);
  }
  DCHECK(!peers.empty());

  // Make a copy of the old file before rewriting it.
  Env* env = Env::Default();
  FsManager fs_manager(env, FsManagerOpts());
  RETURN_NOT_OK(fs_manager.Open());
  RETURN_NOT_OK(BackupConsensusMetadata(&fs_manager, tablet_id));

  // Load the cmeta file and rewrite the raft config.
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(new ConsensusMetadataManager(&fs_manager));
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager->Load(tablet_id, &cmeta));
  RaftConfigPB current_config = cmeta->CommittedConfig();
  RaftConfigPB new_config = current_config;
  new_config.clear_peers();
  for (const auto& p : peers) {
    RaftPeerPB new_peer;
    new_peer.set_member_type(RaftPeerPB::VOTER);
    new_peer.set_permanent_uuid(p.first);
    HostPortPB new_peer_host_port_pb;
    RETURN_NOT_OK(HostPortToPB(p.second, &new_peer_host_port_pb));
    new_peer.mutable_last_known_addr()->CopyFrom(new_peer_host_port_pb);
    new_config.add_peers()->CopyFrom(new_peer);
  }
  cmeta->set_committed_config(new_config);
  return cmeta->Flush();
}

Status SetRaftTerm(const RunnerContext& context) {
  // Parse tablet ID argument.
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& new_term_str = FindOrDie(context.required_args, kTermArg);
  int64_t new_term;
  if (!safe_strto64(new_term_str, &new_term) || new_term <= 0) {
    return Status::InvalidArgument("invalid term");
  }

  // Load the current metadata from disk and verify that the intended operation is safe.
  Env* env = Env::Default();
  FsManager fs_manager(env, FsManagerOpts());
  RETURN_NOT_OK(fs_manager.Open());
  // Load the cmeta file and rewrite the raft config.
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(new ConsensusMetadataManager(&fs_manager));
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager->Load(tablet_id, &cmeta));
  if (new_term <= cmeta->current_term()) {
    return Status::InvalidArgument(Substitute(
        "specified term $0 must be higher than current term $1",
        new_term, cmeta->current_term()));
  }

  // Make a copy of the old file before rewriting it.
  RETURN_NOT_OK(BackupConsensusMetadata(&fs_manager, tablet_id));

  // Update and flush.
  cmeta->set_current_term(new_term);
  // The 'voted_for' field is relative to the term stored in 'current_term'. So, if we
  // have changed to a new term, we need to also clear any previous vote record that was
  // associated with the old term.
  cmeta->clear_voted_for();
  return cmeta->Flush();
}

Status CopyFromRemote(const RunnerContext& context) {
  // Parse the tablet ID and source arguments.
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  const string& rpc_address = FindOrDie(context.required_args, "source");

  HostPort hp;
  RETURN_NOT_OK(ParseHostPortString(rpc_address, &hp));

  // Copy the tablet over.
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  RETURN_NOT_OK(fs_manager.Open());
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(new ConsensusMetadataManager(&fs_manager));
  MessengerBuilder builder("tablet_copy_client");
  shared_ptr<Messenger> messenger;
  builder.Build(&messenger);
  TabletCopyClient client(tablet_id, &fs_manager, cmeta_manager,
                          messenger, nullptr /* no metrics */);
  RETURN_NOT_OK(client.Start(hp, nullptr));
  RETURN_NOT_OK(client.FetchAll(nullptr));
  return client.Finish();
}

Status DeleteLocalReplica(const RunnerContext& context) {
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  RETURN_NOT_OK(fs_manager.Open());
  scoped_refptr<ConsensusMetadataManager> cmeta_manager(new ConsensusMetadataManager(&fs_manager));
  boost::optional<OpId> last_logged_opid = boost::none;
  TabletDataState state = TabletDataState::TABLET_DATA_DELETED;
  if (!FLAGS_clean_unsafe) {
    state = TabletDataState::TABLET_DATA_TOMBSTONED;
    // Tombstone the tablet. If we couldn't find the last committed OpId from
    // the log, it's not an error. But if we receive any other error,
    // indicate the user to delete with --clean_unsafe flag.
    OpId opid;
    Status s = FindLastLoggedOpId(&fs_manager, tablet_id, &opid);
    if (s.ok()) {
      last_logged_opid = opid;
    } else if (s.IsNotFound()) {
      LOG(INFO) << "Could not find any replicated OpId from WAL, "
                << "but proceeding with tablet tombstone: " << s.ToString();
    } else {
      LOG(ERROR) << "Error attempting to find last replicated OpId from WAL: " << s.ToString();
      LOG(ERROR) << "Cannot delete (tombstone) the tablet, use --clean_unsafe to delete"
                 << " the tablet permanently from this server.";
      return s;
    }
  }

  // Force the specified tablet on this node to be in 'state'.
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, tablet_id, &meta));
  RETURN_NOT_OK(TSTabletManager::DeleteTabletData(meta, cmeta_manager, state, last_logged_opid));
  return Status::OK();
}

Status SummarizeSize(FsManager* fs,
                     const vector<BlockId>& blocks,
                     StringPiece block_type,
                     int64_t* running_sum) {
  int64_t local_sum = 0;
  for (const auto& b : blocks) {
    unique_ptr<fs::ReadableBlock> rb;
    RETURN_NOT_OK_PREPEND(fs->OpenBlock(b, &rb),
                          Substitute("could not open block $0", b.ToString()));
    uint64_t size = 0;
    RETURN_NOT_OK_PREPEND(rb->Size(&size),
                          Substitute("could not get size for block $0", b.ToString()));
    local_sum += size;
    if (VLOG_IS_ON(1)) {
      cout << Substitute("$0 block $1: $2 bytes $3",
                         block_type, b.ToString(),
                         size, HumanReadableNumBytes::ToString(size)) << endl;
    }
  }
  *running_sum += local_sum;
  return Status::OK();
}

namespace {
struct TabletSizeStats {
  int64_t redo_bytes = 0;
  int64_t undo_bytes = 0;
  int64_t bloom_bytes = 0;
  int64_t pk_index_bytes = 0;
  map<string, int64_t, autodigit_less> column_bytes;

  void Add(const TabletSizeStats& other) {
    redo_bytes += other.redo_bytes;
    undo_bytes += other.undo_bytes;
    bloom_bytes += other.bloom_bytes;
    pk_index_bytes += other.pk_index_bytes;
    for (const auto& p : other.column_bytes) {
      column_bytes[p.first] += p.second;
    }
  }

  void AddToTable(const string& table_id,
                  const string& tablet_id,
                  const string& rowset_id,
                  DataTable* table) const {
    vector<pair<string, int64_t>> to_print(column_bytes.begin(), column_bytes.end());
    to_print.emplace_back("REDO", redo_bytes);
    to_print.emplace_back("UNDO", undo_bytes);
    to_print.emplace_back("BLOOM", bloom_bytes);
    to_print.emplace_back("PK", pk_index_bytes);

    int64_t total = 0;
    for (const auto& e : to_print) {
      table->AddRow({table_id, tablet_id, rowset_id, e.first,
              HumanReadableNumBytes::ToString(e.second)});
      total += e.second;
    }
    table->AddRow({table_id, tablet_id, rowset_id, "*", HumanReadableNumBytes::ToString(total)});
  }
};
} // anonymous namespace

Status SummarizeDataSize(const RunnerContext& context) {
  const string& tablet_id_pattern = FindOrDie(context.required_args, kTabletIdGlobArg);
  unique_ptr<FsManager> fs;
  RETURN_NOT_OK(FsInit(&fs));

  vector<string> tablets;
  RETURN_NOT_OK(fs->ListTabletIds(&tablets));

  std::unordered_map<string, TabletSizeStats> size_stats_by_table_id;

  DataTable output_table({ "table id", "tablet id", "rowset id", "block type", "size" });

  for (const string& tablet_id : tablets) {
    TabletSizeStats tablet_stats;
    if (!MatchPattern(tablet_id, tablet_id_pattern)) continue;
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs.get(), tablet_id, &meta),
                          Substitute("could not load tablet metadata for $0", tablet_id));
    const string& table_id = meta->table_id();
    for (const shared_ptr<RowSetMetadata>& rs_meta : meta->rowsets()) {
      TabletSizeStats rowset_stats;
      RETURN_NOT_OK(SummarizeSize(fs.get(), rs_meta->redo_delta_blocks(),
                                  "REDO", &rowset_stats.redo_bytes));
      RETURN_NOT_OK(SummarizeSize(fs.get(), rs_meta->undo_delta_blocks(),
                                  "UNDO", &rowset_stats.undo_bytes));
      RETURN_NOT_OK(SummarizeSize(fs.get(), { rs_meta->bloom_block() },
                                  "Bloom", &rowset_stats.bloom_bytes));
      if (rs_meta->has_adhoc_index_block()) {
        RETURN_NOT_OK(SummarizeSize(fs.get(), { rs_meta->adhoc_index_block() },
                                    "PK index", &rowset_stats.pk_index_bytes));
      }
      const auto& column_blocks_by_id = rs_meta->GetColumnBlocksById();
      for (const auto& e : column_blocks_by_id) {
        const auto& col_id = e.first;
        const auto& block = e.second;
        const auto& col_idx = meta->schema().find_column_by_id(col_id);
        string col_key = Substitute(
            "c$0 ($1)", col_id,
            (col_idx != Schema::kColumnNotFound) ?
                meta->schema().column(col_idx).name() : "?");
        RETURN_NOT_OK(SummarizeSize(
            fs.get(), { block }, col_key, &rowset_stats.column_bytes[col_key]));
      }
      rowset_stats.AddToTable(table_id, tablet_id, std::to_string(rs_meta->id()), &output_table);
      tablet_stats.Add(rowset_stats);
    }
    tablet_stats.AddToTable(table_id, tablet_id, "*", &output_table);
    size_stats_by_table_id[table_id].Add(tablet_stats);
  }
  for (const auto& e : size_stats_by_table_id) {
    const auto& table_id = e.first;
    const auto& stats = e.second;
    stats.AddToTable(table_id, "*", "*", &output_table);
  }
  RETURN_NOT_OK(output_table.PrintTo(cout));
  return Status::OK();
}

Status DumpWals(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  shared_ptr<LogReader> reader;
  RETURN_NOT_OK(LogReader::Open(fs_manager.get(),
                                scoped_refptr<LogIndex>(),
                                tablet_id,
                                scoped_refptr<MetricEntity>(),
                                &reader));

  SegmentSequence segments;
  RETURN_NOT_OK(reader->GetSegmentsSnapshot(&segments));

  for (const scoped_refptr<ReadableLogSegment>& segment : segments) {
    RETURN_NOT_OK(PrintSegment(segment));
  }

  return Status::OK();
}

Status ListBlocksInRowSet(const Schema& schema,
                          const RowSetMetadata& rs_meta) {
  RowSetMetadata::ColumnIdToBlockIdMap col_blocks =
      rs_meta.GetColumnBlocksById();
  for (const RowSetMetadata::ColumnIdToBlockIdMap::value_type& e :
      col_blocks) {
    ColumnId col_id = e.first;
    const BlockId& block_id = e.second;
    cout << "Column block for column ID " << col_id;
    int col_idx = schema.find_column_by_id(col_id);
    if (col_idx != -1) {
      cout << " (" << schema.column(col_idx).ToString() << ")";
    }
    cout << ": ";
    cout << block_id.ToString() << endl;
  }

  for (const BlockId& block : rs_meta.undo_delta_blocks()) {
    cout << "UNDO: " << block.ToString() << endl;
  }

  for (const BlockId& block : rs_meta.redo_delta_blocks()) {
    cout << "REDO: " << block.ToString() << endl;
  }

  return Status::OK();
}

Status DumpBlockIdsForLocalReplica(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(TabletMetadata::Load(fs_manager.get(), tablet_id, &meta));

  if (meta->rowsets().empty()) {
    cout << "No rowsets found on disk for tablet "
         << tablet_id << endl;
    return Status::OK();
  }

  cout << "Listing all data blocks in tablet "
       << tablet_id << ":" << endl;

  Schema schema = meta->schema();

  size_t idx = 0;
  for (const shared_ptr<RowSetMetadata>& rs_meta : meta->rowsets())  {
    cout << "Rowset " << idx++ << endl;
    RETURN_NOT_OK(ListBlocksInRowSet(schema, *rs_meta));
  }

  return Status::OK();
}

Status DumpTabletMeta(FsManager* fs_manager,
                       const string& tablet_id, int indent) {
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(TabletMetadata::Load(fs_manager, tablet_id, &meta));

  const Schema& schema = meta->schema();

  cout << Indent(indent) << "Partition: "
       << meta->partition_schema().PartitionDebugString(meta->partition(),
                                                        meta->schema())
       << endl;
  cout << Indent(indent) << "Table name: " << meta->table_name()
       << " Table id: " << meta->table_id() << endl;
  cout << Indent(indent) << "Schema (version="
       << meta->schema_version() << "): "
       << schema.ToString() << endl;

  tablet::TabletSuperBlockPB pb;
  RETURN_NOT_OK_PREPEND(meta->ToSuperBlock(&pb), "Could not get superblock");
  cout << "Superblock:\n" << pb_util::SecureDebugString(pb) << endl;

  return Status::OK();
}

Status ListLocalReplicas(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));

  vector<string> tablets;
  RETURN_NOT_OK(fs_manager->ListTabletIds(&tablets));
  for (const string& tablet : tablets) {
    if (FLAGS_list_detail) {
      cout << "Tablet: " << tablet << endl;
      RETURN_NOT_OK(DumpTabletMeta(fs_manager.get(), tablet, 2));
    } else {
      cout << tablet << endl;
    }
  }
  return Status::OK();
}

Status DumpCFileBlockInternal(FsManager* fs_manager,
                              const BlockId& block_id,
                              int indent) {
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs_manager->OpenBlock(block_id, &block));
  unique_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  cout << Indent(indent) << "CFile Header: "
       << pb_util::SecureShortDebugString(reader->header()) << endl;
  if (!FLAGS_dump_data) {
    return Status::OK();
  }
  cout << Indent(indent) << reader->footer().num_values()
       << " values:" << endl;

  gscoped_ptr<CFileIterator> it;
  RETURN_NOT_OK(reader->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK));
  RETURN_NOT_OK(it->SeekToFirst());
  return DumpIterator(*reader, it.get(), &cout, FLAGS_nrows, indent + 2);
}

Status DumpDeltaCFileBlockInternal(FsManager* fs_manager,
                                   const Schema& schema,
                                   const shared_ptr<RowSetMetadata>& rs_meta,
                                   const BlockId& block_id,
                                   DeltaType delta_type,
                                   int indent) {
  // Open the delta reader
  unique_ptr<ReadableBlock> readable_block;
  RETURN_NOT_OK(fs_manager->OpenBlock(block_id, &readable_block));
  shared_ptr<DeltaFileReader> delta_reader;
  RETURN_NOT_OK(DeltaFileReader::Open(std::move(readable_block),
                                      delta_type,
                                      ReaderOptions(),
                                      &delta_reader));

  cout << Indent(indent) << "Delta stats: "
       << delta_reader->delta_stats().ToString() << endl;
  if (FLAGS_metadata_only) {
    return Status::OK();
  }

  // Create the delta iterator.
  // TODO: see if it's worth re-factoring NewDeltaIterator to return a
  // gscoped_ptr that can then be released if we need a raw or shared
  // pointer.
  DeltaIterator* raw_iter;

  MvccSnapshot snap_all;
  if (delta_type == tablet::REDO) {
    snap_all = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
  } else if (delta_type == tablet::UNDO) {
    snap_all = MvccSnapshot::CreateSnapshotIncludingNoTransactions();
  }

  Status s = delta_reader->NewDeltaIterator(&schema, snap_all, &raw_iter);

  if (s.IsNotFound()) {
    cout << "Empty delta block." << endl;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  // NewDeltaIterator returns Status::OK() iff a new DeltaIterator is created. Thus,
  // it's safe to have a unique_ptr take possesion of 'raw_iter' here.
  unique_ptr<DeltaIterator> delta_iter(raw_iter);
  RETURN_NOT_OK(delta_iter->Init(NULL));
  RETURN_NOT_OK(delta_iter->SeekToOrdinal(0));

  // TODO: it's awkward that whenever we want to iterate over deltas we also
  // need to open the CFileSet for the rowset. Ideally, we should use
  // information stored in the footer/store additional information in the
  // footer as to make it feasible iterate over all deltas using a
  // DeltaFileIterator alone.
  shared_ptr<CFileSet> cfileset;
  RETURN_NOT_OK(CFileSet::Open(rs_meta, MemTracker::GetRootTracker(), &cfileset));
  gscoped_ptr<CFileSet::Iterator> cfileset_iter(cfileset->NewIterator(&schema));

  RETURN_NOT_OK(cfileset_iter->Init(NULL));

  const size_t kRowsPerBlock  = 100;
  size_t nrows = 0;
  size_t ndeltas = 0;
  Arena arena(32 * 1024);
  RowBlock block(schema, kRowsPerBlock, &arena);

  // See tablet/delta_compaction.cc to understand why this loop is structured the way
  // it is.
  while (cfileset_iter->HasNext()) {
    size_t n;
    if (FLAGS_nrows > 0) {
      // Note: number of deltas may not equal the number of rows, but
      // since this is a CLI tool (and the nrows option exists
      // primarily to limit copious output) it's okay not to be
      // exact here.
      size_t remaining = FLAGS_nrows - nrows;
      if (remaining == 0) break;
      n = std::min(remaining, kRowsPerBlock);
    } else {
      n = kRowsPerBlock;
    }

    arena.Reset();
    cfileset_iter->PrepareBatch(&n);

    block.Resize(n);

    RETURN_NOT_OK(delta_iter->PrepareBatch(
        n, DeltaIterator::PREPARE_FOR_COLLECT));
    vector<DeltaKeyAndUpdate> out;
    RETURN_NOT_OK(
        delta_iter->FilterColumnIdsAndCollectDeltas(vector<ColumnId>(),
                                                              &out,
                                                              &arena));
    for (const DeltaKeyAndUpdate& upd : out) {
      if (FLAGS_dump_data) {
        cout << Indent(indent) << upd.key.ToString() << " "
             << RowChangeList(upd.cell).ToString(schema) << endl;
        ++ndeltas;
      }
    }
    RETURN_NOT_OK(cfileset_iter->FinishBatch());

    nrows += n;
  }

  VLOG(1) << "Processed " << ndeltas << " deltas, for total of "
          << nrows << " possible rows.";
  return Status::OK();
}

Status DumpRowSetInternal(FsManager* fs_manager,
                          const Schema& schema,
                          const shared_ptr<RowSetMetadata>& rs_meta,
                          int indent) {
  tablet::RowSetDataPB pb;
  rs_meta->ToProtobuf(&pb);

  cout << Indent(indent) << "RowSet metadata: " << pb_util::SecureDebugString(pb)
       << endl << endl;

  RowSetMetadata::ColumnIdToBlockIdMap col_blocks =
      rs_meta->GetColumnBlocksById();
  for (const RowSetMetadata::ColumnIdToBlockIdMap::value_type& e :
      col_blocks) {
    ColumnId col_id = e.first;
    const BlockId& block_id = e.second;

    cout << Indent(indent) << "Dumping column block " << block_id
         << " for column id " << col_id;
    int col_idx = schema.find_column_by_id(col_id);
    if (col_idx != -1) {
      cout << "( " << schema.column(col_idx).ToString() <<  ")";
    }
    cout << ":" << endl;
    cout << Indent(indent) << kSeparatorLine;
    if (FLAGS_metadata_only) continue;
    RETURN_NOT_OK(DumpCFileBlockInternal(fs_manager, block_id, indent));
    cout << endl;
  }

  for (const BlockId& block : rs_meta->undo_delta_blocks()) {
    cout << Indent(indent) << "Dumping undo delta block " << block << ":"
         << endl << Indent(indent) << kSeparatorLine;
    RETURN_NOT_OK(DumpDeltaCFileBlockInternal(fs_manager,
                                              schema,
                                              rs_meta,
                                              block,
                                              tablet::UNDO,
                                              indent));
    cout << endl;
  }

  for (const BlockId& block : rs_meta->redo_delta_blocks()) {
    cout << Indent(indent) << "Dumping redo delta block " << block << ":"
         << endl << Indent(indent) << kSeparatorLine;
    RETURN_NOT_OK(DumpDeltaCFileBlockInternal(fs_manager,
                                              schema,
                                              rs_meta,
                                              block,
                                              tablet::REDO,
                                              indent));
    cout << endl;
  }

  return Status::OK();
}

Status DumpRowSet(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(TabletMetadata::Load(fs_manager.get(), tablet_id, &meta));
  if (meta->rowsets().empty()) {
    cout << Indent(0) << "No rowsets found on disk for tablet "
         << tablet_id << endl;
    return Status::OK();
  }

  // If rowset index is provided, only dump that rowset.
  if (FLAGS_rowset_index != -1) {
    for (const shared_ptr<RowSetMetadata>& rs_meta : meta->rowsets())  {
      if (rs_meta->id() == FLAGS_rowset_index) {
        return DumpRowSetInternal(fs_manager.get(), meta->schema(),
                                  rs_meta, 0);
      }
    }
    return Status::InvalidArgument(
        Substitute("Could not find rowset $0 in tablet id $1",
                   FLAGS_rowset_index, tablet_id));
  }

  // Rowset index not provided, dump all rowsets
  size_t idx = 0;
  for (const shared_ptr<RowSetMetadata>& rs_meta : meta->rowsets())  {
    cout << endl << "Dumping rowset " << idx++ << endl << kSeparatorLine;
    RETURN_NOT_OK(DumpRowSetInternal(fs_manager.get(), meta->schema(),
                                     rs_meta, 2));
  }
  return Status::OK();
}

Status DumpMeta(const RunnerContext& context) {
  unique_ptr<FsManager> fs_manager;
  RETURN_NOT_OK(FsInit(&fs_manager));
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);
  RETURN_NOT_OK(DumpTabletMeta(fs_manager.get(), tablet_id, 0));
  return Status::OK();
}

unique_ptr<Mode> BuildDumpMode() {
  unique_ptr<Action> dump_block_ids =
      ActionBuilder("block_ids", &DumpBlockIdsForLocalReplica)
      .Description("Dump the IDs of all blocks belonging to a local replica")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_meta =
      ActionBuilder("meta", &DumpMeta)
      .Description("Dump the metadata of a local replica")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_rowset =
      ActionBuilder("rowset", &DumpRowSet)
      .Description("Dump the rowset contents of a local replica")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("dump_data")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("metadata_only")
      .AddOptionalParameter("nrows")
      .AddOptionalParameter("rowset_index")
      .Build();

  unique_ptr<Action> dump_wals =
      ActionBuilder("wals", &DumpWals)
      .Description("Dump all WAL (write-ahead log) segments of "
        "a local replica")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("print_entries")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("truncate_data")
      .Build();

  return ModeBuilder("dump")
      .Description("Dump a Kudu filesystem")
      .AddAction(std::move(dump_block_ids))
      .AddAction(std::move(dump_meta))
      .AddAction(std::move(dump_rowset))
      .AddAction(std::move(dump_wals))
      .Build();
}

} // anonymous namespace

unique_ptr<Mode> BuildLocalReplicaMode() {
  unique_ptr<Action> print_replica_uuids =
      ActionBuilder("print_replica_uuids", &PrintReplicaUuids)
      .Description("Print all tablet replica peer UUIDs found in a "
        "tablet's Raft configuration")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> rewrite_raft_config =
      ActionBuilder("rewrite_raft_config", &RewriteRaftConfig)
      .Description("Rewrite a tablet replica's Raft configuration")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredVariadicParameter({
        "peers", "List of peers where each peer is of "
        "form 'uuid:hostname:port'" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> set_term =
      ActionBuilder("set_term", &SetRaftTerm)
      .Description("Bump the current term stored in consensus metadata")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ kTermArg, "the new raft term (must be greater "
        "than the current term)" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Mode> cmeta =
      ModeBuilder("cmeta")
      .Description("Operate on a local tablet replica's consensus "
        "metadata file")
      .AddAction(std::move(print_replica_uuids))
      .AddAction(std::move(rewrite_raft_config))
      .AddAction(std::move(set_term))
      .Build();

  unique_ptr<Action> copy_from_remote =
      ActionBuilder("copy_from_remote", &CopyFromRemote)
      .Description("Copy a tablet replica from a remote server")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddRequiredParameter({ "source", "Source RPC address of "
        "form hostname:port" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> list =
      ActionBuilder("list", &ListLocalReplicas)
      .Description("Show list of tablet replicas in the local filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("list_detail")
      .Build();

  unique_ptr<Action> delete_local_replica =
      ActionBuilder("delete", &DeleteLocalReplica)
      .Description("Delete a tablet replica from the local filesystem. "
          "By default, leaves a tombstone record.")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("clean_unsafe")
      .Build();

  unique_ptr<Action> data_size =
      ActionBuilder("data_size", &SummarizeDataSize)
      .Description("Summarize the data size/space usage of the given local replica(s).")
      .AddRequiredParameter({ kTabletIdGlobArg, kTabletIdGlobArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("format")
      .Build();

  return ModeBuilder("local_replica")
      .Description("Operate on local tablet replicas via the local filesystem")
      .AddMode(std::move(cmeta))
      .AddAction(std::move(copy_from_remote))
      .AddAction(std::move(data_size))
      .AddAction(std::move(delete_local_replica))
      .AddAction(std::move(list))
      .AddMode(BuildDumpMode())
      .Build();
}

} // namespace tools
} // namespace kudu
