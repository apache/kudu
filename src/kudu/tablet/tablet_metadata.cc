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

#include "kudu/tablet/tablet_metadata.h"

#include <algorithm>
#include <functional>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_bool(enable_tablet_orphaned_block_deletion, true,
            "Whether to enable deletion of orphaned blocks from disk. "
            "Note: This is only exposed for debugging purposes and used "
            "in CLI tools.");
TAG_FLAG(enable_tablet_orphaned_block_deletion, advanced);
TAG_FLAG(enable_tablet_orphaned_block_deletion, hidden);
TAG_FLAG(enable_tablet_orphaned_block_deletion, runtime);

DEFINE_int32(tablet_metadata_load_inject_latency_ms, 0,
             "Amount of latency in ms to inject when load tablet metadata file. "
             "Only for testing.");
TAG_FLAG(tablet_metadata_load_inject_latency_ms, hidden);

using base::subtle::Barrier_AtomicIncrement;
using kudu::consensus::MinimumOpId;
using kudu::consensus::OpId;
using kudu::fs::BlockDeletionTransaction;
using kudu::fs::BlockManager;
using kudu::log::MinLogIndexAnchorer;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::memory_order_relaxed;
using std::make_optional;
using std::nullopt;
using std::optional;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const string& table_name,
                                 const string& table_id,
                                 const Schema& schema,
                                 const PartitionSchema& partition_schema,
                                 const Partition& partition,
                                 const TabletDataState& initial_tablet_data_state,
                                 optional<OpId> tombstone_last_logged_opid,
                                 bool supports_live_row_count,
                                 optional<TableExtraConfigPB> extra_config,
                                 optional<string> dimension_label,
                                 optional<TableTypePB> table_type,
                                 scoped_refptr<TabletMetadata>* metadata) {

  // Verify that no existing tablet exists with the same ID.
  if (fs_manager->env()->FileExists(fs_manager->GetTabletMetadataPath(tablet_id))) {
    return Status::AlreadyPresent("Tablet already exists", tablet_id);
  }

  RETURN_NOT_OK_PREPEND(fs_manager->dd_manager()->CreateDataDirGroup(tablet_id),
      "Failed to create TabletMetadata");
  auto dir_group_cleanup = MakeScopedCleanup([&]() {
    fs_manager->dd_manager()->DeleteDataDirGroup(tablet_id);
  });
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(
      fs_manager,
      tablet_id,
      table_name,
      table_id,
      schema,
      partition_schema,
      partition,
      initial_tablet_data_state,
      std::move(tombstone_last_logged_opid),
      supports_live_row_count,
      std::move(extra_config),
      std::move(dimension_label),
      !table_type ||
          *table_type == TableTypePB::DEFAULT_TABLE ? nullopt
                                                    : std::move(table_type)));
  RETURN_NOT_OK(ret->Flush());
  dir_group_cleanup.cancel();

  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            const string& tablet_id,
                            scoped_refptr<TabletMetadata>* metadata) {
  MAYBE_INJECT_FIXED_LATENCY(FLAGS_tablet_metadata_load_inject_latency_ms);
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager, tablet_id));
  RETURN_NOT_OK(ret->LoadFromDisk());
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    const string& tablet_id,
                                    const string& table_name,
                                    const string& table_id,
                                    const Schema& schema,
                                    const PartitionSchema& partition_schema,
                                    const Partition& partition,
                                    const TabletDataState& initial_tablet_data_state,
                                    optional<OpId> tombstone_last_logged_opid,
                                    optional<TableExtraConfigPB> extra_config,
                                    optional<string> dimension_label,
                                    optional<TableTypePB> table_type,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, tablet_id, metadata);
  if (s.ok()) {
    const SchemaPtr schema_ptr = (*metadata)->schema();
    if (*schema_ptr != schema) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", schema_ptr->ToString(),
        schema.ToString()));
    }
    return Status::OK();
  }
  if (s.IsNotFound()) {
    return CreateNew(fs_manager, tablet_id, table_name, table_id, schema,
                     partition_schema, partition, initial_tablet_data_state,
                     std::move(tombstone_last_logged_opid),
                     /*supports_live_row_count=*/ true,
                     std::move(extra_config),
                     std::move(dimension_label),
                     std::move(table_type),
                     metadata);
  }
  return s;
}

vector<BlockIdPB> TabletMetadata::CollectBlockIdPBs(const TabletSuperBlockPB& superblock) {
  vector<BlockIdPB> block_ids;
  for (const RowSetDataPB& rowset : superblock.rowsets()) {
    for (const ColumnDataPB& column : rowset.columns()) {
      block_ids.push_back(column.block());
    }
    for (const DeltaDataPB& redo : rowset.redo_deltas()) {
      block_ids.push_back(redo.block());
    }
    for (const DeltaDataPB& undo : rowset.undo_deltas()) {
      block_ids.push_back(undo.block());
    }
    if (rowset.has_bloom_block()) {
      block_ids.push_back(rowset.bloom_block());
    }
    if (rowset.has_adhoc_index_block()) {
      block_ids.push_back(rowset.adhoc_index_block());
    }
  }
  return block_ids;
}

BlockIdContainer TabletMetadata::CollectBlockIds() const {
  BlockIdContainer block_ids;
  for (const auto& r : rowsets_) {
    BlockIdContainer rowset_block_ids = r->GetAllBlocks();
    block_ids.insert(block_ids.end(),
                     rowset_block_ids.begin(),
                     rowset_block_ids.end());
  }
  return block_ids;
}

BlockId TabletMetadata::GetMaxLiveBlockId() const {
  BlockId max_block_id;
  for (const auto& r : rowsets_) {
    max_block_id = std::max(max_block_id, r->GetMaxLiveBlockId());
  }
  return max_block_id;
}

Status TabletMetadata::DeleteTabletData(TabletDataState delete_type,
                                        const optional<OpId>& last_logged_opid) {
  DCHECK(!last_logged_opid || last_logged_opid->IsInitialized());
  CHECK(delete_type == TABLET_DATA_DELETED ||
        delete_type == TABLET_DATA_TOMBSTONED ||
        delete_type == TABLET_DATA_COPYING)
      << "DeleteTabletData() called with unsupported delete_type on tablet "
      << tablet_id_ << ": " << TabletDataState_Name(delete_type)
      << " (" << delete_type << ")";

  // First add all of our blocks to the orphan list
  // and clear our rowsets. This serves to erase all the data.
  //
  // We also set the state in our persisted metadata to indicate that
  // we have been deleted.
  {
    std::lock_guard<LockType> l(data_lock_);
    for (const shared_ptr<RowSetMetadata>& rsmd : rowsets_) {
      AddOrphanedBlocksUnlocked(rsmd->GetAllBlocks());
    }
    rowsets_.clear();
    tablet_data_state_ = delete_type;
    if (last_logged_opid) {
      tombstone_last_logged_opid_ = last_logged_opid;
    }
  }

  // Unregister the tablet's data dir group in memory (it is stored on disk in
  // the superblock). Even if we fail to flush below, the expectation is that
  // we will no longer be writing to the tablet, and therefore don't need its
  // data dir group.
  fs_manager_->dd_manager()->DeleteDataDirGroup(tablet_id_);

  // Flushing will sync the new tablet_data_state_ to disk and will now also
  // delete all the data.
  RETURN_NOT_OK(Flush());

  // Re-sync to disk one more time.
  // This call will typically re-sync with an empty orphaned blocks list
  // (unless deleting any orphans failed during the last Flush()), so that we
  // don't try to re-delete the deleted orphaned blocks on every startup.
  return Flush();
}

bool TabletMetadata::IsTombstonedWithNoBlocks() const {
  std::lock_guard<LockType> l(data_lock_);
  return tablet_data_state_ == TABLET_DATA_TOMBSTONED &&
      rowsets_.empty() &&
      orphaned_blocks_.empty();
}

Status TabletMetadata::DeleteSuperBlock() {
  std::lock_guard<LockType> l(data_lock_);
  if (!orphaned_blocks_.empty()) {
    return Status::InvalidArgument("The metadata for tablet " + tablet_id_ +
                                   " still references orphaned blocks. "
                                   "Call DeleteTabletData() first");
  }
  if (tablet_data_state_ != TABLET_DATA_DELETED) {
    return Status::IllegalState(
        Substitute("Tablet $0 is not in TABLET_DATA_DELETED state. "
                   "Call DeleteTabletData(TABLET_DATA_DELETED) first. "
                   "Tablet data state: $1 ($2)",
                   tablet_id_,
                   TabletDataState_Name(tablet_data_state_),
                   tablet_data_state_));
  }

  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(fs_manager_->env()->DeleteFile(path),
                        "Unable to delete superblock for tablet " + tablet_id_);
  return Status::OK();
}

TabletMetadata::TabletMetadata(FsManager* fs_manager, string tablet_id,
                               string table_name, string table_id,
                               const Schema& schema, PartitionSchema partition_schema,
                               Partition partition,
                               const TabletDataState& tablet_data_state,
                               optional<OpId> tombstone_last_logged_opid,
                               bool supports_live_row_count,
                               optional<TableExtraConfigPB> extra_config,
                               optional<string> dimension_label,
                               optional<TableTypePB> table_type)
    : state_(kNotWrittenYet),
      tablet_id_(std::move(tablet_id)),
      table_id_(std::move(table_id)),
      partition_(std::move(partition)),
      fs_manager_(fs_manager),
      log_prefix_(Substitute("T $0 P $1: ", tablet_id_, fs_manager_->uuid())),
      next_rowset_idx_(0),
      last_durable_mrs_id_(kNoDurableMemStore),
      schema_(std::make_shared<Schema>(schema)),
      schema_version_(0),
      table_name_(std::move(table_name)),
      partition_schema_(std::move(partition_schema)),
      tablet_data_state_(tablet_data_state),
      tombstone_last_logged_opid_(std::move(tombstone_last_logged_opid)),
      extra_config_(std::move(extra_config)),
      dimension_label_(std::move(dimension_label)),
      table_type_(std::move(table_type)),
      num_flush_pins_(0),
      needs_flush_(false),
      flush_count_for_tests_(0),
      pre_flush_callback_(&DoNothingStatusClosure),
      supports_live_row_count_(supports_live_row_count) {
  CHECK(schema_->has_column_ids());
  CHECK_GT(schema_->num_key_columns(), 0);
}

TabletMetadata::~TabletMetadata() {
}

TabletMetadata::TabletMetadata(FsManager* fs_manager, string tablet_id)
    : state_(kNotLoadedYet),
      tablet_id_(std::move(tablet_id)),
      fs_manager_(fs_manager),
      next_rowset_idx_(0),
      num_flush_pins_(0),
      needs_flush_(false),
      flush_count_for_tests_(0),
      pre_flush_callback_(&DoNothingStatusClosure),
      supports_live_row_count_(false) {}

Status TabletMetadata::LoadFromDisk() {
  TRACE_EVENT1("tablet", "TabletMetadata::LoadFromDisk",
               "tablet_id", tablet_id_);

  CHECK_EQ(state_, kNotLoadedYet);

  TabletSuperBlockPB superblock;
  RETURN_NOT_OK(ReadSuperBlockFromDisk(&superblock));
  RETURN_NOT_OK_PREPEND(LoadFromSuperBlock(superblock),
                        "Failed to load data from superblock protobuf");
  RETURN_NOT_OK(UpdateOnDiskSize());
  state_ = kInitialized;
  return Status::OK();
}

Status TabletMetadata::UpdateOnDiskSize() {
  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  uint64_t on_disk_size;
  RETURN_NOT_OK(fs_manager()->env()->GetFileSize(path, &on_disk_size));
  on_disk_size_.store(on_disk_size, memory_order_relaxed);
  return Status::OK();
}

Status TabletMetadata::LoadFromSuperBlock(const TabletSuperBlockPB& superblock) {
  BlockIdContainer orphaned_blocks;

  VLOG(2) << "Loading TabletMetadata from SuperBlockPB:" << std::endl
          << SecureDebugString(superblock);

  {
    std::lock_guard<LockType> l(data_lock_);

    // Verify that the tablet id matches with the one in the protobuf
    if (superblock.tablet_id() != tablet_id_) {
      return Status::Corruption("Expected id=" + tablet_id_ +
                                " found " + superblock.tablet_id(),
                                SecureDebugString(superblock));
    }

    last_durable_mrs_id_ = superblock.last_durable_mrs_id();

    table_name_ = superblock.table_name();

    uint32_t schema_version = superblock.schema_version();
    SchemaPtr schema = std::make_shared<Schema>();
    RETURN_NOT_OK_PREPEND(SchemaFromPB(superblock.schema(), schema.get()),
                          "Failed to parse Schema from superblock " +
                          SecureShortDebugString(superblock));
    {
      SchemaPtr old_schema;
      SwapSchemaUnlocked(schema, schema_version, &old_schema);
    }

    if (!superblock.has_partition()) {
      // KUDU-818: Possible backward compatibility issue with tables created
      // with version <= 0.5, throw warning.
      LOG_WITH_PREFIX(WARNING) << "Upgrading from Kudu 0.5.0 directly to this"
          << " version is not supported. Please upgrade to 0.6.0 before"
          << " moving to a higher version.";
      return Status::NotFound("Missing partition in superblock "+
                              SecureDebugString(superblock));
    }

    // Some metadata fields are assumed to be immutable and thus are
    // only read from the protobuf when the tablet metadata is loaded
    // for the very first time. See KUDU-1500 for more details.
    if (state_ == kNotLoadedYet) {
      table_id_ = superblock.table_id();
      RETURN_NOT_OK(PartitionSchema::FromPB(superblock.partition_schema(),
                                            *schema_, &partition_schema_));
      Partition::FromPB(superblock.partition(), &partition_);
    } else {
      CHECK_EQ(table_id_, superblock.table_id());
      PartitionSchema partition_schema;
      RETURN_NOT_OK(PartitionSchema::FromPB(superblock.partition_schema(),
                                            *schema_, &partition_schema));
      CHECK(partition_schema_ == partition_schema);

      Partition partition;
      Partition::FromPB(superblock.partition(), &partition);
      CHECK(partition_ == partition);
    }

    tablet_data_state_ = superblock.tablet_data_state();

    // This field should be parsed before parsing RowSetDataPB.
    supports_live_row_count_ = superblock.supports_live_row_count();

    rowsets_.clear();
    for (const RowSetDataPB& rowset_pb : superblock.rowsets()) {
      unique_ptr<RowSetMetadata> rowset_meta;
      RETURN_NOT_OK(RowSetMetadata::Load(this, rowset_pb, &rowset_meta));
      next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
      rowsets_.push_back(shared_ptr<RowSetMetadata>(rowset_meta.release()));
    }

    // Determine the largest block ID known to the tablet metadata so we can
    // notify the block manager of blocks it may have missed (e.g. if a data
    // directory failed and the blocks on it were not read).
    BlockId max_block_id = GetMaxLiveBlockId();

    for (const BlockIdPB& block_pb : superblock.orphaned_blocks()) {
      BlockId orphaned_block_id = BlockId::FromPB(block_pb);
      max_block_id = std::max(max_block_id, orphaned_block_id);
      orphaned_blocks.push_back(orphaned_block_id);
    }
    AddOrphanedBlocksUnlocked(orphaned_blocks);

    // Notify the block manager of the highest block ID seen.
    fs_manager()->block_manager()->NotifyBlockId(max_block_id);

    if (superblock.has_data_dir_group()) {
      // An error loading the data dir group is non-fatal, it just means the
      // tablet will fail to bootstrap later.
      WARN_NOT_OK(fs_manager_->dd_manager()->LoadDataDirGroupFromPB(
          tablet_id_, superblock.data_dir_group()),
          "failed to load DataDirGroup from superblock");
    } else if (tablet_data_state_ == TABLET_DATA_READY) {
      // If the superblock does not contain a DataDirGroup, this server has
      // likely been upgraded from before 1.5.0. Create a new DataDirGroup for
      // the tablet. If the data is not TABLET_DATA_READY, group creation is
      // pointless, as the tablet metadata will be deleted anyway.
      //
      // Since we don't know what directories the existing blocks are in, we
      // should assume the data is spread across all disks.
      RETURN_NOT_OK(fs_manager_->dd_manager()->CreateDataDirGroup(tablet_id_,
          fs::DataDirManager::DirDistributionMode::ACROSS_ALL_DIRS));
    }

    // Note: Previous versions of Kudu used MinimumOpId() as a "null" value on
    // disk for the last-logged opid, so we special-case it at load time and
    // consider it equal to "not present".
    if (superblock.has_tombstone_last_logged_opid() &&
        superblock.tombstone_last_logged_opid().IsInitialized() &&
        !OpIdEquals(MinimumOpId(), superblock.tombstone_last_logged_opid())) {
      tombstone_last_logged_opid_ = superblock.tombstone_last_logged_opid();
    } else {
      tombstone_last_logged_opid_.reset();
    }

    if (superblock.has_extra_config()) {
      extra_config_ = superblock.extra_config();
    } else {
      extra_config_.reset();
    }

    if (superblock.has_dimension_label()) {
      dimension_label_ = superblock.dimension_label();
    } else {
      dimension_label_.reset();
    }

    if (superblock.has_table_type() && superblock.table_type() != TableTypePB::DEFAULT_TABLE) {
      table_type_ = superblock.table_type();
    }

    std::unordered_map<int64_t, scoped_refptr<TxnMetadata>> txn_metas;
    for (const auto& txn_id_and_metadata : superblock.txn_metadata()) {
      const auto& txn_meta = txn_id_and_metadata.second;
      EmplaceOrDie(&txn_metas, txn_id_and_metadata.first,
          new TxnMetadata(
              txn_meta.has_aborted() && txn_meta.aborted(),
              txn_meta.has_commit_mvcc_op_timestamp() ?
                  make_optional(Timestamp(txn_meta.commit_mvcc_op_timestamp())) :
                  nullopt,
              txn_meta.has_commit_timestamp() ?
                  make_optional(Timestamp(txn_meta.commit_timestamp())) :
                  nullopt,
              txn_meta.has_flushed_committed_mrs() && txn_meta.flushed_committed_mrs()
          ));
    }
    txn_metadata_by_txn_id_ = std::move(txn_metas);
  }

  // Now is a good time to clean up any orphaned blocks that may have been
  // left behind from a crash just after replacing the superblock.
  if (!fs_manager()->read_only()) {
    DeleteOrphanedBlocks(orphaned_blocks);
  }

  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id,
                                      const vector<TxnInfoBeingFlushed>& txns_being_flushed) {
  {
    std::lock_guard<LockType> l(data_lock_);
    RETURN_NOT_OK(UpdateUnlocked(to_remove, to_add, last_durable_mrs_id, txns_being_flushed));
  }
  return Flush();
}

void TabletMetadata::AddOrphanedBlocks(const BlockIdContainer& block_ids) {
  std::lock_guard<LockType> l(data_lock_);
  AddOrphanedBlocksUnlocked(block_ids);
}

void TabletMetadata::AddOrphanedBlocksUnlocked(const BlockIdContainer& block_ids) {
  DCHECK(data_lock_.is_locked());
  orphaned_blocks_.insert(block_ids.begin(), block_ids.end());
}

void TabletMetadata::DeleteOrphanedBlocks(const BlockIdContainer& blocks) {
  if (PREDICT_FALSE(!FLAGS_enable_tablet_orphaned_block_deletion)) {
    LOG_WITH_PREFIX(WARNING) << "Not deleting " << blocks.size()
        << " block(s) from disk. Block deletion disabled via "
        << "--enable_tablet_orphaned_block_deletion=false";
    return;
  }

  BlockManager* bm = fs_manager()->block_manager();
  shared_ptr<BlockDeletionTransaction> transaction = bm->NewDeletionTransaction();
  for (const BlockId& b : blocks) {
    transaction->AddDeletedBlock(b);
  }
  WARN_NOT_OK(transaction->CommitDeletedBlocks(nullptr),
              "not all orphaned blocks were deleted");

  // Regardless of whether we deleted all the blocks or not, remove them from
  // the orphaned blocks list. If we failed to delete the blocks due to
  // hardware issues, there's not much we can do and we assume the disk isn't
  // coming back. At worst, this leaves some untracked orphaned blocks.
  {
    std::lock_guard<LockType> l(data_lock_);
    for (const BlockId& b : blocks) {
      orphaned_blocks_.erase(b);
    }
  }
}

void TabletMetadata::PinFlush() {
  std::lock_guard<LockType> l(data_lock_);
  CHECK_GE(num_flush_pins_, 0);
  num_flush_pins_++;
  VLOG(1) << "Number of flush pins: " << num_flush_pins_;
}

Status TabletMetadata::UnPinFlush() {
  std::unique_lock<LockType> l(data_lock_);
  CHECK_GT(num_flush_pins_, 0);
  num_flush_pins_--;
  if (needs_flush_) {
    l.unlock();
    RETURN_NOT_OK(Flush());
  }
  return Status::OK();
}

Status TabletMetadata::Flush() {
  TRACE_EVENT1("tablet", "TabletMetadata::Flush",
               "tablet_id", tablet_id_);

  MutexLock l_flush(flush_lock_);
  BlockIdContainer orphaned;
  TabletSuperBlockPB pb;
  vector<unique_ptr<MinLogIndexAnchorer>> anchors_needing_flush;
  {
    std::lock_guard<LockType> l(data_lock_);
    CHECK_GE(num_flush_pins_, 0);
    if (num_flush_pins_ > 0) {
      needs_flush_ = true;
      LOG(INFO) << "Not flushing: waiting for " << num_flush_pins_ << " pins to be released.";
      return Status::OK();
    }
    needs_flush_ = false;

    RETURN_NOT_OK(ToSuperBlockUnlocked(&pb, rowsets_));

    // Make a copy of the orphaned blocks list which corresponds to the superblock
    // that we're writing. It's important to take this local copy to avoid a race
    // in which another thread may add new orphaned blocks to the 'orphaned_blocks_'
    // set while we're in the process of writing the new superblock to disk. We don't
    // want to accidentally delete those blocks before that next metadata update
    // is persisted. See KUDU-701 for details.
    orphaned.assign(orphaned_blocks_.begin(), orphaned_blocks_.end());
    anchors_needing_flush = std::move(anchors_needing_flush_);
  }
  pre_flush_callback_();
  RETURN_NOT_OK(ReplaceSuperBlockUnlocked(pb));
  TRACE("Metadata flushed");
  l_flush.Unlock();

  // Now that we've flushed, we can unanchor our WALs by destructing our
  // anchors.
  anchors_needing_flush.clear();

  // Now that the superblock is written, try to delete the orphaned blocks.
  //
  // If we crash just before the deletion, we'll retry when reloading from
  // disk; the orphaned blocks were persisted as part of the superblock.
  DeleteOrphanedBlocks(orphaned);

  return Status::OK();
}

Status TabletMetadata::UpdateUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id,
    const vector<TxnInfoBeingFlushed>& txns_being_flushed) {
  DCHECK(data_lock_.is_locked());
  CHECK_NE(state_, kNotLoadedYet);
  if (last_durable_mrs_id != kNoMrsFlushed) {
    DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);
    last_durable_mrs_id_ = last_durable_mrs_id;
  }
  for (const auto& txn_id : txns_being_flushed) {
    auto txn_meta = FindOrDie(txn_metadata_by_txn_id_, txn_id);
    txn_meta->set_flushed_committed_mrs();
  }

  RowSetMetadataVector new_rowsets = rowsets_;
  auto it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      AddOrphanedBlocksUnlocked((*it)->GetAllBlocks());
      it = new_rowsets.erase(it);
    } else {
      it++;
    }
  }

  for (const shared_ptr<RowSetMetadata>& meta : to_add) {
    new_rowsets.push_back(meta);
  }
  rowsets_ = new_rowsets;

  TRACE("TabletMetadata updated");
  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlock(const TabletSuperBlockPB &pb) {
  {
    MutexLock l(flush_lock_);
    RETURN_NOT_OK_PREPEND(ReplaceSuperBlockUnlocked(pb), "Unable to replace superblock");
    fs_manager_->dd_manager()->DeleteDataDirGroup(tablet_id_);
  }

  RETURN_NOT_OK_PREPEND(LoadFromSuperBlock(pb),
                        "Failed to load data from superblock protobuf");

  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb) {
  flush_lock_.AssertAcquired();

  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
                            fs_manager_->env(), path, pb,
                            pb_util::OVERWRITE, pb_util::SYNC,
                            pb_util::SENSITIVE),
                        Substitute("Failed to write tablet metadata $0", tablet_id_));
  flush_count_for_tests_++;
  RETURN_NOT_OK(UpdateOnDiskSize());

  return Status::OK();
}

void TabletMetadata::SetPreFlushCallback(StatusClosure callback) {
  MutexLock l_flush(flush_lock_);
  pre_flush_callback_ = std::move(callback);
}

std::optional<consensus::OpId> TabletMetadata::tombstone_last_logged_opid() const {
  std::lock_guard<LockType> l(data_lock_);
  return tombstone_last_logged_opid_;
}

Status TabletMetadata::ReadSuperBlockFromDisk(TabletSuperBlockPB* superblock) const {
  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::ReadPBContainerFromPath(fs_manager_->env(), path, superblock, pb_util::SENSITIVE),
      Substitute("Could not load tablet metadata from $0", path));
  return Status::OK();
}

Status TabletMetadata::ToSuperBlock(TabletSuperBlockPB* super_block) const {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  std::lock_guard<LockType> l(data_lock_);
  return ToSuperBlockUnlocked(super_block, rowsets_);
}

Status TabletMetadata::ToSuperBlockUnlocked(TabletSuperBlockPB* super_block,
                                            const RowSetMetadataVector& rowsets) const {
  DCHECK(data_lock_.is_locked());
  // Convert to protobuf
  TabletSuperBlockPB pb;
  pb.set_table_id(table_id_);
  pb.set_tablet_id(tablet_id_);
  partition_.ToPB(pb.mutable_partition());
  pb.set_last_durable_mrs_id(last_durable_mrs_id_);
  pb.set_schema_version(schema_version_);
  RETURN_NOT_OK(partition_schema_.ToPB(*schema_, pb.mutable_partition_schema()));
  pb.set_table_name(table_name_);

  for (const shared_ptr<RowSetMetadata>& meta : rowsets) {
    meta->ToProtobuf(pb.add_rowsets());
  }

  for (const auto& txn_id_and_metadata : txn_metadata_by_txn_id_) {
    const auto& txn_meta = txn_id_and_metadata.second;
    TxnMetadataPB meta_pb = txn_meta->ToPB();
    InsertOrDie(pb.mutable_txn_metadata(), txn_id_and_metadata.first, meta_pb);
  }

  DCHECK(schema_->has_column_ids());
  RETURN_NOT_OK_PREPEND(SchemaToPB(*schema_, pb.mutable_schema()),
                        "Couldn't serialize schema into superblock");

  pb.set_tablet_data_state(tablet_data_state_);
  if (tombstone_last_logged_opid_ &&
      !OpIdEquals(MinimumOpId(), *tombstone_last_logged_opid_)) {
    *pb.mutable_tombstone_last_logged_opid() = *tombstone_last_logged_opid_;
  }

  for (const BlockId& block_id : orphaned_blocks_) {
    block_id.CopyToPB(pb.mutable_orphaned_blocks()->Add());
  }

  // Serialize the tablet's DataDirGroupPB if one exists. One may not exist if
  // this is called during a tablet deletion.
  DataDirGroupPB group_pb;
  if (fs_manager_->dd_manager()->GetDataDirGroupPB(tablet_id_, &group_pb).ok()) {
    pb.mutable_data_dir_group()->Swap(&group_pb);
  }

  pb.set_supports_live_row_count(supports_live_row_count_);

  if (extra_config_) {
    *pb.mutable_extra_config() = *extra_config_;
  }

  if (dimension_label_) {
    pb.set_dimension_label(*dimension_label_);
  }

  if (table_type_) {
    DCHECK_NE(TableTypePB::DEFAULT_TABLE, *table_type_);
    pb.set_table_type(*table_type_);
  }

  super_block->Swap(&pb);
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata>* rowset) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  unique_ptr<RowSetMetadata> scoped_rsm;
  RETURN_NOT_OK(RowSetMetadata::CreateNew(this, rowset_idx, &scoped_rsm));
  rowset->reset(DCHECK_NOTNULL(scoped_rsm.release()));
  return Status::OK();
}

void TabletMetadata::AddTxnMetadata(int64_t txn_id, unique_ptr<MinLogIndexAnchorer> log_anchor) {
  std::lock_guard<LockType> l(data_lock_);
  EmplaceOrDie(&txn_metadata_by_txn_id_, txn_id, new TxnMetadata());
  anchors_needing_flush_.emplace_back(std::move(log_anchor));
}

void TabletMetadata::BeginCommitTransaction(int64_t txn_id, Timestamp mvcc_op_timestamp,
                                            unique_ptr<MinLogIndexAnchorer> log_anchor) {
  std::lock_guard<LockType> l(data_lock_);
  auto txn_metadata = FindOrDie(txn_metadata_by_txn_id_, txn_id);
  // NOTE: we may already have an MVCC op timestamp if we are bootstrapping and
  // the timestamp was persisted already, in which case, we don't need to
  // anchor the WAL to ensure the timestamp's persistence in metadata.
  if (!txn_metadata->commit_mvcc_op_timestamp()) {
    txn_metadata->set_commit_mvcc_op_timestamp(mvcc_op_timestamp);
    anchors_needing_flush_.emplace_back(std::move(log_anchor));
  }
}

void TabletMetadata::AddCommitTimestamp(int64_t txn_id, Timestamp commit_timestamp,
                                        unique_ptr<MinLogIndexAnchorer> log_anchor) {
  std::lock_guard<LockType> l(data_lock_);
  auto txn_metadata = FindOrDie(txn_metadata_by_txn_id_, txn_id);
  txn_metadata->set_commit_timestamp(commit_timestamp);
  anchors_needing_flush_.emplace_back(std::move(log_anchor));
}

void TabletMetadata::AbortTransaction(int64_t txn_id, unique_ptr<MinLogIndexAnchorer> log_anchor) {
  std::lock_guard<LockType> l(data_lock_);
  // NOTE: we can't emplace with a raw pointer here; if the lookup succeeds, we
  // wouldn't use it and we'd have a memory leak, so use scoped_refptr.
  auto txn_metadata = LookupOrEmplace(&txn_metadata_by_txn_id_, txn_id,
                                      scoped_refptr<TxnMetadata>(new TxnMetadata));
  CHECK(txn_metadata);
  txn_metadata->set_aborted();
  anchors_needing_flush_.emplace_back(std::move(log_anchor));
}

bool TabletMetadata::HasTxnMetadata(int64_t txn_id, TxnState* state, Timestamp* timestamp) {
  std::lock_guard<LockType> l(data_lock_);
  auto txn_meta = FindPtrOrNull(txn_metadata_by_txn_id_, txn_id);
  if (txn_meta) {
    if (!state) return true;
    if (txn_meta->commit_timestamp()) {
      *state = kCommitted;
      if (timestamp) {
        DCHECK(txn_meta->commit_timestamp());
        *timestamp = *txn_meta->commit_timestamp();
      }
    } else if (txn_meta->aborted()) {
      *state = kAborted;
    } else if (txn_meta->commit_mvcc_op_timestamp()) {
      *state = kCommitInProgress;
      if (timestamp) {
        DCHECK(txn_meta->commit_mvcc_op_timestamp());
        *timestamp = *txn_meta->commit_mvcc_op_timestamp();
      }
    } else {
      *state = kOpen;
    }
    return true;
  }
  return false;
}

void TabletMetadata::GetTxnIds(unordered_set<int64_t>* in_flight_txn_ids,
                               unordered_set<int64_t>* terminal_txn_ids,
                               unordered_set<int64_t>* txn_ids_with_mrs) {
  std::unordered_set<int64_t> in_flights;
  std::unordered_set<int64_t> terminals;
  std::unordered_set<int64_t> needs_mrs;
  std::lock_guard<LockType> l(data_lock_);
  for (const auto& txn_id_and_metadata : txn_metadata_by_txn_id_) {
    const auto& txn_id = txn_id_and_metadata.first;
    const auto& txn_meta = txn_id_and_metadata.second;
    if (txn_meta->commit_timestamp() || txn_meta->aborted()) {
      if (terminal_txn_ids) {
        EmplaceOrDie(&terminals, txn_id);
      }
    } else {
      EmplaceOrDie(&in_flights, txn_id);
    }
    // If we have not flushed the MRS after committing, the bootstrap process
    // will need to create an MRS for it, even if the transaction is committed.
    if (txn_ids_with_mrs &&
        !txn_meta->flushed_committed_mrs() &&
        !txn_meta->aborted()) {
      EmplaceOrDie(&needs_mrs, txn_id);
    }
  }
  *in_flight_txn_ids = std::move(in_flights);
  if (terminal_txn_ids) {
    *terminal_txn_ids = std::move(terminals);
  }
  if (txn_ids_with_mrs) {
    *txn_ids_with_mrs = std::move(needs_mrs);
  }
}

unordered_map<int64_t, scoped_refptr<TxnMetadata>> TabletMetadata::GetTxnMetadata() const {
  std::lock_guard<LockType> l(data_lock_);
  return txn_metadata_by_txn_id_;
}

bool TabletMetadata::GetTxnMetadataPB(int64_t txn_id, TxnMetadataPB* pb) const {
  std::lock_guard<LockType> l(data_lock_);
  auto txn_meta = FindPtrOrNull(txn_metadata_by_txn_id_, txn_id);
  if (!txn_meta) {
    return false;
  }
  *pb = txn_meta->ToPB();
  return true;
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) const {
  for (const shared_ptr<RowSetMetadata>& rowset_meta : rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return nullptr;
}

RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) {
  std::lock_guard<LockType> l(data_lock_);
  for (const shared_ptr<RowSetMetadata>& rowset_meta : rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return nullptr;
}

void TabletMetadata::SetSchema(const SchemaPtr& schema, uint32_t version) {
  // In case this is the last reference to the schema, destruct the pointer
  // outside the lock.
  SchemaPtr old_schema;
  {
    std::lock_guard<LockType> l(data_lock_);
    SwapSchemaUnlocked(schema, version, &old_schema);
  }
}

void TabletMetadata::SwapSchemaUnlocked(SchemaPtr schema, uint32_t version,
                                        SchemaPtr* old_schema) {
  DCHECK(schema->has_column_ids());
  *old_schema = std::move(schema_);
  schema_ = std::move(schema);
  schema_version_ = version;
}

void TabletMetadata::SetTableName(const string& table_name) {
  std::lock_guard<LockType> l(data_lock_);
  table_name_ = table_name;
}

string TabletMetadata::table_name() const {
  std::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return table_name_;
}

uint32_t TabletMetadata::schema_version() const {
  std::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return schema_version_;
}

void TabletMetadata::set_tablet_data_state(TabletDataState state) {
  std::lock_guard<LockType> l(data_lock_);
  if (state == TABLET_DATA_READY) {
    tombstone_last_logged_opid_.reset();
  }
  tablet_data_state_ = state;
}

TabletDataState TabletMetadata::tablet_data_state() const {
  std::lock_guard<LockType> l(data_lock_);
  return tablet_data_state_;
}

void TabletMetadata::SetExtraConfig(TableExtraConfigPB extra_config) {
  std::lock_guard<LockType> l(data_lock_);
  extra_config_ = std::move(extra_config);
}

optional<TableExtraConfigPB> TabletMetadata::extra_config() const {
  std::lock_guard<LockType> l(data_lock_);
  return extra_config_;
}

optional<string> TabletMetadata::dimension_label() const {
  std::lock_guard<LockType> l(data_lock_);
  return dimension_label_;
}

const optional<TableTypePB>& TabletMetadata::table_type() const {
  std::lock_guard<LockType> l(data_lock_);
  return table_type_;
}

} // namespace tablet
} // namespace kudu
