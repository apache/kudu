// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/tablet_metadata.h"

#include <algorithm>
#include <string>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

using base::subtle::Barrier_AtomicIncrement;
using strings::Substitute;

using kudu::consensus::QuorumPB;

namespace kudu {
namespace tablet {

const int64 kNoDurableMemStore = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const string& table_name,
                                 const Schema& schema,
                                 const string& start_key, const string& end_key,
                                 const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                                 scoped_refptr<TabletMetadata>* metadata) {

  // Verify that no existing tablet exists with the same ID.
  if (fs_manager->env()->FileExists(fs_manager->GetTabletMetadataPath(tablet_id))) {
    return Status::AlreadyPresent("Tablet already exists", tablet_id);
  }

  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager,
                                                       tablet_id,
                                                       table_name,
                                                       schema,
                                                       start_key,
                                                       end_key,
                                                       initial_remote_bootstrap_state));
  RETURN_NOT_OK(ret->Flush());
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            const string& tablet_id,
                            scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager, tablet_id));
  RETURN_NOT_OK(ret->LoadFromDisk());
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    const string& tablet_id,
                                    const string& table_name,
                                    const Schema& schema,
                                    const string& start_key, const string& end_key,
                                    const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, tablet_id, metadata);
  if (s.ok()) {
    if (!(*metadata)->schema().Equals(schema)) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", (*metadata)->schema().ToString(),
        schema.ToString()));
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    return CreateNew(fs_manager, tablet_id, table_name, schema,
                     start_key, end_key, initial_remote_bootstrap_state,
                     metadata);
  } else {
    return s;
  }
}

TabletMetadata::TabletMetadata(FsManager *fs_manager,
                               const string& tablet_id,
                               const string& table_name,
                               const Schema& schema,
                               const string& start_key,
                               const string& end_key,
                               const TabletBootstrapStatePB& remote_bootstrap_state)
  : state_(kNotWrittenYet),
    tablet_id_(tablet_id),
    start_key_(start_key), end_key_(end_key),
    fs_manager_(fs_manager),
    next_rowset_idx_(0),
    last_durable_mrs_id_(kNoDurableMemStore),
    schema_(schema),
    schema_version_(0),
    table_name_(table_name),
    remote_bootstrap_state_(remote_bootstrap_state),
    num_flush_pins_(0),
    needs_flush_(false),
    pre_flush_callback_(Bind(DoNothingStatusClosure)) {
  CHECK(schema_.has_column_ids());
  CHECK_GT(schema_.num_key_columns(), 0);
}

TabletMetadata::~TabletMetadata() {
}

TabletMetadata::TabletMetadata(FsManager *fs_manager, const string& tablet_id)
  : state_(kNotLoadedYet),
    tablet_id_(tablet_id),
    fs_manager_(fs_manager),
    next_rowset_idx_(0),
    num_flush_pins_(0),
    needs_flush_(false),
    pre_flush_callback_(Bind(DoNothingStatusClosure)) {
}

Status TabletMetadata::LoadFromDisk() {
  TRACE_EVENT1("tablet", "TabletMetadata::LoadFromDisk",
               "tablet_id", tablet_id_);

  CHECK_EQ(state_, kNotLoadedYet);

  TabletSuperBlockPB superblock;
  string path = fs_manager_->GetTabletMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(pb_util::ReadPBContainerFromPath(
                            fs_manager_->env(), path, &superblock),
                        Substitute("Could not load tablet metadata from $0", path));

  RETURN_NOT_OK_PREPEND(LoadFromSuperBlock(superblock),
                        "Failed to load data from superblock protobuf");
  state_ = kInitialized;
  return Status::OK();
}

Status TabletMetadata::LoadFromSuperBlock(const TabletSuperBlockPB& superblock) {
  {
    boost::lock_guard<LockType> l(data_lock_);

    // Verify that the tablet id matches with the one in the protobuf
    if (superblock.tablet_id() != tablet_id_) {
      return Status::Corruption("Expected id=" + tablet_id_ +
                                " found " + superblock.tablet_id(),
                                superblock.DebugString());
    }

    start_key_ = superblock.start_key();
    end_key_ = superblock.end_key();
    table_id_ = superblock.table_id();
    last_durable_mrs_id_ = superblock.last_durable_mrs_id();

    table_name_ = superblock.table_name();
    schema_version_ = superblock.schema_version();
    RETURN_NOT_OK_PREPEND(SchemaFromPB(superblock.schema(), &schema_),
                          "Failed to parse Schema from superblock " +
                          superblock.ShortDebugString());
    DCHECK(schema_.has_column_ids());

    remote_bootstrap_state_ = superblock.remote_bootstrap_state();

    BOOST_FOREACH(const RowSetDataPB& rowset_pb, superblock.rowsets()) {
      gscoped_ptr<RowSetMetadata> rowset_meta;
      RETURN_NOT_OK(RowSetMetadata::Load(this, rowset_pb, &rowset_meta));
      next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
      rowsets_.push_back(shared_ptr<RowSetMetadata>(rowset_meta.release()));
    }

    {
      vector<BlockId> blocks;
      BOOST_FOREACH(const BlockIdPB& block_pb, superblock.orphaned_blocks()) {
        blocks.push_back(BlockId::FromPB(block_pb));
      }
      AddOrphanedBlocksUnlocked(blocks);
    }
  }

  // Now is a good time to clean up any orphaned blocks that may have been
  // left behind from a crash just after replacing the superblock.
  DeleteOrphanedBlocks();

  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id) {
  {
    boost::lock_guard<LockType> l(data_lock_);
    RETURN_NOT_OK(UpdateUnlocked(to_remove, to_add, last_durable_mrs_id));
  }
  return Flush();
}

void TabletMetadata::AddOrphanedBlocks(const vector<BlockId>& blocks) {
  boost::lock_guard<LockType> l(data_lock_);
  AddOrphanedBlocksUnlocked(blocks);
}

void TabletMetadata::AddOrphanedBlocksUnlocked(const vector<BlockId>& blocks) {
  DCHECK(data_lock_.is_locked());
  orphaned_blocks_.insert(orphaned_blocks_.end(),
                          blocks.begin(), blocks.end());
}

void TabletMetadata::DeleteOrphanedBlocks() {
  vector<BlockId> blocks;
  {
    boost::lock_guard<LockType> l(data_lock_);
    blocks.swap(orphaned_blocks_);
  }
  vector<BlockId> failed;
  Status first_failure;
  BOOST_FOREACH(const BlockId& b, blocks) {
    Status s = fs_manager()->DeleteBlock(b);
    if (s.IsNotFound()) {
      continue;
    }
    WARN_NOT_OK(s, Substitute("Could not delete block $0", b.ToString()));
    if (!s.ok()) {
      failed.push_back(b);
      if (first_failure.ok()) {
        first_failure = s;
      }
    }
  }

  if (!first_failure.ok()) {
    AddOrphanedBlocks(failed);
    LOG(WARNING) << "Could not delete " << failed.size() <<
                    " blocks. First failure: " << first_failure.ToString();
  }
}

void TabletMetadata::PinFlush() {
  boost::lock_guard<LockType> l(data_lock_);
  CHECK_GE(num_flush_pins_, 0);
  num_flush_pins_++;
  VLOG(1) << "Number of flush pins: " << num_flush_pins_;
}

Status TabletMetadata::UnPinFlush() {
  boost::lock_guard<LockType> l(data_lock_);
  CHECK_GT(num_flush_pins_, 0);
  num_flush_pins_--;
  if (needs_flush_) {
    RETURN_NOT_OK(Flush());
  }
  return Status::OK();
}

Status TabletMetadata::Flush() {
  TRACE_EVENT1("tablet", "TabletMetadata::Flush",
               "tablet_id", tablet_id_);

  MutexLock l_flush(flush_lock_);

  TabletSuperBlockPB pb;
  {
    boost::lock_guard<LockType> l(data_lock_);
    CHECK_GE(num_flush_pins_, 0);
    if (num_flush_pins_ > 0) {
      needs_flush_ = true;
      LOG(INFO) << "Not flushing: waiting for " << num_flush_pins_ << " pins to be released.";
      return Status::OK();
    }
    needs_flush_ = false;

    RETURN_NOT_OK(ToSuperBlockUnlocked(&pb, rowsets_));
  }
  pre_flush_callback_.Run();
  RETURN_NOT_OK(ReplaceSuperBlockUnlocked(pb));
  TRACE("Metadata flushed");
  return Status::OK();
}

Status TabletMetadata::UpdateUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id) {
  DCHECK(data_lock_.is_locked());
  CHECK_NE(state_, kNotLoadedYet);
  if (last_durable_mrs_id != kNoMrsFlushed) {
    DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);
    last_durable_mrs_id_ = last_durable_mrs_id;
  }

  RowSetMetadataVector new_rowsets = rowsets_;
  RowSetMetadataVector::iterator it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      AddOrphanedBlocksUnlocked((*it)->GetAllBlocks());
      it = new_rowsets.erase(it);
    } else {
      it++;
    }
  }

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, to_add) {
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
                            pb_util::OVERWRITE, pb_util::SYNC),
                        Substitute("Failed to write tablet metadata $0", tablet_id_));

  // Now that the superblock is written, try to delete the orphaned blocks.
  //
  // If we crash just before the deletion, we'll retry when reloading from
  // disk; the orphaned blocks were persisted as part of the superblock.
  DeleteOrphanedBlocks();

  return Status::OK();
}

Status TabletMetadata::ToSuperBlock(TabletSuperBlockPB* super_block) const {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  boost::lock_guard<LockType> l(data_lock_);
  return ToSuperBlockUnlocked(super_block, rowsets_);
}

Status TabletMetadata::ToSuperBlockUnlocked(TabletSuperBlockPB* super_block,
                                            const RowSetMetadataVector& rowsets) const {
  DCHECK(data_lock_.is_locked());
  // Convert to protobuf
  TabletSuperBlockPB pb;
  pb.set_table_id(table_id_);
  pb.set_tablet_id(tablet_id_);
  pb.set_start_key(start_key_);
  pb.set_end_key(end_key_);
  pb.set_last_durable_mrs_id(last_durable_mrs_id_);
  pb.set_schema_version(schema_version_);
  pb.set_table_name(table_name_);

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, rowsets) {
    meta->ToProtobuf(pb.add_rowsets());
  }

  DCHECK(schema_.has_column_ids());
  RETURN_NOT_OK_PREPEND(SchemaToPB(schema_, pb.mutable_schema()),
                        "Couldn't serialize schema into superblock");

  pb.set_remote_bootstrap_state(remote_bootstrap_state_);

  BOOST_FOREACH(const BlockId& block_id, orphaned_blocks_) {
    block_id.CopyToPB(pb.mutable_orphaned_blocks()->Add());
  }

  super_block->Swap(&pb);
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata> *rowset,
                                    const Schema& schema) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  gscoped_ptr<RowSetMetadata> scoped_rsm;
  RETURN_NOT_OK(RowSetMetadata::CreateNew(this, rowset_idx, schema, &scoped_rsm));
  rowset->reset(DCHECK_NOTNULL(scoped_rsm.release()));
  return Status::OK();
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) const {
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return NULL;
}

RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) {
  boost::lock_guard<LockType> l(data_lock_);
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return NULL;
}

void TabletMetadata::SetSchema(const Schema& schema, uint32_t version) {
  DCHECK(schema.has_column_ids());
  boost::lock_guard<LockType> l(data_lock_);
  schema_ = schema;
  schema_version_ = version;
}

void TabletMetadata::SetTableName(const string& table_name) {
  boost::lock_guard<LockType> l(data_lock_);
  table_name_ = table_name;
}

const string& TabletMetadata::table_name() const {
  boost::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return table_name_;
}

uint32_t TabletMetadata::schema_version() const {
  boost::lock_guard<LockType> l(data_lock_);
  DCHECK_NE(state_, kNotLoadedYet);
  return schema_version_;
}

Schema TabletMetadata::schema() const {
  boost::lock_guard<LockType> l(data_lock_);
  return schema_;
}

void TabletMetadata::set_remote_bootstrap_state(TabletBootstrapStatePB state) {
  boost::lock_guard<LockType> l(data_lock_);
  remote_bootstrap_state_ = state;
}

TabletBootstrapStatePB TabletMetadata::remote_bootstrap_state() const {
  boost::lock_guard<LockType> l(data_lock_);
  return remote_bootstrap_state_;
}

} // namespace tablet
} // namespace kudu
