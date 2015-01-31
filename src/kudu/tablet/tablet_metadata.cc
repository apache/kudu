// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/tablet_metadata.h"

#include <algorithm>
#include <string>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

using base::subtle::Barrier_AtomicIncrement;
using strings::Substitute;

using kudu::metadata::QuorumPB;

namespace kudu {
namespace tablet {

const int64 kNoDurableMemStore = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================

Status TabletMetadata::CreateNew(FsManager* fs_manager,
                                 const TabletMasterBlockPB& master_block,
                                 const string& table_name,
                                 const Schema& schema,
                                 const string& start_key, const string& end_key,
                                 const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                                 scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager,
                                                       master_block,
                                                       table_name,
                                                       schema,
                                                       start_key,
                                                       end_key,
                                                       initial_remote_bootstrap_state));
  RETURN_NOT_OK(ret->Flush());
  metadata->swap(ret);
  // TODO: should we verify that neither of the blocks referenced in the master block
  // exist?
  return Status::OK();
}

Status TabletMetadata::Load(FsManager* fs_manager,
                            const TabletMasterBlockPB& master_block,
                            scoped_refptr<TabletMetadata>* metadata) {
  scoped_refptr<TabletMetadata> ret(new TabletMetadata(fs_manager, master_block));
  RETURN_NOT_OK(ret->LoadFromDisk());
  metadata->swap(ret);
  return Status::OK();
}

Status TabletMetadata::LoadOrCreate(FsManager* fs_manager,
                                    const TabletMasterBlockPB& master_block,
                                    const string& table_name,
                                    const Schema& schema,
                                    const string& start_key, const string& end_key,
                                    const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                                    scoped_refptr<TabletMetadata>* metadata) {
  Status s = Load(fs_manager, master_block, metadata);
  if (s.ok()) {
    if (!(*metadata)->schema().Equals(schema)) {
      return Status::Corruption(Substitute("Schema on disk ($0) does not "
        "match expected schema ($1)", (*metadata)->schema().ToString(),
        schema.ToString()));
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    return CreateNew(fs_manager, master_block, table_name, schema,
                     start_key, end_key, initial_remote_bootstrap_state,
                     metadata);
  } else {
    return s;
  }
}

Status TabletMetadata::OpenMasterBlock(Env* env,
                                       const string& master_block_path,
                                       const string& expected_tablet_id,
                                       TabletMasterBlockPB* master_block) {
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(env, master_block_path,
                                                 master_block));
  if (expected_tablet_id != master_block->tablet_id()) {
    LOG_AND_RETURN(ERROR, Status::Corruption(
        strings::Substitute("Corrupt master block $0: PB has wrong tablet ID",
                            master_block_path),
        master_block->ShortDebugString()));
  }
  return Status::OK();
}

Status TabletMetadata::PersistMasterBlock(FsManager* fs_manager,
                                          const TabletMasterBlockPB& pb) {
  string path = fs_manager->GetMasterBlockPath(pb.tablet_id());
  return pb_util::WritePBContainerToPath(fs_manager->env(), path, pb,
      FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
}


TabletMetadata::TabletMetadata(FsManager *fs_manager,
                               const TabletMasterBlockPB& master_block,
                               const string& table_name,
                               const Schema& schema,
                               const string& start_key,
                               const string& end_key,
                               const TabletBootstrapStatePB& remote_bootstrap_state)
  : state_(kNotWrittenYet),
    start_key_(start_key), end_key_(end_key),
    fs_manager_(fs_manager),
    master_block_(master_block),
    sblk_sequence_(0),
    next_rowset_idx_(0),
    last_durable_mrs_id_(kNoDurableMemStore),
    schema_(schema),
    schema_version_(0),
    table_name_(table_name),
    remote_bootstrap_state_(remote_bootstrap_state),
    num_flush_pins_(0),
    needs_flush_(false) {
  CHECK(schema_.has_column_ids());
}

TabletMetadata::~TabletMetadata() {
}

TabletMetadata::TabletMetadata(FsManager *fs_manager, const TabletMasterBlockPB& master_block)
  : state_(kNotLoadedYet),
    fs_manager_(fs_manager),
    master_block_(master_block),
    next_rowset_idx_(0),
    num_flush_pins_(0),
    needs_flush_(false) {
}

Status TabletMetadata::LoadFromDisk() {
  CHECK_EQ(state_, kNotLoadedYet);
  TabletSuperBlockPB superblock;
  RETURN_NOT_OK(ReadSuperBlock(&superblock));
  VLOG(1) << "Loaded tablet superblock " << superblock.DebugString();

  RETURN_NOT_OK_PREPEND(LoadFromSuperBlockUnlocked(superblock),
                        "Failed to load data from superblock protobuf");

  state_ = kInitialized;
  return Status::OK();
}

Status TabletMetadata::LoadFromSuperBlockUnlocked(const TabletSuperBlockPB& superblock) {
  // Verify that the tablet id matches with the one in the protobuf
  if (superblock.oid() != master_block_.tablet_id()) {
    return Status::Corruption("Expected id=" + master_block_.tablet_id() +
                              " found " + superblock.oid(),
                              superblock.DebugString());
  }

  sblk_sequence_ = superblock.sequence() + 1;
  start_key_ = superblock.start_key();
  end_key_ = superblock.end_key();
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

  return Status::OK();
}

Status TabletMetadata::ReadSuperBlock(TabletSuperBlockPB *pb) {
  CHECK_EQ(state_, kNotLoadedYet);
  TabletSuperBlockPB pb2;
  Status sa, sb;

  // Try to read the block_a if exists
  sa = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_a()), pb);

  // Try to read the block_b if exists
  sb = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_b()), &pb2);

  // Both super-blocks are valid, pick the latest
  if (sa.ok() && sb.ok()) {
    if (pb->sequence() < pb2.sequence()) {
      *pb = pb2;
    }
    return Status::OK();
  }

  // block-a is valid, block-b is not (may not exists or be corrupted)
  if (sa.ok() && !sb.ok()) {
    return Status::OK();
  }

  // block-b is valid, block-a is not (may not exists or be corrupted)
  if (!sa.ok() && sb.ok()) {
    *pb = pb2;
    return Status::OK();
  }

  // No super-block found
  if (sa.IsNotFound() && sb.IsNotFound()) {
    return Status::NotFound("Tablet '" + master_block_.tablet_id() + "' SuperBlock not found",
                            master_block_.DebugString());
  }

  // Both super-blocks are corrupted
  if (sa.IsCorruption() && sb.IsCorruption()) {
    return Status::NotFound("Tablet '" + master_block_.tablet_id() + "' SuperBlocks are corrupted",
                            master_block_.DebugString());
  }

  return sa;
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add) {
  {
    boost::lock_guard<LockType> l(data_lock_);
    RETURN_NOT_OK(UpdateUnlocked(to_remove, to_add, last_durable_mrs_id_));
  }
  return Flush();
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
  DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);
  last_durable_mrs_id_ = last_durable_mrs_id;

  RowSetMetadataVector new_rowsets = rowsets_;
  RowSetMetadataVector::iterator it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
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

  {
    boost::lock_guard<LockType> l(data_lock_);
    RETURN_NOT_OK_PREPEND(LoadFromSuperBlockUnlocked(pb),
                          "Failed to load data from superblock protobuf");
  }
  return Status::OK();
}

Status TabletMetadata::ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb) {
  flush_lock_.AssertAcquired();
  // Write out and replace one of the two superblocks.
  //
  // When writing out the first superblock, it's OK to retain the second
  // (stale) one. While opening the tablet, the superblock with the latest
  // sequence number will be loaded and the other one will be ignored.
  BlockId a_blk(master_block_.block_a());
  BlockId b_blk(master_block_.block_b());
  TRACE("Writing metadata block");
  RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(
      sblk_sequence_ & 1 ? a_blk : b_blk, pb));
  sblk_sequence_++;
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
  pb.set_sequence(sblk_sequence_);
  pb.set_oid(oid());
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
