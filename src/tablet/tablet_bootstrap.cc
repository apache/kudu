// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_bootstrap.h"

#include <boost/foreach.hpp>
#include <string>
#include <utility>
#include <vector>

#include "common/partial_row.h"
#include "common/row_changelist.h"
#include "common/wire_protocol.h"
#include "consensus/log.h"
#include "consensus/log_reader.h"
#include "consensus/log_util.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/ref_counted.h"
#include "gutil/stl_util.h"
#include "gutil/strings/util.h"
#include "gutil/walltime.h"
#include "server/clock.h"
#include "server/metadata.h"
#include "server/fsmanager.h"
#include "tablet/lock_manager.h"
#include "tablet/tablet.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/change_config_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/transactions/write_util.h"
#include "util/path_util.h"
#include "util/locks.h"

using boost::shared_lock;
using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusContext;
using kudu::consensus::OperationPB;
using kudu::consensus::OpId;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::ALTER_SCHEMA_OP;
using kudu::consensus::CHANGE_CONFIG_OP;
using kudu::consensus::OP_ABORT;
using kudu::consensus::WRITE_OP;
using kudu::log::Log;
using kudu::log::LogEntryPB;
using kudu::log::LogOptions;
using kudu::log::LogReader;
using kudu::log::OpIdAnchorRegistry;
using kudu::log::OPERATION;
using kudu::metadata::QuorumPB;
using kudu::metadata::TabletMetadata;
using kudu::metadata::TabletSuperBlockPB;
using kudu::metadata::RowSetMetadata;
using kudu::server::Clock;
using kudu::tablet::MutationResultPB;
using kudu::tablet::PreparedRowWrite;
using kudu::tablet::Tablet;
using kudu::tserver::AlterSchemaRequestPB;
using kudu::tserver::ChangeConfigRequestPB;
using kudu::tserver::WriteRequestPB;
using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {
namespace tablet {

using log::OpIdHashFunctor;
using log::OpIdEquals;
using log::OpIdEqualsFunctor;

struct MutationInput;
struct ReplayState;

// Bootstraps an existing tablet, fetching the initial state from other replicas
// or locally and rebuilding soft state by playing log segments. A bootstrapped tablet
// can then be added to an existing quorum as a LEARNER, which will bring its
// state up to date with the rest of the quorum, or it can start serving the data
// itself, after it has been appointed LEADER of that particular quorum.
//
// TODO Because the table that is being rebuilt is never flushed/compacted, consensus
// is only set on the tablet after bootstrap, when we get to flushes/compactions though
// we need to set it before replay or we won't be able to re-rebuild.
class TabletBootstrap {
 public:
  TabletBootstrap(gscoped_ptr<metadata::TabletMetadata> meta,
                  const scoped_refptr<Clock>& clock,
                  MetricContext* metric_context,
                  TabletStatusListener* listener);

  // Plays the log segments, rebuilding the portion of the Tablet's soft
  // state that is present in the log (additional soft state may be present
  // in other replicas).
  // A successful call will yield the rebuilt tablet and the rebuilt log.
  Status Bootstrap(std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                   gscoped_ptr<log::Log>* rebuilt_log,
                   scoped_refptr<log::OpIdAnchorRegistry>* opid_anchor_registry);

 private:
  // Fetches the latest blocks for a tablet and Open()s that tablet.
  //
  // TODO get blocks from other replicas
  Status FetchBlocksAndOpenTablet(bool* fetched);

  // Fetches the latest log segments for the Tablet that will allow to rebuild
  // the tablet's soft state. If there are existing log segments in the tablet's
  // log directly they are moved to a "log-recovery" directory which is deleted
  // when the replay process is completed (as they have been duplicated in the
  // current log directory).
  //
  // If a "log-recovery" directory is already present, we will continue to replay
  // from the "log-recovery" directory. Tablet metadata is updated once replay
  // has finished from the "log-recovery" directory.
  //
  // TODO get log segments from other replicas.
  Status FetchLogSegments(bool* needs_recovery);

  // Opens a new log in the tablet's log directory.
  // The directory is expected to be clean.
  Status OpenNewLog();

  // Checks if a previous attempt at a recovery has been made: if so,
  // sets 'needs_recovery' to true.  Otherwise, moves the log segments
  // present in the tablet's log dir into the log recovery directory.
  //
  // Replaying the segments in the log recovery directory will create
  // a new log that will go into the normal tablet wal directory.
  Status PrepareRecoveryDir(bool* needs_recovery);

  // Plays the log segments into the tablet being built.
  // The process of playing the segments generates a new log that can be continued
  // later on when then tablet is rebuilt and starts accepting writes from clients.
  Status PlaySegments();

  Status PlayWriteRequest(OperationPB* replicate_op,
                          const OperationPB& commit_op);

  Status PlayAlterSchemaRequest(OperationPB* replicate_op,
                                const OperationPB& commit_op);

  Status PlayChangeConfigRequest(OperationPB* replicate_op,
                                 const OperationPB& commit_op);

  // Plays inserts, skipping those that have already been flushed.
  Status PlayInsertions(WriteTransactionContext* tx_ctx,
                        const SchemaPB& schema_pb,
                        const RowOperationsPB& rows,
                        const TxResultPB& result,
                        const consensus::OpId& committed_op_id,
                        int32_t* last_insert_op_idx);

  // Plays a mutations block, skipping those that have already been flushed.
  Status PlayMutations(WriteTransactionContext* tx_ctx,
                       RowwiseRowBlockPB* row_keys,
                       const string& encoded_mutations,
                       const TxResultPB& result,
                       const consensus::OpId& committed_op_id,
                       int32_t first_mutate_op_idx);

  // Plays a single mutation. 'applied_mutation' indicates whether the mutation
  // was actually applied or was skipped.
  Status PlayMutation(const MutationInput& mutation_input,
                      const TxOperationPB& op_result,
                      bool* applied_mutation);

  // Checks if the MRS that is the target of this mutation has
  // been flushed and applies it otherwise. 'applied_mutation'
  // indicates whether the mutation was actually applied or was
  // skipped.
  Status HandleMRSMutation(const MutationInput& mutation_input,
                           const MutationTargetPB& mutation_target,
                           bool* applied_mutation);

  // Checks if the DMS that is the target of this mutation has
  // been flushed and applies it otherwise. 'applied_mutation'
  // indicates whether the mutation was actually applied or was
  // skipped.
  Status HandleDMSMutation(const MutationInput& mutation_input,
                           const MutationTargetPB& mutation_target,
                           bool* applied_mutation);

  // Applies a mutation
  Status ApplyMutation(const MutationInput& mutation_input);

  // Handlers for each type of message seen in the log during replay.
  Status HandleEntry(ReplayState* state, LogEntryPB* entry);
  Status HandleReplicateMessage(ReplayState* state, LogEntryPB* entry);
  Status HandleCommitMessage(ReplayState* state, LogEntryPB* entry);
  Status HandleEntryPair(LogEntryPB* replicate_entry, LogEntryPB* commit_entry);

  // Decodes a Timestamp from the provided string and updates the clock
  // with it.
  Status UpdateClock(const string& timestamp);

  // Removes the recovery directory.
  Status RemoveRecoveryDir();

  gscoped_ptr<metadata::TabletMetadata> meta_;
  scoped_refptr<Clock> clock_;
  MetricContext* metric_context_;
  TabletStatusListener* listener_;
  gscoped_ptr<tablet::Tablet> tablet_;
  scoped_refptr<log::OpIdAnchorRegistry> opid_anchor_registry_;
  gscoped_ptr<log::Log> log_;
  gscoped_ptr<log::LogReader> log_reader_;

  Arena arena_;

  DISALLOW_COPY_AND_ASSIGN(TabletBootstrap);
};

TabletStatusListener::TabletStatusListener(
    const metadata::TabletMetadata& meta)
    : tablet_id_(meta.oid()),
      table_name_(meta.table_name()),
      start_key_(meta.start_key()),
      end_key_(meta.end_key()),
      last_status_("") {
}

TabletStatusListener::~TabletStatusListener() {
}

void TabletStatusListener::StatusMessage(const string& status) {
  LOG(INFO) << "Tablet id " << tablet_id_ << ": " << status;
  boost::lock_guard<boost::shared_mutex> l(lock_);
  last_status_ = status;
}


// helper to encapsulate the arguments required for a mutation
struct MutationInput {
  MutationInput(WriteTransactionContext* tx_ctx,
                const consensus::OpId& committed_op_id,
                int mutation_op_block_index,
                const Schema& key_schema,
                const Schema& changelist_schema,
                const uint8_t* row_key_ptr,
                const RowChangeList* changelist);

  WriteTransactionContext* tx_ctx;
  const consensus::OpId& committed_op_id;
  int mutation_op_block_index;
  const Schema& key_schema;
  const Schema& changelist_schema;
  const uint8_t* row_key_ptr;
  const RowChangeList* changelist;
};

Status BootstrapTablet(gscoped_ptr<metadata::TabletMetadata> meta,
                       const scoped_refptr<Clock>& clock,
                       MetricContext* metric_context,
                       TabletStatusListener* listener,
                       std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                       gscoped_ptr<log::Log>* rebuilt_log,
                       scoped_refptr<log::OpIdAnchorRegistry>* opid_anchor_registry) {
  TabletBootstrap bootstrap(meta.Pass(), clock, metric_context, listener);
  RETURN_NOT_OK(bootstrap.Bootstrap(rebuilt_tablet, rebuilt_log, opid_anchor_registry));
  // This is necessary since OpenNewLog() initially disables sync.
  RETURN_NOT_OK((*rebuilt_log)->ReEnableSyncIfRequired());
  return Status::OK();
}

MutationInput::MutationInput(WriteTransactionContext* tx_ctx_,
                             const consensus::OpId& committed_op_id_,
                             int mutation_op_block_index_,
                             const Schema& key_schema_,
                             const Schema& changelist_schema_,
                             const uint8_t* row_key_ptr_,
                             const RowChangeList* changelist_)
    : tx_ctx(tx_ctx_),
      committed_op_id(committed_op_id_),
      mutation_op_block_index(mutation_op_block_index_),
      key_schema(key_schema_),
      changelist_schema(changelist_schema_),
      row_key_ptr(row_key_ptr_),
      changelist(changelist_) {
}

static Status DecodeBlock(RowwiseRowBlockPB* block_pb,
                          bool is_inserts,
                          Schema* client_schema,
                          vector<const uint8_t*>* row_block) {

  RETURN_NOT_OK(ColumnPBsToSchema(block_pb->schema(), client_schema));
  DCHECK(!client_schema->has_column_ids());
  if (is_inserts) {
    RETURN_NOT_OK(ExtractRowsFromRowBlockPB(*client_schema, block_pb, row_block));
  } else {
    RETURN_NOT_OK(ExtractRowsFromRowBlockPB(client_schema->CreateKeyProjection(),
                                            block_pb,
                                            row_block));
  }
  return Status::OK();
}

static string DebugInfo(const string& tablet_id,
                        int segment_idx,
                        int entry_idx,
                        const string& segment_path,
                        const LogEntryPB& entry) {
  // Truncate the debug string to a reasonable length for logging.
  // Otherwise, glog will truncate for us and we may miss important
  // information which came after this long string.
  string debug_str = entry.ShortDebugString();
  if (debug_str.size() > 500) {
    debug_str.resize(500);
    debug_str.append("...");
  }
  return Substitute("Debug Info: Error playing entry $0 of segment $1 of tablet $2. "
                    "Segment path: $3. Entry: $4", entry_idx, segment_idx, tablet_id,
                    segment_path, debug_str);
}

TabletBootstrap::TabletBootstrap(gscoped_ptr<TabletMetadata> meta,
                                 const scoped_refptr<Clock>& clock,
                                 MetricContext* metric_context,
                                 TabletStatusListener* listener)
    : meta_(meta.Pass()),
      clock_(clock),
      metric_context_(metric_context),
      listener_(listener),
      arena_(256*1024, 4*1024*1024) {
}

Status TabletBootstrap::Bootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                                  gscoped_ptr<Log>* rebuilt_log,
                                  scoped_refptr<OpIdAnchorRegistry>* opid_anchor_registry) {

  string tablet_id = meta_->oid();
  meta_->PinFlush();

  listener_->StatusMessage("Bootstrap starting.");

  if (VLOG_IS_ON(1)) {
    shared_ptr<TabletSuperBlockPB> super_block;
    RETURN_NOT_OK(meta_->ToSuperBlock(&super_block));
    VLOG(1) << "Tablet Metadata: " << super_block->DebugString();
  }

  // Create new OpIdAnchorRegistry for use by the log and tablet.
  opid_anchor_registry_ = new OpIdAnchorRegistry();

  // TODO these are done serially for now, but there is no reason why fetching
  // the tablet's blocks and log segments cannot be done in parallel, particularly
  // in a dist. setting.
  bool fetched_blocks;
  RETURN_NOT_OK(FetchBlocksAndOpenTablet(&fetched_blocks));

  bool needs_recovery;
  RETURN_NOT_OK(FetchLogSegments(&needs_recovery));

  // This is a new tablet just return OK()
  if (!fetched_blocks && !needs_recovery) {
    LOG(INFO) << "No previous blocks or log segments found for tablet: " << tablet_id
        << " creating new one.";
    RETURN_NOT_OK_PREPEND(OpenNewLog(), "Failed to open new log");
    RETURN_NOT_OK(tablet_->metadata()->UnPinFlush());
    rebuilt_tablet->reset(tablet_.release());
    rebuilt_log->reset(log_.release());
    *opid_anchor_registry = opid_anchor_registry_;
    return Status::OK();
  }


  // If there were blocks there must be segments to replay
  // TODO this actually may not be a requirement if the tablet was Flush()ed
  // before shutdown *and* the Log was GC()'d but because we aren't doing Log
  // GC on shutdown there should be some segments available even if there is
  // no soft state to rebuild.
  if (fetched_blocks && !needs_recovery) {
    return Status::IllegalState(Substitute("Tablet: $0 had rowsets but no log "
                                           "segments could be found.",
                                           tablet_id));
  }

  RETURN_NOT_OK_PREPEND(PlaySegments(), "Failed log replay. Reason");
  RETURN_NOT_OK(tablet_->metadata()->UnPinFlush());
  RETURN_NOT_OK(RemoveRecoveryDir());
  listener_->StatusMessage("Bootstrap complete.");
  rebuilt_tablet->reset(tablet_.release());
  rebuilt_log->reset(log_.release());
  *opid_anchor_registry = opid_anchor_registry_;
  return Status::OK();
}

Status TabletBootstrap::FetchBlocksAndOpenTablet(bool* fetched) {
  gscoped_ptr<Tablet> tablet(new Tablet(meta_.Pass(),
                                        clock_,
                                        metric_context_,
                                        opid_anchor_registry_.get()));
  // doing nothing for now except opening a tablet locally.
  RETURN_NOT_OK(tablet->Open());
  // set 'fetched' to true if there were any local blocks present
  *fetched = tablet->num_rowsets() != 0;
  tablet_.reset(tablet.release());
  return Status::OK();
}

Status TabletBootstrap::FetchLogSegments(bool* needs_recovery) {
  RETURN_NOT_OK(PrepareRecoveryDir(needs_recovery));

  // TODO in a dist setting we want to get segments from other nodes
  // and do not require that local segments are present but for now
  // we do, i.e. a tablet having local blocks but no local log segments
  // signals lost state.
  if (!*needs_recovery) {
    return Status::OK();
  }

  VLOG(1) << "Existing Log segments found, opening log reader.";
  // Open the reader.
  RETURN_NOT_OK_PREPEND(LogReader::OpenFromRecoveryDir(tablet_->metadata()->fs_manager(),
                                                       tablet_->metadata()->oid(),
                                                       &log_reader_),
                        "Could not open LogReader. Reason");
  return Status::OK();
}

Status TabletBootstrap::PrepareRecoveryDir(bool* needs_recovery) {

  *needs_recovery = false;

  FsManager* fs_manager = tablet_->metadata()->fs_manager();
  string tablet_id = tablet_->metadata()->oid();
  string log_dir = fs_manager->GetTabletWalDir(tablet_id);

  if (!fs_manager->Exists(log_dir)) {
    RETURN_NOT_OK_PREPEND(fs_manager->CreateDirIfMissing(log_dir),
                          "Failed to create log dir");
    return Status::OK();
  }

  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_id);
  if (fs_manager->Exists(recovery_path)) {
    LOG(INFO) << "Replaying from previous recovery directory: " << recovery_path;
    vector<string> children;
    RETURN_NOT_OK_PREPEND(fs_manager->ListDir(log_dir, &children),
                          "Couldn't list log segments.");
    BOOST_FOREACH(const string& child, children) {
      if (!log::IsLogFileName(child)) {
        continue;
      }
      string path = JoinPathSegments(log_dir, child);
      LOG(INFO) << "Removing old log file from previous aborted recovery attempt: " << path;
      RETURN_NOT_OK(fs_manager->env()->DeleteFile(path));
    }
    *needs_recovery = true;
    return Status::OK();
  }

  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager->ListDir(log_dir, &children),
                        "Couldn't list log segments.");

  BOOST_FOREACH(const string& child, children) {
    if (!log::IsLogFileName(child)) {
      continue;
    }

    string source_path = JoinPathSegments(log_dir, child);
    string dest_path = JoinPathSegments(recovery_path, child);
    LOG(INFO) << "Will attempt to recover log segment: " << source_path << " to: " << dest_path;
    *needs_recovery = true;
  }

  if (*needs_recovery) {
    // Atomically rename the log directory to the recovery directory
    // and then re-create the log directory.
    RETURN_NOT_OK_PREPEND(fs_manager->env()->RenameFile(log_dir, recovery_path),
                          Substitute("Could not move log directory $0 to recovery dir $1",
                                     log_dir, recovery_path));
    LOG(INFO) << "Moved log directory: " << log_dir << " to recovery directory: " << recovery_path;
    RETURN_NOT_OK_PREPEND(fs_manager->CreateDirIfMissing(log_dir),
                          "Failed to recreate log directory " + log_dir);
  }
  return Status::OK();
}

Status TabletBootstrap::RemoveRecoveryDir() {
  FsManager* fs_manager = tablet_->metadata()->fs_manager();
  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_->metadata()->oid());

  DCHECK(fs_manager->Exists(recovery_path))
      << "Tablet WAL recovery dir " << recovery_path << " does not exist.";

  string tmp_path = Substitute("$0-$1", recovery_path, GetCurrentTimeMicros());
  RETURN_NOT_OK_PREPEND(fs_manager->env()->RenameFile(recovery_path, tmp_path),
                        Substitute("Could not rename old recovery dir from: $0 to: $1",
                                   recovery_path, tmp_path));
  LOG(INFO) << "Renamed old recovery dir from: "  << recovery_path << " to: " << tmp_path;
  RETURN_NOT_OK_PREPEND(fs_manager->env()->DeleteRecursively(tmp_path),
                        "Could not remove renamed recovery dir" +  tmp_path);
  LOG(INFO) << "Removed renamed recovery dir: " << tmp_path;
  return Status::OK();
}

Status TabletBootstrap::OpenNewLog() {
  OpId init;
  init.set_term(0);
  init.set_index(0);
  shared_ptr<TabletSuperBlockPB> super_block;
  RETURN_NOT_OK(tablet_->metadata()->ToSuperBlock(&super_block));
  RETURN_NOT_OK(Log::Open(LogOptions(),
                          tablet_->metadata()->fs_manager(),
                          tablet_->tablet_id(),
                          opid_anchor_registry_.get(),
                          metric_context_,
                          &log_));
  // Disable sync temporarily in order to speed up appends during the
  // bootstrap process.
  log_->DisableSync();
  return Status::OK();
}

typedef unordered_map<OpId, LogEntryPB*, OpIdHashFunctor, OpIdEqualsFunctor> OpToEntryMap;

// State kept during replay.
struct ReplayState {
  ReplayState() {
    prev_op_id.set_term(0);
    prev_op_id.set_index(0);
  }

  ~ReplayState() {
    STLDeleteValues(&pending_replicates);
  }

  // Return true if 'b' is allowed to immediately follow 'a' in the log.
  bool valid_sequence(const OpId& a, const OpId& b) {
    if (a.term() == 0 && a.index() == 0) {
      // Not initialized - can start with any opid.
      return true;
    }

    // Within the same term, each entry should be have an index
    // exactly one higher than the previous.
    if (b.term() == a.term() &&
        b.index() != a.index() + 1) {
      return false;
    }

    // If the the terms don't match, then the new term should be higher
    if (b.term() < a.term()) {
      return false;
    }
    return true;
  }

  // Return a Corruption status if 'id' seems to be out-of-sequence in the log.
  Status CheckSequentialOpId(const OpId& id) {
    if (!valid_sequence(prev_op_id, id)) {
      return Status::Corruption(
        Substitute("Unexpected opid $0 following opid $1",
                   id.ShortDebugString(),
                   prev_op_id.ShortDebugString()));
    }

    prev_op_id.CopyFrom(id);
    return Status::OK();
  }

  OpId prev_op_id;

  // REPLICATE log entries whose corresponding COMMIT/ABORT record has
  // not yet been seen. Keyed by opid.
  OpToEntryMap pending_replicates;
};

// Handle the given log entry. If OK is returned, then takes ownership of 'entry'.
// Otherwise, caller frees.
Status TabletBootstrap::HandleEntry(ReplayState* state, LogEntryPB* entry) {
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Handling entry: " << entry->ShortDebugString();
  }

  switch (entry->type()) {
    case OPERATION:
      if (entry->operation().has_replicate()) {
        RETURN_NOT_OK(HandleReplicateMessage(state, entry));
      } else if (entry->operation().has_commit()) {
        // check the unpaired ops for the matching replicate msg, abort if not found
        RETURN_NOT_OK(HandleCommitMessage(state, entry));
      }
      break;
    // TODO support other op types when we run distributedly
    default:
      return Status::Corruption(Substitute("Unexpected log entry type: $0", entry->type()));
  }
  return Status::OK();
}

Status TabletBootstrap::HandleReplicateMessage(ReplayState* state, LogEntryPB* entry) {
  RETURN_NOT_OK(state->CheckSequentialOpId(entry->operation().id()));

  // Append the replicate message to the log as is
  RETURN_NOT_OK(log_->Append(entry));

  LogEntryPB** existing_entry_ptr = InsertOrReturnExisting(
    &state->pending_replicates, entry->operation().id(), entry);
  if (existing_entry_ptr) {
    LogEntryPB* existing_entry = *existing_entry_ptr;
    // We already had an entry with the same ID.
    return Status::Corruption("Found previous entry with the same id",
                              existing_entry->ShortDebugString());
  }
  return Status::OK();
}

// Deletes 'entry' only on OK status.
Status TabletBootstrap::HandleCommitMessage(ReplayState* state, LogEntryPB* entry) {
  DCHECK(entry->operation().has_commit()) << "Not a commit message: " << entry->DebugString();

  // TODO: on a term switch, the first commit in any term should discard any
  // pending REPLICATEs from the previous term.

  // All log entries should have an OpId.
  DCHECK(entry->operation().has_id()) << "Entry has no OpId: " << entry->DebugString();

  // Match up the COMMIT/ABORT record with the original entry that it's applied to.
  const OpId& committed_op_id = entry->operation().commit().commited_op_id();

  gscoped_ptr<LogEntryPB> existing_entry;
  // Consensus commits must be sequentially increasing.
  RETURN_NOT_OK(state->CheckSequentialOpId(entry->operation().id()));
  // They should also have an associated replicate OpId (it may have been in a
  // deleted log segment though).
  existing_entry.reset(EraseKeyReturnValuePtr(&state->pending_replicates, committed_op_id));

  if (existing_entry != NULL) {
    // We found a match.
    RETURN_NOT_OK(HandleEntryPair(existing_entry.get(), entry));
  } else {
    // FIXME: This is likely incorrect, we could have an orphaned commit on log roll.
    // Needs test. See KUDU-141.

    return Status::Corruption(Substitute("Found orphaned commit: $0",
                                         entry->operation().commit().DebugString()));
  }

  delete entry;
  return Status::OK();
}

// Never deletes 'replicate_entry'.
// Deletes 'commit_entry' only on OK status.
Status TabletBootstrap::HandleEntryPair(LogEntryPB* replicate_entry, LogEntryPB* commit_entry) {

  ReplicateMsg* replicate = replicate_entry->mutable_operation()->mutable_replicate();
  const CommitMsg& commit = commit_entry->operation().commit();

  switch (commit.op_type()) {
    case OP_ABORT:
      // aborted write, log and continue
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Skipping replicate message because it was originally aborted."
                << " OpId: " << commit.commited_op_id().DebugString();
      }
      // return here so we don't update the clock as OP_ABORT's have invalid timestamps.
      return Status::OK();

    case WRITE_OP:
      // successful write, play it into the tablet, filtering flushed entries
      RETURN_NOT_OK_PREPEND(PlayWriteRequest(replicate_entry->mutable_operation(),
                                             commit_entry->operation()),
                            Substitute("Failed to play write request. "
                                       "ReplicateMsg: $0 CommitMsg: $1\n",
                                       replicate->DebugString(),
                                       commit.DebugString()));
      break;

    case ALTER_SCHEMA_OP:
      RETURN_NOT_OK_PREPEND(PlayAlterSchemaRequest(replicate_entry->mutable_operation(),
                                                   commit_entry->operation()),
                            Substitute("Failed to play alter schema request. "
                                "ReplicateMsg: $0 CommitMsg: $1\n",
                                replicate->DebugString(),
                                commit.DebugString()));
      break;

    case CHANGE_CONFIG_OP:
      RETURN_NOT_OK_PREPEND(PlayChangeConfigRequest(replicate_entry->mutable_operation(),
                                                    commit_entry->operation()),
                            Substitute("Failed to play change config. request. "
                                       "ReplicateMsg: $0 CommitMsg: $1\n",
                                       replicate->DebugString(),
                                       commit.DebugString()));
      break;

    default:
      return Status::IllegalState(Substitute("Unsupported commit entry type: $0",
                                             commit.op_type()));
  }

  // update the clock with the commit timestamp
  RETURN_NOT_OK(UpdateClock(commit.timestamp()));

  return Status::OK();
}

Status TabletBootstrap::PlaySegments() {
  RETURN_NOT_OK_PREPEND(OpenNewLog(), "Failed to open new log");

  ReplayState state;
  for (int segment_idx = 0; segment_idx < log_reader_->size(); ++segment_idx) {
    vector<LogEntryPB*> entries;
    ElementDeleter deleter(&entries);
    // TODO: Optimize this to not read the whole thing into memory?
    Status read_status = log_reader_->ReadEntries(log_reader_->segments()[segment_idx], &entries);
    for (int entry_idx = 0; entry_idx < entries.size(); ++entry_idx) {
      LogEntryPB* entry = entries[entry_idx];
      RETURN_NOT_OK_PREPEND(HandleEntry(&state, entry),
                            DebugInfo(tablet_->tablet_id(), segment_idx,
                                      entry_idx, log_reader_->segments()[segment_idx]->path(),
                                      *entry));

      // If HandleEntry returns OK, then it has taken ownership of the entry.
      // So, we have to remove it from the entries vector to avoid it getting
      // freed by ElementDeleter.
      entries[entry_idx] = NULL;
    }

    // If the LogReader failed to read for some reason, we'll still try to
    // replay as many entries as possible, and then fail with Corruption.
    // TODO: this is sort of scary -- why doesn't LogReader expose an
    // entry-by-entry iterator-like API instead? Seems better to avoid
    // exposing the idea of segments to callers.
    if (PREDICT_FALSE(!read_status.ok())) {
      return Status::Corruption(Substitute("Error reading Log Segment of tablet: $0. "
                                           "Read up to entry: $1 of segment: $2, in path: $3.",
                                           tablet_->tablet_id(),
                                           entries.size(),
                                           segment_idx,
                                           log_reader_->segments()[segment_idx]->path()));
    }

    // TODO: could be more granular here and log during the segments as well,
    // plus give info about number of MB processed, but this is better than
    // nothing.
    listener_->StatusMessage(Substitute("Bootstrap replayed $0/$1 log segments.",
                                        segment_idx + 1, log_reader_->size()));
  }

  int num_orphaned = state.pending_replicates.size();
  if (num_orphaned > 0) {
    LOG(INFO) << "WAL for " << tablet_->tablet_id() << " included " << num_orphaned
              << " REPLICATE messages with no corresponding commit/abort messages."
              << " These transactions were probably in-flight when the server crashed.";
    BOOST_FOREACH(const OpToEntryMap::value_type& e, state.pending_replicates) {
      LOG(INFO) << "  " << e.second->ShortDebugString();
    }
  }

  return Status::OK();
}

Status TabletBootstrap::PlayWriteRequest(OperationPB* replicate_op,
                                         const OperationPB& commit_op) {
  WriteRequestPB* write = replicate_op->mutable_replicate()->mutable_write_request();

  // TODO should we re-append to the new log when all operations were
  // skipped? On one hand appending allows this node to catch up other
  // nodes even its log entries go back further than its current
  // flushed state. On the other hand it just seems wasteful...

  WriteTransactionContext tx_ctx;
  gscoped_ptr<ScopedTransaction> mvcc_tx(new ScopedTransaction(tablet_->mvcc_manager()));
  tx_ctx.set_current_mvcc_tx(mvcc_tx.Pass());

  int32_t last_insert_op_idx = 0;
  if (write->has_to_insert_rows()) {
    RETURN_NOT_OK(PlayInsertions(&tx_ctx,
                                 write->schema(),
                                 write->to_insert_rows(),
                                 commit_op.commit().result(),
                                 replicate_op->id(),
                                 &last_insert_op_idx));
  }
  if (write->has_to_mutate_row_keys()) {
    RETURN_NOT_OK(PlayMutations(&tx_ctx,
                                write->mutable_to_mutate_row_keys(),
                                write->encoded_mutations(),
                                commit_op.commit().result(),
                                replicate_op->id(),
                                last_insert_op_idx));
  }

  // Append the commit msg to the log but replace the result with the new one
  LogEntryPB commit_entry;
  commit_entry.set_type(OPERATION);
  OperationPB* new_commit_op = commit_entry.mutable_operation();
  new_commit_op->mutable_id()->CopyFrom(commit_op.id());
  CommitMsg* commit = new_commit_op->mutable_commit();
  commit->CopyFrom(commit_op.commit());
  commit->mutable_result()->CopyFrom(tx_ctx.Result());
  RETURN_NOT_OK(log_->Append(&commit_entry));


  return Status::OK();
}

Status TabletBootstrap::PlayAlterSchemaRequest(OperationPB* replicate_op,
                                               const OperationPB& commit_op) {
  AlterSchemaRequestPB* alter_schema =
      replicate_op->mutable_replicate()->mutable_alter_schema_request();

  // Decode schema
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(alter_schema->schema(), &schema));

  AlterSchemaTransactionContext tx_ctx(alter_schema);

  // TODO maybe we shouldn't acquire the tablet lock on replay?
  RETURN_NOT_OK(tablet_->CreatePreparedAlterSchema(&tx_ctx, &schema));

  // apply the alter schema to the tablet
  RETURN_NOT_OK_PREPEND(tablet_->AlterSchema(&tx_ctx), "Failed to AlterSchema:");

  LogEntryPB commit_entry;
  commit_entry.set_type(OPERATION);
  OperationPB* new_commit_op = commit_entry.mutable_operation();
  new_commit_op->mutable_id()->CopyFrom(commit_op.id());
  CommitMsg* commit = new_commit_op->mutable_commit();
  commit->CopyFrom(commit_op.commit());
  RETURN_NOT_OK(log_->Append(&commit_entry));

  return Status::OK();
}

Status TabletBootstrap::PlayChangeConfigRequest(OperationPB* replicate_op,
                                                const OperationPB& commit_op) {
  ChangeConfigRequestPB* change_config =
      replicate_op->mutable_replicate()->mutable_change_config_request();

  ChangeConfigTransactionContext tx_ctx(change_config);

  QuorumPB quorum = change_config->new_config();

  // if the sequence number is higher than the current one change the configuration
  // otherwise skip it.
  if (quorum.seqno() > tablet_->metadata()->Quorum().seqno()) {
    tablet_->metadata()->SetQuorum(quorum);
  } else {
    VLOG(1) << "Configuration sequence number lower than current sequence number. "
        "Skipping config change";
  }

  LogEntryPB commit_entry;
  commit_entry.set_type(OPERATION);
  OperationPB* new_commit_op = commit_entry.mutable_operation();
  new_commit_op->mutable_id()->CopyFrom(commit_op.id());
  CommitMsg* commit = new_commit_op->mutable_commit();
  commit->CopyFrom(commit_op.commit());
  RETURN_NOT_OK(log_->Append(&commit_entry));

  return Status::OK();
}

Status TabletBootstrap::PlayInsertions(WriteTransactionContext* tx_ctx,
                                       const SchemaPB& schema_pb,
                                       const RowOperationsPB& rows,
                                       const TxResultPB& result,
                                       const OpId& committed_op_id,
                                       int32_t* last_insert_op_idx) {

  Schema inserts_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(schema_pb, &inserts_schema),
                        "Couldn't decode client schema");


  vector<uint8_t*> row_block;

  // TODO: this makes a needless copy here, even though we know that we won't
  // have concurrent schema change. However, we can't use schema_ptr since we don't
  // hold component_lock yet here.
  Schema tablet_schema(tablet_->schema());

  arena_.Reset();
  RETURN_NOT_OK_PREPEND(PartialRow::DecodeAndProject(
                          rows, inserts_schema, tablet_schema, &row_block, &arena_),
                        Substitute("Could not decode block: $0", rows.ShortDebugString()));

  int32_t insert_idx = 0;
  BOOST_FOREACH(const uint8_t* row_ptr, row_block) {
    TxOperationPB op_result = result.inserts(insert_idx++);
    // check if the insert failed in the original transaction
    if (PREDICT_FALSE(op_result.has_failed_status())) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Skipping insert that resulted in error. OpId: "
            << committed_op_id.DebugString() << " insert index: "
            << insert_idx - 1 << " original error: "
            << op_result.failed_status().DebugString();
      }
      tx_ctx->AddFailedInsert(Status::RuntimeError("Row insert failed previously."));
      continue;
    }
    if (PREDICT_FALSE(!op_result.has_mrs_id())) {
      return Status::Corruption(Substitute("Insert operation result must have an mrs_id: $0",
                                           op_result.ShortDebugString()));
    }
    // check if the insert is already flushed
    if (op_result.mrs_id() <= tablet_->metadata()->last_durable_mrs_id()) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Skipping insert that was already flushed. OpId: "
            << committed_op_id.DebugString() << " insert index: "
            << insert_idx - 1 << " flushed to: " << op_result.mrs_id()
            << " latest durable mrs id: " << tablet_->metadata()->last_durable_mrs_id();
      }
      tx_ctx->AddFailedInsert(Status::AlreadyPresent("Row to insert was already flushed."));
      continue;
    }
    // Note: Using InsertUnlocked as the old API will eventually disappear

    gscoped_ptr<shared_lock<rw_semaphore> > component_lock(
      new shared_lock<rw_semaphore>(*tablet_->component_lock()));
    tx_ctx->set_component_lock(component_lock.Pass());

    const ConstContiguousRow* row = tx_ctx->AddToAutoReleasePool(
      new ConstContiguousRow(*tablet_->schema_ptr(), row_ptr));

    gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row));
    gscoped_ptr<PreparedRowWrite> prepared_row;
    // TODO maybe we shouldn't acquire the row lock on replay?
    RETURN_NOT_OK(tablet_->CreatePreparedInsert(tx_ctx, row, &prepared_row));

    // Use committed OpId for mem store anchoring.
    tx_ctx->mutable_op_id()->CopyFrom(committed_op_id);

    // apply the insert to the tablet
    RETURN_NOT_OK_PREPEND(tablet_->InsertUnlocked(tx_ctx, prepared_row.get()),
                          Substitute("Failed to insert row $0. Reason",
                                     inserts_schema.DebugRow(*row)));

    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Applied Insert. OpId: "
          << committed_op_id.DebugString() << " insert index: "
          << insert_idx - 1 << " row: " << inserts_schema.DebugRow(*row);
    }
  }
  *last_insert_op_idx = insert_idx;
  return Status::OK();
}

Status TabletBootstrap::PlayMutations(WriteTransactionContext* tx_ctx,
                                      RowwiseRowBlockPB* row_keys,
                                      const string& encoded_mutations,
                                      const TxResultPB& result,
                                      const OpId& committed_op_id,
                                      int32_t first_mutate_op_idx) {
  Schema mutates_schema;
  vector<const uint8_t*> row_key_block;
  RETURN_NOT_OK(DecodeBlock(row_keys,
                            false,
                            &mutates_schema,
                            &row_key_block));

  // TODO(perf): we're copying the schema here for every update which is
  // overkill. But schema_ptr() will throw an assertion since we don't
  // hold the component lock here. Perhaps we should take the component lock
  // even though we don't strictly need it for concurrency control.
  Schema tablet_schema = tablet_->schema();
  DeltaProjector delta_projector(&mutates_schema, &tablet_schema);
  if (!delta_projector.is_identity()) {
    RETURN_NOT_OK(tablet_->schema().VerifyProjectionCompatibility(mutates_schema));
    RETURN_NOT_OK(mutates_schema.GetProjectionMapping(tablet_->schema(), &delta_projector));
  }

  Schema mutates_key_schema = mutates_schema.CreateKeyProjection();
  vector<const RowChangeList *> mutations;
  ElementDeleter deleter(&mutations);
  RETURN_NOT_OK(ExtractMutationsFromBuffer(row_key_block.size(),
                                 reinterpret_cast<const uint8_t*>(
                                     encoded_mutations.data()),
                                     encoded_mutations.size(),
                                     &mutations));

  uint32_t mutate_op_idx = first_mutate_op_idx;
  BOOST_FOREACH(const uint8_t* row_key_ptr, row_key_block) {
    const TxOperationPB& op_result = result.mutations(mutate_op_idx++);
    // check if the mutation failed in the original transaction
    if (PREDICT_FALSE(op_result.has_failed_status())) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Skipping mutation that resulted in error. OpId: "
            << committed_op_id.DebugString() << " mutation index: "
            << mutate_op_idx - 1 << " original error: "
            << op_result.failed_status().DebugString();
      }
      tx_ctx->AddFailedMutation(Status::RuntimeError("Row mutate failed previously."));
      continue;
    }
    const RowChangeList* mutation = ProjectMutation(tx_ctx, delta_projector,
                                                    mutations[mutate_op_idx -1]);
    MutationInput mutation_input(tx_ctx,
                                 committed_op_id,
                                 mutate_op_idx - 1,
                                 mutates_key_schema,
                                 mutates_schema,
                                 row_key_ptr,
                                 mutation);
    bool applied_mutation = false;
    RETURN_NOT_OK(PlayMutation(mutation_input, op_result, &applied_mutation));
    // FIXME: Not checking value of applied_mutation result. See KUDU-159.
  }
  return Status::OK();
}

Status TabletBootstrap::PlayMutation(const MutationInput& mutation_input,
                                     const TxOperationPB& op_result,
                                     bool* applied_mutation) {

  switch (op_result.mutation_result().type()) {
    // With MRS_MUTATIONs we do much like with inserts, i.e. we check if the
    // MRS has not been flushed and apply it otherwise.
    case MutationResultPB::MRS_MUTATION:
    {
      if (PREDICT_FALSE(op_result.mutation_result().mutations_size() != 1)) {
        return Status::Corruption(Substitute("MRS Mutations must only have one mutation: $0",
                                             op_result.ShortDebugString()));
      }
      const MutationTargetPB& mutation_target = op_result.mutation_result().mutations(0);
      RETURN_NOT_OK(HandleMRSMutation(mutation_input, mutation_target, applied_mutation));
      return Status::OK();
    }
    // With DELTA_MUTATIONs we check if the delta of the rs in question has
    // been flushed and apply it otherwise.
    case MutationResultPB::DELTA_MUTATION:
    {
      if (PREDICT_FALSE(op_result.mutation_result().mutations_size() != 1)) {
        return Status::Corruption(Substitute("DMS Mutations must only have one mutation: $0",
                                             op_result.ShortDebugString()));
      }
      const MutationTargetPB& mutation_target = op_result.mutation_result().mutations(0);
      RETURN_NOT_OK(HandleDMSMutation(mutation_input, mutation_target, applied_mutation));
      return Status::OK();
    }
    // Duplicated mutations happen mid compaction, and either one has been
    // flushed or none have been flushed.
    case MutationResultPB::DUPLICATED_MUTATION:
    {
      if (PREDICT_FALSE(op_result.mutation_result().mutations_size() != 2)) {
        return Status::Corruption(Substitute("Duplicated Mutations must have two mutations: $0",
                                             op_result.ShortDebugString()));
      }

      // The first mutation in a duplicated mutation might not have been flushed when
      // a node fails mid compaction/flush, after swapping in the duplicating rowsets
      // but before flushing the new metadata. If the first mutation was applied
      // we skip the second.
      const MutationTargetPB& first_mutation_target = op_result.mutation_result().mutations(0);
      if (first_mutation_target.has_mrs_id()) {
        RETURN_NOT_OK(HandleMRSMutation(mutation_input, first_mutation_target, applied_mutation));
      } else {
        RETURN_NOT_OK(HandleDMSMutation(mutation_input, first_mutation_target, applied_mutation));
      }

      if (*applied_mutation) return Status::OK();

      // If the first mutation was not applied we remove the result as duplicated
      // mutations turn into regular mutations on replay.
      // We need to const cast the result to remove the last mutation result.
      const_cast<TxResultPB&>(mutation_input.tx_ctx->Result()).mutable_mutations()->RemoveLast();

      // If the first duplicated mutation was not applied we try to apply the second
      const MutationTargetPB& second_mutation_target = op_result.mutation_result().mutations(1);
      if (second_mutation_target.has_mrs_id()) {
        RETURN_NOT_OK(HandleMRSMutation(mutation_input, second_mutation_target, applied_mutation));
      } else {
        RETURN_NOT_OK(HandleDMSMutation(mutation_input, second_mutation_target, applied_mutation));
      }
      return Status::OK();
    }
    default:
      return Status::IllegalState(Substitute("Unsupported mutation type: $0",
                                             op_result.ShortDebugString()));
  }
  LOG(DFATAL);
  return Status::IllegalState("");
}

Status TabletBootstrap::HandleMRSMutation(const MutationInput& mutation_input,
                                          const MutationTargetPB& mutation_target,
                                          bool* applied_mutation) {
  if (mutation_target.mrs_id() <= tablet_->metadata()->last_durable_mrs_id()) {
    string mutation = mutation_input.changelist->ToString(mutation_input.changelist_schema);
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Skipping MRS_MUTATION that was already flushed. OpId: "
          << mutation_input.committed_op_id.DebugString()
          << " insert index: " << mutation_input.mutation_op_block_index
          << " flushed to: " << mutation_target.mrs_id()
          << " latest durable mrs id: " << mutation_target.mrs_id()
          << " mutation: " << mutation;
    }
    mutation_input.tx_ctx->AddFailedMutation(Status::AlreadyPresent(
                                               Substitute("MRS mutation "
                                                          "flushed: $0", mutation)));
    *applied_mutation = false;
    return Status::OK();
  }
  RETURN_NOT_OK(ApplyMutation(mutation_input));
  *applied_mutation = true;
  return Status::OK();
}

Status TabletBootstrap::HandleDMSMutation(const MutationInput& mutation_input,
                                          const MutationTargetPB& mutation_target,
                                          bool* applied_mutation) {
  // TODO right now this is using GetRowSetForTests which goes through
  // the rs's every time. Just adding a method that gets row sets by id
  // is not enough. We really need to take a snapshot of the initial
  // metadata with regard to which row sets are alive at the time. By
  // doing this we decouple replaying from the current state of the tablet,
  // which allows us to do compactions/flushes on replay.
  const RowSetMetadata* row_set = tablet_->metadata()->GetRowSetForTests(
      mutation_target.rs_id());

  // if we can't find the row_set it was compacted
  if (row_set == NULL) {
    string mutation = mutation_input.changelist->ToString(mutation_input.changelist_schema);
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Skipping DELTA_MUTATION that was already compacted. OpId: "
          << mutation_input.committed_op_id.DebugString()
          << " mutation index: " << mutation_input.mutation_op_block_index
          << " flushed to: " << mutation_target.rs_id()
          << " mutation: " <<mutation;
    }
    mutation_input.tx_ctx->AddFailedMutation(
      Status::AlreadyPresent(Substitute("DMS mutation flushed and compacted: $0",
                                        mutation)));
    *applied_mutation = false;
    return Status::OK();
  }

  // if it exists we check if the mutation is already flushed
  if (mutation_target.delta_id() <= row_set->last_durable_redo_dms_id()) {
    string mutation = mutation_input.changelist->ToString(mutation_input.changelist_schema);
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Skipping DELTA_MUTATION that was already flushed. OpId: "
          << mutation_input.committed_op_id.DebugString()
          << " mutation index: " << mutation_input.mutation_op_block_index
          << " flushed to: " << mutation_target.rs_id()
          << " latest durable dms id: " << row_set->last_durable_redo_dms_id()
          << " mutation: " << mutation;
    }
    mutation_input.tx_ctx->AddFailedMutation(Status::AlreadyPresent(
                                               Substitute("DMS mutation "
                                                          "flushed: $0", mutation)));
    *applied_mutation = false;
    return Status::OK();
  }
  RETURN_NOT_OK(ApplyMutation(mutation_input));
  *applied_mutation = true;
  return Status::OK();
}

Status TabletBootstrap::ApplyMutation(const MutationInput& mutation_input) {
  gscoped_ptr<ConstContiguousRow> row_key(new ConstContiguousRow(mutation_input.key_schema,
                                                                 mutation_input.row_key_ptr));
  gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row_key));
  gscoped_ptr<PreparedRowWrite> prepared_row;
  // TODO maybe we shouldn't acquire the row lock on replay?
  RETURN_NOT_OK(tablet_->CreatePreparedMutate(mutation_input.tx_ctx, row_key.get(),
                                              mutation_input.changelist,
                                              &prepared_row));

  // Use committed OpId for mem store anchoring.
  mutation_input.tx_ctx->mutable_op_id()->CopyFrom(mutation_input.committed_op_id);

  RETURN_NOT_OK(tablet_->MutateRowUnlocked(mutation_input.tx_ctx, prepared_row.get()));

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Applied Mutation. OpId: " << mutation_input.committed_op_id.DebugString()
          << " mutation index: " << mutation_input.mutation_op_block_index
          << " row key: " << mutation_input.key_schema.DebugRow(*row_key)
          << " mutation: " << mutation_input.changelist->ToString(mutation_input.changelist_schema);
  }
  return Status::OK();
}

Status TabletBootstrap::UpdateClock(const string& timestamp) {
  Timestamp ts;
  RETURN_NOT_OK(ts.DecodeFromString(timestamp));
  RETURN_NOT_OK(clock_->Update(ts));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
