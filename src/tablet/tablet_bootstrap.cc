// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_bootstrap.h"

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <string>
#include <utility>
#include <vector>

#include "common/partial_row.h"
#include "common/row_changelist.h"
#include "common/row_operations.h"
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

DEFINE_bool(skip_remove_old_recovery_dir, false,
            "Skip removing WAL recovery dir after startup. (useful for debugging)");

namespace kudu {
namespace tablet {

using boost::shared_lock;
using consensus::CommitMsg;
using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusRound;
using consensus::OperationPB;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::ALTER_SCHEMA_OP;
using consensus::CHANGE_CONFIG_OP;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using log::Log;
using log::LogEntryPB;
using log::LogOptions;
using log::LogReader;
using log::OpIdAnchorRegistry;
using log::OpIdEquals;
using log::OpIdEqualsFunctor;
using log::OpIdHashFunctor;
using log::OPERATION;
using log::ReadableLogSegment;
using log::ReadableLogSegmentMap;
using metadata::QuorumPB;
using metadata::TabletMetadata;
using metadata::TabletSuperBlockPB;
using metadata::RowSetMetadata;
using server::Clock;
using tablet::OperationResultPB;
using tablet::PreparedRowWrite;
using tablet::Tablet;
using tserver::AlterSchemaRequestPB;
using tserver::ChangeConfigRequestPB;
using tserver::WriteRequestPB;
using std::tr1::shared_ptr;
using strings::Substitute;

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
  TabletBootstrap(const scoped_refptr<metadata::TabletMetadata>& meta,
                  const scoped_refptr<Clock>& clock,
                  MetricContext* metric_context,
                  TabletStatusListener* listener);

  // Plays the log segments, rebuilding the portion of the Tablet's soft
  // state that is present in the log (additional soft state may be present
  // in other replicas).
  // A successful call will yield the rebuilt tablet and the rebuilt log.
  Status Bootstrap(std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                   gscoped_ptr<log::Log>* rebuilt_log,
                   scoped_refptr<log::OpIdAnchorRegistry>* opid_anchor_registry,
                   ConsensusBootstrapInfo* results);

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
  Status PlaySegments(ConsensusBootstrapInfo* results);

  Status PlayWriteRequest(OperationPB* replicate_op,
                          const OperationPB& commit_op);

  Status PlayAlterSchemaRequest(OperationPB* replicate_op,
                                const OperationPB& commit_op);

  Status PlayChangeConfigRequest(OperationPB* replicate_op,
                                 const OperationPB& commit_op);

  // Plays operations, skipping those that have already been flushed.
  Status PlayRowOperations(WriteTransactionState* tx_state,
                           const SchemaPB& schema_pb,
                           const RowOperationsPB& ops_pb,
                           const TxResultPB& result);

  // Play a single insert operation.
  Status PlayInsert(WriteTransactionState* tx_state,
                    const DecodedRowOperation& op,
                    const OperationResultPB& op_result);

  // Plays a single mutation.
  Status PlayMutation(WriteTransactionState* tx_state,
                      const DecodedRowOperation& op,
                      const OperationResultPB& op_result);

  bool WasStoreAlreadyFlushed(const MemStoreTargetPB& target);

  // Applies a mutation
  Status ApplyMutation(WriteTransactionState* tx_state,
                       const DecodedRowOperation& op);

  // Handlers for each type of message seen in the log during replay.
  Status HandleEntry(ReplayState* state, LogEntryPB* entry);
  Status HandleReplicateMessage(ReplayState* state, LogEntryPB* entry);
  Status HandleCommitMessage(ReplayState* state, LogEntryPB* entry);
  Status HandleEntryPair(LogEntryPB* replicate_entry, LogEntryPB* commit_entry);

  void DumpOrphanedReplicates(const vector<OperationPB*>& ops);

  // Decodes a Timestamp from the provided string and updates the clock
  // with it.
  Status UpdateClock(uint64_t timestamp);

  // Removes the recovery directory.
  Status RemoveRecoveryDir();

  scoped_refptr<metadata::TabletMetadata> meta_;
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

TabletStatusListener::TabletStatusListener(const scoped_refptr<metadata::TabletMetadata>& meta)
    : meta_(meta),
      last_status_("") {
}

const string TabletStatusListener::tablet_id() const {
  return meta_->oid();
}

const string TabletStatusListener::table_name() const {
  return meta_->table_name();
}

const string TabletStatusListener::start_key() const {
  return meta_->start_key();
}

const string TabletStatusListener::end_key() const {
  return meta_->end_key();
}

const Schema TabletStatusListener::schema() const {
  return meta_->schema();
}

TabletStatusListener::~TabletStatusListener() {
}

void TabletStatusListener::StatusMessage(const string& status) {
  LOG(INFO) << "Tablet id " << tablet_id() << ": " << status;
  boost::lock_guard<boost::shared_mutex> l(lock_);
  last_status_ = status;
}

Status BootstrapTablet(const scoped_refptr<metadata::TabletMetadata>& meta,
                       const scoped_refptr<Clock>& clock,
                       MetricContext* metric_context,
                       TabletStatusListener* listener,
                       std::tr1::shared_ptr<tablet::Tablet>* rebuilt_tablet,
                       gscoped_ptr<log::Log>* rebuilt_log,
                       scoped_refptr<log::OpIdAnchorRegistry>* opid_anchor_registry,
                       ConsensusBootstrapInfo* consensus_info) {
  TabletBootstrap bootstrap(meta, clock, metric_context, listener);
  RETURN_NOT_OK(bootstrap.Bootstrap(rebuilt_tablet, rebuilt_log,
                                    opid_anchor_registry,
                                    consensus_info));
  // This is necessary since OpenNewLog() initially disables sync.
  RETURN_NOT_OK((*rebuilt_log)->ReEnableSyncIfRequired());
  return Status::OK();
}

static string DebugInfo(const string& tablet_id,
                        int segment_seqno,
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
                    "Segment path: $3. Entry: $4", entry_idx, segment_seqno, tablet_id,
                    segment_path, debug_str);
}

TabletBootstrap::TabletBootstrap(const scoped_refptr<TabletMetadata>& meta,
                                 const scoped_refptr<Clock>& clock,
                                 MetricContext* metric_context,
                                 TabletStatusListener* listener)
    : meta_(meta),
      clock_(clock),
      metric_context_(metric_context),
      listener_(listener),
      arena_(256*1024, 4*1024*1024) {
}

Status TabletBootstrap::Bootstrap(shared_ptr<Tablet>* rebuilt_tablet,
                                  gscoped_ptr<Log>* rebuilt_log,
                                  scoped_refptr<OpIdAnchorRegistry>* opid_anchor_registry,
                                  ConsensusBootstrapInfo* consensus_info) {

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
    listener_->StatusMessage("No bootstrap required, opened a new log");
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

  RETURN_NOT_OK_PREPEND(PlaySegments(consensus_info), "Failed log replay. Reason");
  RETURN_NOT_OK(tablet_->metadata()->UnPinFlush());
  RETURN_NOT_OK(RemoveRecoveryDir());
  listener_->StatusMessage("Bootstrap complete.");
  rebuilt_tablet->reset(tablet_.release());
  rebuilt_log->reset(log_.release());
  *opid_anchor_registry = opid_anchor_registry_;

  return Status::OK();
}

Status TabletBootstrap::FetchBlocksAndOpenTablet(bool* fetched) {
  gscoped_ptr<Tablet> tablet(new Tablet(meta_,
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

  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_id);
  if (fs_manager->Exists(recovery_path)) {
    LOG(INFO) << "Replaying from previous recovery directory: " << recovery_path;
    if (fs_manager->Exists(log_dir)) {
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
    } else {
      RETURN_NOT_OK_PREPEND(fs_manager->CreateDirIfMissing(log_dir),
                            "Failed to create log dir");
    }
    *needs_recovery = true;
    return Status::OK();
  }

  if (!fs_manager->Exists(log_dir)) {
    RETURN_NOT_OK_PREPEND(fs_manager->CreateDirIfMissing(log_dir),
                          "Failed to create log dir");
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

  if (FLAGS_skip_remove_old_recovery_dir) {
    LOG(INFO) << "--skip_remove_old_recovery_dir enabled. NOT removing " << tmp_path;
    return Status::OK();
  }
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

  // The last message's id (regardless of type)
  OpId prev_op_id;

  // The last COMMIT message's id
  OpId last_commit_id;

  // The last REPLICATE message's id
  OpId last_replicate_id;


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

  state->last_replicate_id = entry->operation().id();

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

  state->last_commit_id = entry->operation().id();

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
    const CommitMsg& commit = entry->operation().commit();
    // TODO: move this to DEBUG once we have enough test cycles
    BOOST_FOREACH(const OperationResultPB& op_result, commit.result().ops()) {
      BOOST_FOREACH(const MemStoreTargetPB& mutated_store, op_result.mutated_stores()) {
        if (!WasStoreAlreadyFlushed(mutated_store)) {
          return Status::Corruption(
              Substitute("Orphan commit $0 has a mutated store $1 that was NOT already flushed",
                         commit.ShortDebugString(), mutated_store.ShortDebugString()));
        }
      }
    }
    VLOG(1) << "Ignoring orphan commit: " << commit.DebugString();
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

Status TabletBootstrap::PlaySegments(ConsensusBootstrapInfo* consensus_info) {
  RETURN_NOT_OK_PREPEND(OpenNewLog(), "Failed to open new log");

  ReplayState state;
  const ReadableLogSegmentMap& segments = log_reader_->segments();
  int segment_count = 0;
  BOOST_FOREACH(const ReadableLogSegmentMap::value_type& seg_entry, segments) {
    const scoped_refptr<ReadableLogSegment>& segment = seg_entry.second;

    vector<LogEntryPB*> entries;
    ElementDeleter deleter(&entries);
    // TODO: Optimize this to not read the whole thing into memory?
    Status read_status = segment->ReadEntries(&entries);
    for (int entry_idx = 0; entry_idx < entries.size(); ++entry_idx) {
      LogEntryPB* entry = entries[entry_idx];
      RETURN_NOT_OK_PREPEND(HandleEntry(&state, entry),
                            DebugInfo(tablet_->tablet_id(), segment->header().sequence_number(),
                                      entry_idx, segment->path(),
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
                                           segment->header().sequence_number(),
                                           segment->path()));
    }

    // TODO: could be more granular here and log during the segments as well,
    // plus give info about number of MB processed, but this is better than
    // nothing.
    listener_->StatusMessage(Substitute("Bootstrap replayed $0/$1 log segments.",
                                        segment_count + 1, log_reader_->size()));
    segment_count++;
  }

  // Set up the ConsensusBootstrapInfo structure for the caller.
  BOOST_FOREACH(OpToEntryMap::value_type& e, state.pending_replicates) {
    consensus_info->orphaned_replicates.push_back(e.second->release_operation());
  }
  consensus_info->last_commit_id = state.last_commit_id;
  consensus_info->last_replicate_id = state.last_replicate_id;
  consensus_info->last_id = state.prev_op_id;

  // Log any pending REPLICATEs, maybe useful for diagnosis.
  if (!consensus_info->orphaned_replicates.empty()) {
    DumpOrphanedReplicates(consensus_info->orphaned_replicates);
  }

  return Status::OK();
}

void TabletBootstrap::DumpOrphanedReplicates(const vector<OperationPB*>& ops) {
  LOG(INFO) << "WAL for " << tablet_->tablet_id() << " included " << ops.size()
            << " REPLICATE messages with no corresponding commit/abort messages."
            << " These transactions were probably in-flight when the server crashed.";
  BOOST_FOREACH(const OperationPB* op, ops) {
    LOG(INFO) << "  " << op->ShortDebugString();
  }

}

Status TabletBootstrap::PlayWriteRequest(OperationPB* replicate_op,
                                         const OperationPB& commit_op) {
  WriteRequestPB* write = replicate_op->mutable_replicate()->mutable_write_request();

  // TODO should we re-append to the new log when all operations were
  // skipped? On one hand appending allows this node to catch up other
  // nodes even its log entries go back further than its current
  // flushed state. On the other hand it just seems wasteful...

  WriteTransactionState tx_state;
  tx_state.mutable_op_id()->CopyFrom(replicate_op->id());

  gscoped_ptr<ScopedTransaction> mvcc_tx(new ScopedTransaction(tablet_->mvcc_manager()));
  tx_state.set_current_mvcc_tx(mvcc_tx.Pass());

  // Use committed OpId for mem store anchoring.
  tx_state.mutable_op_id()->CopyFrom(replicate_op->id());

  // Take the lock for the whole batch of updates.
  tx_state.set_component_lock(tablet_->component_lock());

  if (write->has_row_operations()) {
    // TODO: is above always true at this point in the code?
    RETURN_NOT_OK(PlayRowOperations(&tx_state,
                                    write->schema(),
                                    write->row_operations(),
                                    commit_op.commit().result()));
  }

  // Append the commit msg to the log but replace the result with the new one
  LogEntryPB commit_entry;
  commit_entry.set_type(OPERATION);
  OperationPB* new_commit_op = commit_entry.mutable_operation();
  new_commit_op->mutable_id()->CopyFrom(commit_op.id());
  CommitMsg* commit = new_commit_op->mutable_commit();
  commit->CopyFrom(commit_op.commit());
  commit->mutable_result()->CopyFrom(tx_state.Result());
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

  AlterSchemaTransactionState tx_state(alter_schema);

  // TODO maybe we shouldn't acquire the tablet lock on replay?
  RETURN_NOT_OK(tablet_->CreatePreparedAlterSchema(&tx_state, &schema));

  // apply the alter schema to the tablet
  RETURN_NOT_OK_PREPEND(tablet_->AlterSchema(&tx_state), "Failed to AlterSchema:");

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

Status TabletBootstrap::PlayRowOperations(WriteTransactionState* tx_state,
                                          const SchemaPB& schema_pb,
                                          const RowOperationsPB& ops_pb,
                                          const TxResultPB& result) {
  Schema inserts_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(schema_pb, &inserts_schema),
                        "Couldn't decode client schema");

  arena_.Reset();
  vector<DecodedRowOperation> decoded_ops;
  shared_ptr<Schema> schema(tablet_->schema_unlocked());
  RowOperationsPBDecoder dec(&ops_pb, &inserts_schema, schema.get(), &arena_);
  RETURN_NOT_OK_PREPEND(dec.DecodeOperations(&decoded_ops),
                        Substitute("Could not decode row operations: $0",
                                   ops_pb.ShortDebugString()));

  int32_t op_idx = 0;
  BOOST_FOREACH(const DecodedRowOperation& op, decoded_ops) {
    const OperationResultPB& op_result = result.ops(op_idx++);
    // check if the operation failed in the original transaction
    if (PREDICT_FALSE(op_result.has_failed_status())) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Skipping operation that originally resulted in error. OpId: "
                << tx_state->op_id().DebugString() << " op index: "
                << op_idx - 1 << " original error: "
                << op_result.failed_status().DebugString();
      }
      tx_state->AddFailedOperation(Status::RuntimeError("Row operation failed previously."));
      continue;
    }

    switch (op.type) {
      case RowOperationsPB::INSERT:
        RETURN_NOT_OK(PlayInsert(tx_state, op, op_result));
        break;
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::DELETE:
        RETURN_NOT_OK(PlayMutation(tx_state, op, op_result));
        break;
      default:
        LOG(FATAL) << "Bad op type: " << op.type;
        break;
    }
  }
  return Status::OK();
}

Status TabletBootstrap::PlayInsert(WriteTransactionState* tx_state,
                                   const DecodedRowOperation& op,
                                   const OperationResultPB& op_result) {
  DCHECK(op.type == RowOperationsPB::INSERT)
    << RowOperationsPB::Type_Name(op.type);


  if (PREDICT_FALSE(op_result.mutated_stores_size() != 1 ||
                    !op_result.mutated_stores(0).has_mrs_id())) {
    return Status::Corruption(Substitute("Insert operation result must have an mrs_id: $0",
                                         op_result.ShortDebugString()));
  }
  // check if the insert is already flushed
  if (WasStoreAlreadyFlushed(op_result.mutated_stores(0))) {
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Skipping insert that was already flushed. OpId: "
              << tx_state->op_id().DebugString()
              << " flushed to: " << op_result.mutated_stores(0).mrs_id()
              << " latest durable mrs id: " << tablet_->metadata()->last_durable_mrs_id();
    }

    tx_state->AddFailedOperation(Status::AlreadyPresent("Row to insert was already flushed."));
    return Status::OK();
  }

  shared_ptr<Schema> schema(tablet_->schema_unlocked());
  const ConstContiguousRow* row = tx_state->AddToAutoReleasePool(
    new ConstContiguousRow(*schema.get(), op.row_data));

  gscoped_ptr<PreparedRowWrite> prepared_row;
  // TODO maybe we shouldn't acquire the row lock on replay?
  RETURN_NOT_OK(tablet_->CreatePreparedInsert(tx_state, row, &prepared_row));

  // apply the insert to the tablet
  RETURN_NOT_OK_PREPEND(tablet_->InsertUnlocked(tx_state, prepared_row.get()),
                        Substitute("Failed to insert row $0. Reason",
                                   row->schema().DebugRow(*row)));

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Applied Insert. OpId: "
            << tx_state->op_id().DebugString()
            << " row: " << row->schema().DebugRow(*row);
  }
  return Status::OK();
}

Status TabletBootstrap::PlayMutation(WriteTransactionState* tx_state,
                                     const DecodedRowOperation& op,
                                     const OperationResultPB& op_result) {
  DCHECK(op.type == RowOperationsPB::UPDATE ||
         op.type == RowOperationsPB::DELETE)
    << RowOperationsPB::Type_Name(op.type);

  int num_mutated_stores = op_result.mutated_stores_size();
  if (PREDICT_FALSE(num_mutated_stores == 0 || num_mutated_stores > 2)) {
    return Status::Corruption(Substitute("Mutations must have one or two mutated_stores: $0",
                                         op_result.ShortDebugString()));
  }

  // The mutation may have been duplicated, so we'll check whether any of the
  // output targets was "unflushed".
  int num_unflushed_stores = 0;
  BOOST_FOREACH(const MemStoreTargetPB& mutated_store, op_result.mutated_stores()) {
    if (!WasStoreAlreadyFlushed(mutated_store)) {
      num_unflushed_stores++;
    } else {
      if (VLOG_IS_ON(1)) {
        string mutation = op.changelist.ToString(*tablet_->schema_unlocked().get());
        VLOG(1) << "Skipping mutation to " << mutated_store.ShortDebugString()
                << " that was already flushed. "
                << "OpId: " << tx_state->op_id().DebugString();
      }
    }
  }

  if (num_unflushed_stores == 0) {
    // The mutation was fully flushed.
    tx_state->AddFailedOperation(Status::AlreadyPresent("Update was already flushed."));
    return Status::OK();
  }

  if (num_unflushed_stores == 2) {
    // 18:47 < dralves> off the top of my head, if we crashed before writing the meta
    //                  at the end of a flush/compation then both mutations could
    //                  potentually be considered unflushed
    // This case is not currently covered by any tests -- we need to add test coverage
    // for this. See KUDU-218. It's likely the correct behavior is just to apply the edit,
    // ie not fatal below.
    LOG(DFATAL) << "TODO: add test coverage for case where op is unflushed in both duplicated "
                << "targets";
  }

  RETURN_NOT_OK(ApplyMutation(tx_state, op));
  return Status::OK();
}


bool TabletBootstrap::WasStoreAlreadyFlushed(const MemStoreTargetPB& target) {
  if (target.has_mrs_id()) {
    DCHECK(!target.has_rs_id());
    DCHECK(!target.has_delta_id());

    // The original mutation went to the MRS. It is flushed if it went to an MRS
    // with a lower ID than the latest flushed one.
    return target.mrs_id() <= tablet_->metadata()->last_durable_mrs_id();
  } else {
    // The original mutation went to a DRS's delta store.

    // TODO right now this is using GetRowSetForTests which goes through
    // the rs's every time. Just adding a method that gets row sets by id
    // is not enough. We really need to take a snapshot of the initial
    // metadata with regard to which row sets are alive at the time. By
    // doing this we decouple replaying from the current state of the tablet,
    // which allows us to do compactions/flushes on replay.
    const RowSetMetadata* row_set = tablet_->metadata()->GetRowSetForTests(
      target.rs_id());

    // if we can't find the row_set it was compacted
    if (row_set == NULL) {
      return true;
    }

    // if it exists we check if the mutation is already flushed
    if (target.delta_id() <= row_set->last_durable_redo_dms_id()) {
      return true;
    }

    return false;
  }
}

Status TabletBootstrap::ApplyMutation(WriteTransactionState* tx_state,
                                      const DecodedRowOperation& op) {
  shared_ptr<Schema> schema(tablet_->schema_unlocked());
  gscoped_ptr<ConstContiguousRow> row_key(
    new ConstContiguousRow(*schema.get(), op.row_data));
  gscoped_ptr<PreparedRowWrite> prepared_row;
  // TODO maybe we shouldn't acquire the row lock on replay?
  RETURN_NOT_OK(tablet_->CreatePreparedMutate(tx_state, row_key.get(),
                                              op.changelist,
                                              &prepared_row));

  RETURN_NOT_OK(tablet_->MutateRowUnlocked(tx_state, prepared_row.get()));

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Applied Mutation. OpId: " << tx_state->op_id().DebugString()
            << " row key: " << schema.get()->DebugRowKey(*row_key)
            << " mutation: " << op.changelist.ToString(*schema.get());
  }
  return Status::OK();
}

Status TabletBootstrap::UpdateClock(uint64_t timestamp) {
  Timestamp ts;
  RETURN_NOT_OK(ts.FromUint64(timestamp));
  RETURN_NOT_OK(clock_->Update(ts));
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
