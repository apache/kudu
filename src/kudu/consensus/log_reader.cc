// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/log_reader.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <algorithm>

#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding.h"
#include "kudu/util/env_util.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace log {

namespace {
struct LogSegmentSeqnoComparator {
  bool operator() (const scoped_refptr<ReadableLogSegment>& a,
                   const scoped_refptr<ReadableLogSegment>& b) {
    return a->header().sequence_number() < b->header().sequence_number();
  }
};
}

using consensus::OpId;
using consensus::ReplicateMsg;
using env_util::ReadFully;
using strings::Substitute;

Status LogReader::Open(FsManager *fs_manager,
                       const string& tablet_oid,
                       gscoped_ptr<LogReader> *reader) {
  gscoped_ptr<LogReader> log_reader(new LogReader(fs_manager, tablet_oid));

  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_oid);

  RETURN_NOT_OK(log_reader->Init(tablet_wal_path))
  reader->reset(log_reader.release());
  return Status::OK();
}

Status LogReader::OpenFromRecoveryDir(FsManager *fs_manager,
                                      const string& tablet_oid,
                                      gscoped_ptr<LogReader>* reader) {
  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_oid);
  gscoped_ptr<LogReader> log_reader(new LogReader(fs_manager, tablet_oid));
  RETURN_NOT_OK_PREPEND(log_reader->Init(recovery_path),
                        "Unable to initialize log reader");
  reader->reset(log_reader.release());
  return Status::OK();
}

LogReader::LogReader(FsManager *fs_manager,
                     const string& tablet_oid)
  : fs_manager_(fs_manager),
    tablet_oid_(tablet_oid),
    state_(kLogReaderInitialized) {
}

Status LogReader::Init(const string& tablet_wal_path) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(state_, kLogReaderInitialized) << "bad state for Init(): " << state_;
  }
  VLOG(1) << "Reading wal from path:" << tablet_wal_path;

  Env* env = fs_manager_->env();

  if (!fs_manager_->Exists(tablet_wal_path)) {
    return Status::IllegalState("Cannot find wal location at", tablet_wal_path);
  }

  VLOG(1) << "Parsing segments from path: " << tablet_wal_path;
  // list existing segment files
  vector<string> log_files;

  RETURN_NOT_OK_PREPEND(env->GetChildren(tablet_wal_path, &log_files),
                        "Unable to read children from path");

  SegmentSequence read_segments;

  // build a log segment from each file
  BOOST_FOREACH(const string &log_file, log_files) {
    if (HasPrefixString(log_file, FsManager::kWalFileNamePrefix)) {
      string fqp = JoinPathSegments(tablet_wal_path, log_file);
      scoped_refptr<ReadableLogSegment> segment;
      RETURN_NOT_OK_PREPEND(ReadableLogSegment::Open(env, fqp, &segment),
                            "Unable to open readable log segment");
      DCHECK(segment);
      CHECK(segment->IsInitialized()) << "Uninitialized segment at: " << segment->path();

      if (!segment->HasFooter()) {
        LOG(WARNING) << "Segment: " << fqp << " was likely left in-progress "
            "after a previous crash. Will try to rebuild footer by scanning data";
        RETURN_NOT_OK(segment->RebuildFooterByScanning());
      }

      read_segments.push_back(segment);
    }
  }

  // Sort the segments by sequence number.
  std::sort(read_segments.begin(), read_segments.end(), LogSegmentSeqnoComparator());


  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    string previous_seg_path;
    int64_t previous_seg_seqno = -1;
    BOOST_FOREACH(const SegmentSequence::value_type& entry, read_segments) {
      VLOG(1) << " Log Reader Indexed: " << entry->footer().ShortDebugString();
      // Check that the log segments are in sequence.
      if (previous_seg_seqno != -1 && entry->header().sequence_number() != previous_seg_seqno + 1) {
        return Status::Corruption(Substitute("Segment sequence numbers are not consecutive. "
            "Previous segment: seqno $0, path $1; Current segment: seqno $2, path $3",
            previous_seg_seqno, previous_seg_path,
            entry->header().sequence_number(), entry->path()));
        previous_seg_seqno++;
      } else {
        previous_seg_seqno = entry->header().sequence_number();
      }
      previous_seg_path = entry->path();
      RETURN_NOT_OK(AppendSegmentUnlocked(entry));
    }

    state_ = kLogReaderReading;
  }
  return Status::OK();
}

Status LogReader::InitEmptyReaderForTests() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  state_ = kLogReaderReading;
  return Status::OK();
}

Status LogReader::GetSegmentPrefixNotIncluding(int64_t index,
                                               SegmentSequence* segments) const {
  DCHECK_GE(index, 0);
  DCHECK(segments);
  segments->clear();

  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);

  BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments_) {
    // The last segment doesn't have a footer. Never include that one.
    if (!segment->HasFooter()) {
      break;
    }
    if (segment->footer().max_replicate_index() >= index) {
      break;
    }
    // TODO: tests for edge cases here with backwards ordered replicates.
    segments->push_back(segment);
  }

  return Status::OK();
}

Status LogReader::GetSegmentSuffixIncluding(int64_t index,
                                            SegmentSequence* segments) const {
  DCHECK_GE(index, 0);
  boost::lock_guard<simple_spinlock> lock(lock_);
  DCHECK(segments);
  segments->clear();

  CHECK_EQ(state_, kLogReaderReading);

  bool including = false;
  BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments_) {
    // Start including segments with the first one that contains a higher
    // replicate ID (and include all later segments as well).
    if (segment->HasFooter() && segment->footer().max_replicate_index() >= index) {
      including = true;
    }

    // The last segment doesn't have a footer. Always include that one,
    // even if no other segments matched.
    if (!segment->HasFooter() || including) {
      segments->push_back(segment);
    }
  }

  return Status::OK();
}

Status LogReader::ReadAllReplicateEntries(const int64_t starting_after,
                                          const int64_t up_to,
                                          vector<ReplicateMsg*>* replicates) const {
  DCHECK_GE(starting_after, 0);
  DCHECK_GT(up_to, starting_after);

  int64_t num_entries = up_to - starting_after;
  // Pre-reserve space in our result array. We'll fill in the messages as we go,
  // letting later messages replace earlier (they may have been appended by later
  // leaders).
  replicates->assign(num_entries, static_cast<ReplicateMsg*>(NULL));

  // Get all segments which might include 'starting_after' (and anything after it).
  log::SegmentSequence segments_copy;
  RETURN_NOT_OK(GetSegmentSuffixIncluding(starting_after, &segments_copy));

  // Read the entries from the relevant segment(s).
  // TODO: we should do this lazily - right now we're reading way more than
  // necessary!
  vector<log::LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  BOOST_FOREACH(const scoped_refptr<log::ReadableLogSegment>& segment, segments_copy) {
    RETURN_NOT_OK(segment->ReadEntries(&entries));
  }

  // Now filter the entries to make sure we only get replicates and in the range
  // we want.
  BOOST_FOREACH(log::LogEntryPB* entry, entries) {
    if (entry->has_commit() &&
        entry->commit().commited_op_id().index() >= up_to) {
      // We know that once we see a commit for 'up_to' or higher, that it
      // cannot be replaced in the log by a later REPLICATE.
      // If we never see such a COMMIT, we'll read to the very end of the log.
      break;
    }

    // Other than that, we only need to worry about REPLICATE messages.
    if (!entry->has_replicate()) continue;

    int64_t repl_index = entry->replicate().id().index();
    if (repl_index <= starting_after) {
      // We don't care about replicates before our range.
      continue;
    }
    if (repl_index > up_to) {
      // Ignore anything coming after our target range. We can't
      // break here, because we may have a replaced operation with
      // an earlier index which comes later in the log.
      continue;
    }

    // Compute the index in our output array.
    int64_t relative_idx = repl_index - starting_after - 1;
    DCHECK_LT(relative_idx, replicates->size());
    DCHECK_GE(relative_idx, 0);

    // We may be replacing an earlier replicate with the same ID, in which case
    // we need to delete it.
    delete (*replicates)[relative_idx];
    (*replicates)[relative_idx] = entry->release_replicate();
  }

  // Sanity-checks that we found what we were looking for.
  // First, verify the common cases where our lower or upper bounds
  // came before or after the beginning or end of the log, respectively.
  if (replicates->front() == NULL) {
    STLDeleteElements(replicates);
    return Status::NotFound(
      Substitute("Could not find operations starting at $0 in the log "
                 "(perhaps they were already GCed)",
                 starting_after + 1));
  }
  if (replicates->back() == NULL) {
    STLDeleteElements(replicates);
    return Status::NotFound(
      Substitute("Could not find operations through $0 in the log "
                 "(perhaps they were not yet written locally)",
                 up_to));
  }

  // Even though the first and last op might be present, a corrupt log
  // might be missing one in the middle. In this case, we'll return a
  // Corruption instead of NotFound.
  int found_count = 0;
  BOOST_FOREACH(ReplicateMsg* msg, *replicates) {
    if (msg == NULL) {
      STLDeleteElements(replicates);
      return Status::Corruption(
        Substitute("Could not find operations $0 through $1 in the log (missing op $2)",
                   starting_after + 1, up_to, starting_after + 1 + found_count));
    } else {
      found_count++;
    }
  }

  return Status::OK();
}

Status LogReader::GetSegmentsSnapshot(SegmentSequence* segments) const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  segments->assign(segments_.begin(), segments_.end());
  return Status::OK();
}

Status LogReader::TrimSegmentsUpToAndIncluding(uint64_t segment_sequence_number) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  SegmentSequence::iterator iter = segments_.begin();
  int num_deleted_segments = 0;

  while (iter != segments_.end()) {
    if ((*iter)->header().sequence_number() <= segment_sequence_number) {
      iter = segments_.erase(iter);
      num_deleted_segments++;
      continue;
    }
    break;
  }
  LOG(INFO) << "Removed " << num_deleted_segments << " from log reader.";
  return Status::OK();
}

void LogReader::UpdateLastSegmentOffset(uint64_t readable_to_offset) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  DCHECK(!segments_.empty());
  // Get the last segment
  ReadableLogSegment* segment = segments_.back().get();
  DCHECK(!segment->HasFooter());
  segment->UpdateReadableToOffset(readable_to_offset);
}

Status LogReader::ReplaceLastSegment(const scoped_refptr<ReadableLogSegment>& segment) {
  // This is used to replace the last segment once we close it properly so it must
  // have a footer.
  DCHECK(segment->HasFooter());

  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  // Make sure the segment we're replacing has the same sequence number
  CHECK(!segments_.empty());
  CHECK_EQ(segment->header().sequence_number(), segments_.back()->header().sequence_number());
  segments_[segments_.size() - 1] = segment;

  return Status::OK();
}

Status LogReader::AppendSegment(const scoped_refptr<ReadableLogSegment>& segment) {
  DCHECK(segment->IsInitialized());
  if (PREDICT_FALSE(!segment->HasFooter())) {
    RETURN_NOT_OK(segment->RebuildFooterByScanning());
  }
  boost::lock_guard<simple_spinlock> lock(lock_);
  return AppendSegmentUnlocked(segment);
}

Status LogReader::AppendSegmentUnlocked(const scoped_refptr<ReadableLogSegment>& segment) {
  DCHECK(segment->IsInitialized());
  DCHECK(segment->HasFooter());

  if (!segments_.empty()) {
    CHECK_EQ(segments_.back()->header().sequence_number() + 1,
             segment->header().sequence_number());
  }
  segments_.push_back(segment);
  return Status::OK();
}

Status LogReader::AppendEmptySegment(const scoped_refptr<ReadableLogSegment>& segment) {
  DCHECK(segment->IsInitialized());
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  if (!segments_.empty()) {
    CHECK_EQ(segments_.back()->header().sequence_number() + 1,
             segment->header().sequence_number());
  }
  segments_.push_back(segment);
  return Status::OK();
}

const int LogReader::num_segments() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return segments_.size();
}

string LogReader::ToString() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  string ret = "Reader's SegmentSequence: \n";
  BOOST_FOREACH(const SegmentSequence::value_type& entry, segments_) {
    ret.append(Substitute("Segment: $0 Footer: $1\n",
                          entry->header().sequence_number(),
                          !entry->HasFooter() ? "NONE" : entry->footer().ShortDebugString()));
  }
  return ret;
}

}  // namespace log
}  // namespace kudu
