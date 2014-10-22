// Copyright (c) 2013, Cloudera, inc.

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

Status LogReader::GetSegmentPrefixNotIncluding(const consensus::OpId& opid,
                                               SegmentSequence* segments) const {
  DCHECK(opid.IsInitialized());
  DCHECK(segments);
  segments->clear();

  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);

  // This gives us the segment that might include 'opid'
  ReadableLogSegmentIndex::const_iterator pos = segments_idx_.lower_bound(opid);

  // If we couldn't find a segment, that means the operation was already GC'd, just return.
  if (pos == segments_idx_.end()) return Status::OK();

  // Return all segments before, but not including 'pos'.
  BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments_) {
    if (segment->header().sequence_number() < (*pos).second.entry_segment_seqno) {
      segments->push_back(segment);
      continue;
    }
    break;
  }

  return Status::OK();
}

Status LogReader::GetSegmentSuffixIncluding(const consensus::OpId& opid,
                                            SegmentSequence* segments) const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  DCHECK(opid.IsInitialized());
  DCHECK(segments);
  segments->clear();

  CHECK_EQ(state_, kLogReaderReading);

  // This gives us the segment that might include 'opid'
  ReadableLogSegmentIndex::const_iterator pos = segments_idx_.lower_bound(opid);

  // If we couldn't find a segment, that means the operation was already GC'd, return NotFound
  if (pos == segments_idx_.end()) {
    return Status::NotFound(
        Substitute("No segment currently contains, or might contain opid: $0",
                   opid.ShortDebugString()));
  }


  // Return all segments after, and including, 'pos'.
  BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments_) {
    if (segment->header().sequence_number() >= (*pos).second.entry_segment_seqno) {
      segments->push_back(segment);
    }
  }

  return Status::OK();
}

Status LogReader::ReadAllReplicateEntries(const OpId starting_after,
                                          const OpId up_to,
                                          vector<ReplicateMsg*>* replicates) const {
  DCHECK(starting_after.IsInitialized());
  DCHECK(up_to.IsInitialized());

  log::SegmentSequence segments_copy;
  RETURN_NOT_OK(GetSegmentSuffixIncluding(starting_after, &segments_copy));

  // Read the entries from the log
  vector<log::LogEntryPB*> entries;
  ElementDeleter deleter(&entries);
  BOOST_FOREACH(const scoped_refptr<log::ReadableLogSegment>& segment, segments_copy) {
    RETURN_NOT_OK(segment->ReadEntries(&entries));
  }

  // Now filter the entries to make sure we only get replicates and in the range
  // we want.
  OpId previous_entry;
  bool found_start = false;
  bool found_end = false;
  BOOST_FOREACH(log::LogEntryPB* entry, entries) {
    if (!entry->has_replicate()) continue;

    if (!found_start) {
      found_start = OpIdEquals(entry->replicate().id(), starting_after);
      previous_entry.CopyFrom(entry->replicate().id());
      continue;
    }

    DCHECK(found_start);
    DCHECK_EQ(entry->replicate().id().index(), previous_entry.index() + 1);

    ReplicateMsg* replicate = entry->release_replicate();

    replicates->push_back(replicate);
    previous_entry = replicate->id();

    // If the current message is equal to 'to' we're done.
    if (OpIdEquals(replicate->id(), up_to)) {
      found_end = true;
      break;
    }
  }

  if (!found_start) {
    return Status::NotFound(Substitute("Could not find 'starting_after' operation in the log."
                                       " 'starting_after' was: $0",
                                       starting_after.ShortDebugString()));
  }

  if (!found_end) {
    return Status::NotFound(
        Substitute("Could not find 'up_to operation in the log. 'up_to' was: $0, but last was: $1",
                   up_to.ShortDebugString(),
                   replicates->empty() ? "None" : replicates->back()->id().ShortDebugString()));
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
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, kLogReaderReading);
  // Make sure the segment we're replacing has the same sequence number
  CHECK(!segments_.empty());
  CHECK_EQ(segment->header().sequence_number(), segments_.back()->header().sequence_number());
  segments_[segments_.size() - 1] = segment;

  // This is used to replace the last segment once we close it properly so it must
  // have a footer.
  DCHECK(segment->HasFooter());

  // Add/replace the index entries if the segment has a footer.
  BOOST_FOREACH(const SegmentIdxPosPB& pos_pb, segment->footer().idx_entry()) {
    SegmentIdxPos pos;
    pos.entry_pb = pos_pb;
    pos.entry_segment_seqno = segment->header().sequence_number();
    InsertOrDie(&segments_idx_, pos.entry_pb.id(), pos);
  }


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
  if (segment->footer().idx_entry_size() > 0) {
    BOOST_FOREACH(const SegmentIdxPosPB& pos_pb, segment->footer().idx_entry()) {
      SegmentIdxPos pos;
      pos.entry_pb = pos_pb;
      pos.entry_segment_seqno = segment->header().sequence_number();
      if (PREDICT_FALSE(!InsertIfNotPresent(&segments_idx_, pos.entry_pb.id(), pos))) {
        LOG(FATAL) << "Tablet " << tablet_oid_ << ": Created two log segments containing "
            << "the same OpId " << pos.entry_pb.id() << ": one segment has seqno "
            << segment->header().sequence_number() << " and the other has seqno "
            << segments_idx_[pos.entry_pb.id()].entry_segment_seqno;
      }
    }
  }
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
  ret.append("Reader's Segment Index: \n");
  BOOST_FOREACH(const ReadableLogSegmentIndex::value_type& entry, segments_idx_) {
    ret.append(Substitute("Op: $0 Segment: $1\n",
                          entry.first.ShortDebugString(),
                          entry.second.entry_segment_seqno));
  }
  return ret;
}

}  // namespace log
}  // namespace kudu
