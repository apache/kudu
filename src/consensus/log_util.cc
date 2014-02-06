// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log_util.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <utility>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"

DEFINE_int32(log_segment_size_mb, 64,
             "The default segment size for log roll-overs, in MB");

DEFINE_bool(log_force_fsync_all, false,
            "Whether the Log/WAL should explicitly call fsync() after each write.");

DEFINE_bool(log_preallocate_segments, true,
            "Whether the WAL should preallocate the entire segment before writing to it");

namespace kudu {
namespace log {

using google::protobuf::RepeatedPtrField;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using std::tr1::unordered_set;
using metadata::TabletSuperBlockPB;
using metadata::RowSetDataPB;

const char kMagicString[] = "kudulogf";
const char kLogPrefix[] = "log";
const size_t kMagicAndHeaderLength = 12;
const size_t kEntryLengthSize = 4;
const int kLogMajorVersion = 1;
const int kLogMinorVersion = 0;

LogOptions::LogOptions()
: segment_size_mb(FLAGS_log_segment_size_mb),
  force_fsync_all(FLAGS_log_force_fsync_all),
  preallocate_segments(FLAGS_log_preallocate_segments) {
}

LogSegment::LogSegment(const LogSegmentHeaderPB& header,
                       const string &path)
: header_(header),
  path_(path) {
}

ReadableLogSegment::ReadableLogSegment(
    const LogSegmentHeaderPB& header,
    const std::string &path,
    uint64_t first_entry_offset,
    uint64_t file_size,
    const std::tr1::shared_ptr<RandomAccessFile>& readable_file)
: LogSegment(header, path),
  first_entry_offset_(first_entry_offset),
  file_size_(file_size),
  readable_file_(readable_file) {
}

WritableLogSegment::WritableLogSegment(
    const LogSegmentHeaderPB& header,
    const string &path,
    const shared_ptr<WritableFile>& writable_file)
: LogSegment(header, path),
  writable_file_(writable_file) {
}

// helper hash functor for delta store ids
struct DeltaIdHashFunction {
  size_t operator()(const pair<int64_t, int64_t >& id) const {
    return (id.first + 31) ^ id.second;
  }
};

// helper equals functor for delta store ids
struct DeltaIdEqualsTo {
  bool operator()(const pair<int64_t, int64_t >& left,
                  const pair<int64_t, int64_t >& right) const {
    return left.first == right.first && left.second == right.second;
  }
};

bool HasNoCommonMemStores(const TabletSuperBlockPB &older,
                          const TabletSuperBlockPB &newer) {
  // if 'last_durable_mrs_id' in 'newer' is the same as 'older' then the current
  // mrs is the same.
  if (older.last_durable_mrs_id() == newer.last_durable_mrs_id()) {
    VLOG(2) << "Different mrs_ids. 'older' "
            << older.ShortDebugString() << " 'newer': " << newer.ShortDebugString();
    return false;
  }

  // If 'last_durable_mrs_id' is different then we need to go through the
  // 'newer' meta, and for each RowSetDataPB also present in 'older' check if
  // the latest DeltaMemStore has the same id.

  // start by creating a set with pair<row set id, last delta id> for 'newer'
  unordered_set<pair<int64, int64 >, DeltaIdHashFunction, DeltaIdEqualsTo> newer_deltas;
  BOOST_FOREACH(const RowSetDataPB &row_set, newer.rowsets()) {
    if (row_set.deltas_size() > 0) {
      newer_deltas.insert(pair<int64, int64>(row_set.id(),
                                             row_set.deltas(row_set.deltas_size() - 1).id()));
    }
  }

  // now go through the row sets in 'older' if anyone of them is also present
  // in 'newer' *and* its last delta id is the same then the two superblocks have
  // at least one DeltaMemStore in common.
  BOOST_FOREACH(const RowSetDataPB &row_set, older.rowsets()) {
    if (row_set.deltas_size() > 0 &&
        ContainsKey(newer_deltas,
                    pair<int64, int64>(row_set.id(),
                                       row_set.deltas(row_set.deltas_size() - 1).id()))) {
      VLOG(2) << "Common MemStore. 'older' "
          << older.DebugString() << "\n 'newer': " << newer.DebugString();
      return false;
    }
  }
  return true;
}

Status FindStaleSegmentsPrefixSize(
    const vector<shared_ptr<ReadableLogSegment> > &segments,
    const TabletSuperBlockPB &current,
    uint32_t *prefix_size) {

  // Go through the segments and find the first one (if any) that has
  // a header with at least one memory store entry in common with current.
  // If such a segment exists and is the first one then no GC can be performed.
  // If such a segment exists (the nth segment) then n-2 segment can definitely
  // be GC'd (since we're reading headers the first non-stale segment means
  // that the prior one is the one where one of current mem stores was first
  // introduced). It is unlikely that the n-1 segment can be GC'd since the
  // metadata entry that introduced a currently active store would have
  // to be the very last entry in the segment.

  int32_t stale_prefix_size = 0;
  BOOST_FOREACH(const shared_ptr<ReadableLogSegment> &segment, segments) {
    if (HasNoCommonMemStores(segment->header().tablet_meta(), current)) {
      stale_prefix_size++;
      continue;
    }
    break;
  }

  // we need to remove a segment because if, for two subsequent segments, one
  // has no active stores in the header and the next one does, then the first
  // segment has a high probability of having an ActiveMemStoreLogEntryPB with
  // currently active stores somewhere after the header.
  *prefix_size = std::max(0, stale_prefix_size - 1);
  VLOG(1) << "Found " << *prefix_size << " stale segments out of "
      << segments.size();
  return Status::OK();
}

}  // namespace log
}  // namespace kudu
