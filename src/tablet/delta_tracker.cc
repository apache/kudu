// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/shared_mutex.hpp>
#include <string>
#include <tr1/memory>

#include "gutil/strings/strip.h"
#include "util/env.h"
#include "util/status.h"
#include "tablet/deltafile.h"
#include "tablet/delta_tracker.h"
#include "tablet/layer.h"

namespace kudu { namespace tablet {

using std::string;
using std::tr1::shared_ptr;


DeltaTracker::DeltaTracker(Env *env,
                           const Schema &schema,
                           const string &dir) :
  env_(env),
  schema_(schema),
  dir_(dir),
  open_(false),
  dms_(new DeltaMemStore(schema))
{}


// Open any previously flushed DeltaFiles in this layer
Status DeltaTracker::Open() {
  CHECK(delta_trackers_.empty()) << "should call before opening any readers";
  CHECK(!open_);

  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(dir_, &children));
  BOOST_FOREACH(const string &child, children) {
    // Skip hidden files (also '.' and '..')
    if (child[0] == '.') continue;

    string absolute_path = env_->JoinPathSegments(dir_, child);

    string suffix;
    if (TryStripPrefixString(child, Layer::kDeltaPrefix, &suffix)) {
      // The file should be named 'delta_<N>'. N here is the index
      // of the delta file (indicating the order in which it was flushed).
      uint32_t delta_idx;
      if (!safe_strtou32(suffix.c_str(), &delta_idx)) {
        return Status::IOError(string("Bad delta file: ") + absolute_path);
      }

      DeltaFileReader *dfr;
      Status s = DeltaFileReader::Open(env_, absolute_path, schema_, &dfr);
      if (!s.ok()) {
        LOG(ERROR) << "Failed to open delta file " << absolute_path << ": "
                   << s.ToString();
        return s;
      }
      LOG(INFO) << "Successfully opened delta file " << absolute_path;

      delta_trackers_.push_back(shared_ptr<DeltaTrackerInterface>(dfr));

      next_delta_idx_ = std::max(next_delta_idx_,
                                 delta_idx + 1);
    } else if (TryStripPrefixString(child, Layer::kColumnPrefix, &suffix)) {
      // expected: column data
    } else {
      LOG(WARNING) << "ignoring unknown file: " << absolute_path;
    }
  }

  return Status::OK();
}


RowIteratorInterface *DeltaTracker::WrapIterator(
  const shared_ptr<RowIteratorInterface> &base) const
{
  std::vector<shared_ptr<DeltaTrackerInterface> > deltas;

  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    deltas.assign(delta_trackers_.begin(), delta_trackers_.end());
    deltas.push_back(dms_);
  }

  return new DeltaMergingIterator(base, deltas, schema_, base->schema());
}

void DeltaTracker::Update(uint32_t row_idx, const RowDelta &update) {
  // TODO: can probably lock this more fine-grained.
  boost::shared_lock<boost::shared_mutex> lock(component_lock_);

  dms_->Update(row_idx, update);
}


Status DeltaTracker::FlushDMS(const DeltaMemStore &dms,
                       DeltaFileReader **dfr) {
  int delta_idx = next_delta_idx_++;
  string path = Layer::GetDeltaPath(dir_, delta_idx);

  // Open file for write.
  WritableFile *out;
  Status s = env_->NewWritableFile(path, &out);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open output file for delta level " <<
      delta_idx << " at path " << path << ": " << 
      s.ToString();
    return s;
  }

  // Construct a shared_ptr so that, if the writer construction
  // fails, we don't leak the file descriptor.
  shared_ptr<WritableFile> out_shared(out);

  DeltaFileWriter dfw(schema_, out_shared);

  s = dfw.Start();
  if (!s.ok()) {
    LOG(WARNING) << "Unable to start delta file writer for path " <<
      path;
    return s;
  }
  RETURN_NOT_OK(dms.FlushToFile(&dfw));
  RETURN_NOT_OK(dfw.Finish());
  LOG(INFO) << "Flushed delta file: " << path;

  // Now re-open for read
  RETURN_NOT_OK(DeltaFileReader::Open(env_, path, schema_, dfr));
  LOG(INFO) << "Reopened delta file for read: " << path;

  return Status::OK();
}

Status DeltaTracker::Flush() {
  // First, swap out the old DeltaMemStore with a new one,
  // and add it to the list of delta trackers to be reflected
  // in reads.
  shared_ptr<DeltaMemStore> old_dms;
  size_t count;
  {
    // Lock the component_lock_ in exclusive mode.
    // This shuts out any concurrent readers or writers.
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);

    count = dms_->Count();
    if (count == 0) {
      // No need to flush if there are no deltas.
      return Status::OK();
    }

    old_dms = dms_;
    dms_.reset(new DeltaMemStore(schema_));

    delta_trackers_.push_back(old_dms);
  }

  LOG(INFO) << "Flushing " << count << " deltas...";

  // Now, actually flush the contents of the old DMS.
  // TODO: need another lock to prevent concurrent flushers
  // at some point.
  DeltaFileReader *dfr;
  Status s = FlushDMS(*old_dms, &dfr);
  CHECK(s.ok())
    << "Failed to flush DMS: " << s.ToString()
    << "\nTODO: need to figure out what to do with error handling "
    << "if this fails -- we end up with a DeltaMemStore permanently "
    << "in the tracker list. For now, abort.";


  // Now, re-take the lock and swap in the DeltaFileReader in place of
  // of the DeltaMemStore
  {
    boost::lock_guard<boost::shared_mutex> lock(component_lock_);
    size_t idx = delta_trackers_.size() - 1;

    CHECK_EQ(delta_trackers_[idx], old_dms)
      << "Another thread modified the delta tracker list during flush";
    delta_trackers_[idx].reset(dfr);
  }

  return Status::OK();

  // TODO: wherever we write stuff, we should write to a tmp path
  // and rename to final path!
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////

Status DeltaMergingIterator::Init() {
  RETURN_NOT_OK(base_iter_->Init());
  RETURN_NOT_OK(
    projection_.GetProjectionFrom(src_schema_, &projection_mapping_));
  return Status::OK();
}

Status DeltaMergingIterator::CopyNextRows(size_t *nrows, RowBlock *dst)
{
  // Get base data
  RETURN_NOT_OK(base_iter_->CopyNextRows(nrows, dst));
  size_t old_cur_row = cur_row_;
  cur_row_ += *nrows;

  if (*nrows == 0) {
    // TODO: does this happen?
    return Status::OK();
  }

  // Apply updates
  BOOST_FOREACH(shared_ptr<DeltaTrackerInterface> &tracker, delta_trackers_) {
    for (size_t proj_col_idx = 0; proj_col_idx < projection_.num_columns(); proj_col_idx++) {
      ColumnBlock dst_col = dst->column_block(proj_col_idx, *nrows);
      size_t src_col_idx = projection_mapping_[proj_col_idx];

      RETURN_NOT_OK(tracker->ApplyUpdates(src_col_idx, old_cur_row, &dst_col));
    }
  }

  return Status::OK();
}



} // namespace tablet
} // namespace kudu
