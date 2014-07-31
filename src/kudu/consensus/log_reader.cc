// Copyright (c) 2013, Cloudera, inc.

#include "kudu/consensus/log_reader.h"

#include <boost/foreach.hpp>
#include <algorithm>

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

using consensus::OpId;
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
  CHECK_EQ(state_, kLogReaderInitialized) << "bad state for Init(): " << state_;
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

  // build a log segment from each file
  BOOST_FOREACH(const string &log_file, log_files) {
    if (HasPrefixString(log_file, FsManager::kWalFileNamePrefix)) {
      string fqp = JoinPathSegments(tablet_wal_path, log_file);
      scoped_refptr<ReadableLogSegment> segment;
      RETURN_NOT_OK_PREPEND(ReadableLogSegment::Open(env, fqp, &segment),
                            "Unable to open readable log segment");
      DCHECK(segment);
      if (!segment->IsInitialized()) {
        // Skip blank segments.
        LOG(WARNING) << "Skipping blank or empty segment: " << fqp;
        continue;
      }
      const OpId& op_id = segment->header().initial_id();
      InsertOrDie(&segments_, op_id, segment);
    }
  }

  state_ = kLogReaderReading;
  return Status::OK();
}

const uint32_t LogReader::size() {
  return segments_.size();
}

}  // namespace log
}  // namespace kudu
