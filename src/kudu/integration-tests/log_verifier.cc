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

#include "kudu/integration-tests/log_verifier.h"

#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/util/env.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

using std::map;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using consensus::OpId;
using log::LogReader;
using itest::ExternalMiniClusterFsInspector;

LogVerifier::LogVerifier(ExternalMiniCluster* cluster)
    : cluster_(cluster) {
}

LogVerifier::~LogVerifier() {
}

Status LogVerifier::OpenFsManager(ExternalTabletServer* ets,
                                  unique_ptr<FsManager>* fs) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.wal_path = ets->wal_dir();
  fs_opts.data_paths = ets->data_dirs();
  unique_ptr<FsManager> ret(new FsManager(Env::Default(), fs_opts));
  RETURN_NOT_OK_PREPEND(ret->Open(),
                        Substitute("Couldn't initialize FS Manager for $0", ets->wal_dir()));
  fs->swap(ret);
  return Status::OK();
}

Status LogVerifier::ScanForCommittedOpIds(FsManager* fs, const string& tablet_id,
                                          map<int64_t, int64_t>* index_to_term) {

  shared_ptr<LogReader> reader;
  RETURN_NOT_OK(LogReader::Open(fs, scoped_refptr<log::LogIndex>(), tablet_id,
                                scoped_refptr<MetricEntity>(), &reader));
  log::SegmentSequence segs;
  RETURN_NOT_OK(reader->GetSegmentsSnapshot(&segs));
  log::LogEntryPB entry;
  for (const auto& seg : segs) {
    log::LogEntryReader reader(seg.get());
    while (true) {
      Status s = reader.ReadNextEntry(&entry);
      if (s.IsEndOfFile() || s.IsCorruption()) break;
      RETURN_NOT_OK(s);
      if (entry.type() != log::COMMIT) continue;
      const auto& op_id = entry.commit().commited_op_id();

      if (!InsertIfNotPresent(index_to_term, op_id.index(), op_id.term())) {
        return Status::Corruption(Substitute(
            "Index $0 had two COMMIT messages: $1.$0 and $2.$0",
            op_id.index(), op_id.term(), (*index_to_term)[op_id.index()]));
      }
    }
  }

  return Status::OK();
}

Status LogVerifier::ScanForHighestCommittedOpIdInLog(ExternalTabletServer* ets,
                                                     const string& tablet_id,
                                                     OpId* commit_id) {
  unique_ptr<FsManager> fs;
  RETURN_NOT_OK(OpenFsManager(ets, &fs));
  const string& wal_dir = fs->GetTabletWalDir(tablet_id);
  map<int64_t, int64_t> index_to_term;
  RETURN_NOT_OK_PREPEND(ScanForCommittedOpIds(fs.get(), tablet_id, &index_to_term),
                        Substitute("Couldn't scan log in dir $0", wal_dir));
  if (index_to_term.empty()) {
    return Status::NotFound("no COMMITs in log");
  }
  commit_id->set_index(index_to_term.rbegin()->first);
  commit_id->set_term(index_to_term.rbegin()->second);
  return Status::OK();
}

Status LogVerifier::VerifyCommittedOpIdsMatch() {
  ExternalMiniClusterFsInspector inspect(cluster_);
  Env* env = Env::Default();

  for (const string& tablet_id : inspect.ListTablets()) {
    LOG(INFO) << "Checking tablet " << tablet_id;

    // Union set of the op indexes seen on any server.
    set<int64_t> all_op_indexes;
    // For each server in the cluster, a map of [index->term].
    vector<map<int64_t, int64_t>> maps_by_ts(cluster_->num_tablet_servers());

    // Gather the [index->term] map for each of the tablet servers
    // hosting this tablet.
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      unique_ptr<FsManager> fs;
      RETURN_NOT_OK(OpenFsManager(cluster_->tablet_server(i), &fs));
      const string& wal_dir = fs->GetTabletWalDir(tablet_id);
      if (!env->FileExists(wal_dir)) continue;
      map<int64_t, int64_t> index_to_term;
      RETURN_NOT_OK_PREPEND(ScanForCommittedOpIds(fs.get(), tablet_id, &index_to_term),
                            Substitute("Couldn't scan log for TS $0", i));
      for (const auto& index_term : index_to_term) {
        all_op_indexes.insert(index_term.first);
      }
      maps_by_ts[i] = std::move(index_to_term);
    }

    // Check that the terms match up across servers.
    vector<int64_t> committed_terms;
    // Indicates that the op is not on this server.
    const int64_t kNotOnThisServer = -1;
    for (int64_t index : all_op_indexes) {
      committed_terms.clear();
      for (int ts = 0; ts < cluster_->num_tablet_servers(); ts++) {
        committed_terms.push_back(FindWithDefault(maps_by_ts[ts], index, kNotOnThisServer));
      }
      // 'committed_terms' entries should all be kNotOnThisServer or the same as each other.
      boost::optional<int64_t> expected_term;
      for (int ts = 0; ts < cluster_->num_tablet_servers(); ts++) {
        int64_t this_ts_term = committed_terms[ts];
        if (this_ts_term == kNotOnThisServer) continue; // this TS doesn't have the op
        if (expected_term == boost::none) {
          expected_term = this_ts_term;
        } else if (this_ts_term != expected_term) {
          string err = Substitute("Mismatch found for index $0, [", index);
          for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
            if (i != 0) err += ", ";
            strings::SubstituteAndAppend(&err, "T $0=$1",
                                         cluster_->tablet_server(i)->uuid(),
                                         committed_terms[i]);
          }
          err += "]";
          return Status::Corruption(err);
        }
      }
    }

    LOG(INFO) << "Verified matching terms for " << all_op_indexes.size() << " ops in tablet "
              << tablet_id;
  }
  return Status::OK();
}

} // namespace kudu
