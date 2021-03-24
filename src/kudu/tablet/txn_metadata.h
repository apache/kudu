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
#pragma once

#include <glog/logging.h>
#include <boost/optional/optional.hpp>

#include "kudu/common/timestamp.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/mutex.h"

namespace kudu {
namespace tablet {

// Encapsulates the persistent state associated with a transaction.
class TxnMetadata : public RefCountedThreadSafe<TxnMetadata> {
 public:
  explicit TxnMetadata(bool aborted = false,
                       boost::optional<Timestamp> commit_mvcc_op_timestamp = boost::none,
                       boost::optional<Timestamp> commit_ts = boost::none,
                       bool flushed_committed_mrs = false)
      : aborted_(aborted),
        commit_mvcc_op_timestamp_(std::move(commit_mvcc_op_timestamp)),
        commit_timestamp_(std::move(commit_ts)),
        flushed_committed_mrs_(flushed_committed_mrs) {}

  // NOTE: access to 'flushed_committed_mrs_' is not inherently threadsafe --
  // it is expected that the caller will ensure thread safety (e.g.
  // TabletMetadata only calls this with its flush lock held).
  void set_flushed_committed_mrs_unlocked() {
    flushed_committed_mrs_ = true;
  }
  bool flushed_committed_mrs_unlocked() const {
    return flushed_committed_mrs_;
  }

  void set_aborted() {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK(boost::none == commit_timestamp_);
    aborted_ = true;
  }
  void set_commit_timestamp(Timestamp commit_ts) {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK(boost::none == commit_timestamp_);
    CHECK(!aborted_);
    CHECK(boost::none != commit_mvcc_op_timestamp_);
    commit_timestamp_ = commit_ts;
  }
  void set_commit_mvcc_op_timestamp(Timestamp op_ts) {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK(boost::none == commit_timestamp_);
    commit_mvcc_op_timestamp_ = op_ts;
  }

  bool aborted() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return aborted_;
  }
  boost::optional<Timestamp> commit_timestamp() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return commit_timestamp_;
  }
  boost::optional<Timestamp> commit_mvcc_op_timestamp() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return commit_mvcc_op_timestamp_;
  }

  void GetTimestamps(boost::optional<Timestamp>* op_ts,
                     boost::optional<Timestamp>* commit_ts) const {
    std::lock_guard<simple_spinlock> l(lock_);
    *op_ts = commit_mvcc_op_timestamp_;
    *commit_ts = commit_timestamp_;
  }

 private:
  friend class RefCountedThreadSafe<TxnMetadata>;
  ~TxnMetadata() = default;

  // Protects access to all members below.
  mutable simple_spinlock lock_;
  bool aborted_;

  // If the MVCC op with this timestamp is considered applied, either
  // 'aborted_' is set to true, or 'commit_timestamp_' is set.
  boost::optional<Timestamp> commit_mvcc_op_timestamp_;

  boost::optional<Timestamp> commit_timestamp_;

  bool flushed_committed_mrs_;
};

} // namespace tablet
} // namespace kudu
