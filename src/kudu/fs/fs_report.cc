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
#include "kudu/fs/fs_report.h"

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace fs {

using std::cout;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;

///////////////////////////////////////////////////////////////////////////////
// MissingBlockCheck
///////////////////////////////////////////////////////////////////////////////

void MissingBlockCheck::MergeFrom(const MissingBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string MissingBlockCheck::ToString() const {
  // Missing blocks are fatal so the IDs are logged in their entirety to ease
  // troubleshooting.
  //
  // Aggregate missing blocks across tablets.
  unordered_map<string, vector<string>> missing_blocks_by_tablet_id;
  for (const auto& mb : entries) {
    missing_blocks_by_tablet_id[mb.tablet_id].emplace_back(
        mb.block_id.ToString());
  }

  // Add the summary.
  string s = Substitute("Total missing blocks: $0\n", entries.size());

  // Add an entry for each tablet.
  for (const auto& e : missing_blocks_by_tablet_id) {
    SubstituteAndAppend(&s, "Fatal error: tablet $0 missing blocks: [ $1 ]\n",
                        e.first, JoinStrings(e.second, ", "));
  }

  return s;
}

MissingBlockCheck::Entry::Entry(BlockId b, string t)
    : block_id(b),
      tablet_id(std::move(t)) {
}

///////////////////////////////////////////////////////////////////////////////
// OrphanedBlockCheck
///////////////////////////////////////////////////////////////////////////////

void OrphanedBlockCheck::MergeFrom(const OrphanedBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string OrphanedBlockCheck::ToString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t orphaned_block_count_repaired = 0;
  int64_t orphaned_block_bytes = 0;
  int64_t orphaned_block_bytes_repaired = 0;
  for (const auto& ob : entries) {
    if (ob.repaired) {
      orphaned_block_count_repaired++;
    }
    orphaned_block_bytes += ob.length;
    if (ob.repaired) {
      orphaned_block_bytes_repaired += ob.length;
    }
  }

  return Substitute(
      "Total orphaned blocks: $0 ($1 repaired)\n"
      "Total orphaned block bytes: $2 ($3 repaired)\n",
      entries.size(), orphaned_block_count_repaired,
      orphaned_block_bytes, orphaned_block_bytes_repaired);
}

OrphanedBlockCheck::Entry::Entry(BlockId b, int64_t l)
    : block_id(b),
      length(l),
      repaired(false) {
}

///////////////////////////////////////////////////////////////////////////////
// LBMFullContainerSpaceCheck
///////////////////////////////////////////////////////////////////////////////

void LBMFullContainerSpaceCheck::MergeFrom(
    const LBMFullContainerSpaceCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LBMFullContainerSpaceCheck::ToString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t full_container_space_count_repaired = 0;
  int64_t full_container_space_bytes = 0;
  int64_t full_container_space_bytes_repaired = 0;
  for (const auto& fcp : entries) {
    if (fcp.repaired) {
      full_container_space_count_repaired++;
    }
    full_container_space_bytes += fcp.excess_bytes;
    if (fcp.repaired) {
      full_container_space_bytes_repaired += fcp.excess_bytes;
    }
  }

  return Substitute(
      "Total full LBM containers with extra space: $0 ($1 repaired)\n"
      "Total full LBM container extra space in bytes: $2 ($3 repaired)\n",
      entries.size(), full_container_space_count_repaired,
      full_container_space_bytes, full_container_space_bytes_repaired);
}

LBMFullContainerSpaceCheck::Entry::Entry(string c, int64_t e)
    : container(std::move(c)),
      excess_bytes(e),
      repaired(false) {
}

///////////////////////////////////////////////////////////////////////////////
// LBMIncompleteContainerCheck
///////////////////////////////////////////////////////////////////////////////

void LBMIncompleteContainerCheck::MergeFrom(
    const LBMIncompleteContainerCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LBMIncompleteContainerCheck::ToString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t incomplete_container_count_repaired = 0;
  for (const auto& ic : entries) {
    if (ic.repaired) {
      incomplete_container_count_repaired++;
    }
  }

  return Substitute("Total incomplete LBM containers: $0 ($1 repaired)\n",
                    entries.size(), incomplete_container_count_repaired);
}

LBMIncompleteContainerCheck::Entry::Entry(string c)
    : container(std::move(c)),
      repaired(false) {
}

///////////////////////////////////////////////////////////////////////////////
// LBMMalformedRecordCheck
///////////////////////////////////////////////////////////////////////////////

void LBMMalformedRecordCheck::MergeFrom(const LBMMalformedRecordCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LBMMalformedRecordCheck::ToString() const {
  // Malformed records are fatal so they're logged in their entirety to ease
  // troubleshooting.
  string s;
  for (const auto& mr : entries) {
    SubstituteAndAppend(
        &s, "Fatal error: malformed record in container $0: $1\n",
        mr.container, pb_util::SecureDebugString(mr.record));
  }
  return s;
}

LBMMalformedRecordCheck::Entry::Entry(string c, BlockRecordPB* r)
    : container(std::move(c)) {
  record.Swap(r);
}

///////////////////////////////////////////////////////////////////////////////
// LBMMisalignedBlockCheck
///////////////////////////////////////////////////////////////////////////////

void LBMMisalignedBlockCheck::MergeFrom(const LBMMisalignedBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LBMMisalignedBlockCheck::ToString() const {
  // Misaligned blocks should be rare so they're logged in their entirety to
  // ease troubleshooting.
  string s;
  for (const auto& mb : entries) {
    SubstituteAndAppend(&s, "Misaligned block in container $0: $1\n",
                        mb.container, mb.block_id.ToString());
  }
  return s;
}

LBMMisalignedBlockCheck::Entry::Entry(string c, BlockId b)
    : container(std::move(c)),
      block_id(b) {
}

///////////////////////////////////////////////////////////////////////////////
// LBMPartialRecordCheck
///////////////////////////////////////////////////////////////////////////////

void LBMPartialRecordCheck::MergeFrom(
    const LBMPartialRecordCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LBMPartialRecordCheck::ToString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t partial_records_repaired = 0;
  for (const auto& pr : entries) {
    if (pr.repaired) {
      partial_records_repaired++;
    }
  }

  return Substitute("Total LBM partial records: $0 ($1 repaired)\n",
                    entries.size(), partial_records_repaired);
}

LBMPartialRecordCheck::Entry::Entry(string c, int64_t o)
    : container(std::move(c)),
      offset(o),
      repaired(false) {
}

///////////////////////////////////////////////////////////////////////////////
// FsReport::Stats
///////////////////////////////////////////////////////////////////////////////

void FsReport::Stats::MergeFrom(const FsReport::Stats& other) {
  live_block_count += other.live_block_count;
  live_block_bytes += other.live_block_bytes;
  live_block_bytes_aligned += other.live_block_bytes_aligned;
  lbm_container_count += other.lbm_container_count;
  lbm_full_container_count += other.lbm_full_container_count;
}

string FsReport::Stats::ToString() const {
  return Substitute(
      "Total live blocks: $0\n"
      "Total live bytes: $1\n"
      "Total live bytes (after alignment): $2\n"
      "Total number of LBM containers: $3 ($4 full)\n",
      live_block_count, live_block_bytes, live_block_bytes_aligned,
      lbm_container_count, lbm_full_container_count);
}

///////////////////////////////////////////////////////////////////////////////
// FsReport
///////////////////////////////////////////////////////////////////////////////

void FsReport::MergeFrom(const FsReport& other) {
  data_dirs.insert(data_dirs.end(),
                   other.data_dirs.begin(), other.data_dirs.end());

  stats.MergeFrom(other.stats);

#define MERGE_ONE_CHECK(c) \
  if ((c) && other.c) { \
    (c)->MergeFrom(other.c.get()); \
  } else if (other.c) { \
    (c) = other.c; \
  }

  MERGE_ONE_CHECK(missing_block_check);
  MERGE_ONE_CHECK(orphaned_block_check);
  MERGE_ONE_CHECK(full_container_space_check);
  MERGE_ONE_CHECK(incomplete_container_check);
  MERGE_ONE_CHECK(malformed_record_check);
  MERGE_ONE_CHECK(misaligned_block_check);
  MERGE_ONE_CHECK(partial_record_check);

#undef MERGE_ONE_CHECK
}

string FsReport::ToString() const {
  string s;
  s += "Block manager report\n";
  s += "--------------------\n";
  SubstituteAndAppend(&s, "$0 data directories: $1\n", data_dirs.size(),
                      JoinStrings(data_dirs, ", "));
  s += stats.ToString();

#define TOSTRING_ONE_CHECK(c, name) \
  if ((c)) { \
    s += (c)->ToString(); \
  } else { \
    s += "Did not check for " name "\n"; \
  }

  TOSTRING_ONE_CHECK(missing_block_check, "missing blocks");
  TOSTRING_ONE_CHECK(orphaned_block_check, "orphaned blocks");
  TOSTRING_ONE_CHECK(full_container_space_check, "full LBM containers with extra space");
  TOSTRING_ONE_CHECK(incomplete_container_check, "incomplete LBM containers");
  TOSTRING_ONE_CHECK(malformed_record_check, "malformed LBM records");
  TOSTRING_ONE_CHECK(misaligned_block_check, "misaligned LBM blocks");
  TOSTRING_ONE_CHECK(partial_record_check, "partial LBM records");

#undef TOSTRING_ONE_CHECK
  return s;
}

Status FsReport::CheckForFatalErrors() const {
  if (HasFatalErrors()) {
    return Status::Corruption(
        "found at least one fatal error in block manager on-disk state. "
        "See block manager consistency report for details");
  }
  return Status::OK();
}

bool FsReport::HasFatalErrors() const {
  return (missing_block_check && !missing_block_check->entries.empty()) ||
         (malformed_record_check && !malformed_record_check->entries.empty());
}

Status FsReport::LogAndCheckForFatalErrors() const {
  LOG(INFO) << ToString();
  return CheckForFatalErrors();
}

Status FsReport::PrintAndCheckForFatalErrors() const {
  cout << ToString();
  return CheckForFatalErrors();
}

} // namespace fs
} // namespace kudu
