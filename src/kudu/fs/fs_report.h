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

#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/fs/block_id.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace fs {

// Checks for blocks that are referenced by live tablets but missing from the
// block manager.
//
// Error type: fatal and irreparable.
struct MissingBlockCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const MissingBlockCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    Entry(BlockId b, std::string t);
    BlockId block_id;
    std::string tablet_id;
  };
  std::vector<Entry> entries;
};

// Checks for blocks that are referenced by the block manager but not from any
// live tablets.
//
// Error type: non-fatal and repairable (by deleting the blocks).
struct OrphanedBlockCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const OrphanedBlockCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    Entry(BlockId b, int64_t l);
    BlockId block_id;
    int64_t length;
    bool repaired;
  };
  std::vector<Entry> entries;
};

// Checks for LBM containers that are full but have extra space somewhere. It
// may be past the end (e.g. leftover preallocated space) or inside (e.g. an
// unpunched hole).
//
// Error type: non-fatal and repairable (by punching out the holes again and
// truncating the container data files).
struct LBMFullContainerSpaceCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const LBMFullContainerSpaceCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    Entry(std::string c, int64_t e);
    std::string container;
    int64_t excess_bytes;
    bool repaired;
  };
  std::vector<Entry> entries;
};

// Checks for LBM containers where one of the two files in the file pair are
// missing, or where the metadata files are too short to contain even a header.
//
// Error type: non-fatal and repairable (by deleting the container files).
struct LBMIncompleteContainerCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const LBMIncompleteContainerCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    explicit Entry(std::string c);
    std::string container;
    bool repaired;
  };
  std::vector<Entry> entries;
};

// Checks for LBM metadata records that are malformed in some way.
//
// Error type: fatal and irreparable.
struct LBMMalformedRecordCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const LBMMalformedRecordCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    // Note: the BlockRecordPB is passed by pointer so that it can be swapped
    // into the entry.
    Entry(std::string c, BlockRecordPB* r);
    std::string container;
    BlockRecordPB record;
  };
  std::vector<Entry> entries;
};

// Checks for LBM data blocks that aren't properly aligned along filesystem
// block size boundaries.
//
// Error type: non-fatal and irreparable.
struct LBMMisalignedBlockCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const LBMMisalignedBlockCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    Entry(std::string c, BlockId b);
    std::string container;
    BlockId block_id;
  };
  std::vector<Entry> entries;
};

// Checks for partial LBM metadata records at the end of container files.
//
// Error type: fatal and repairable (by truncating the container metadata files).
struct LBMPartialRecordCheck {

  // Merges the contents of another check into this one.
  void MergeFrom(const LBMPartialRecordCheck& other);

  // Returns a multi-line string representation of this check.
  std::string ToString() const;

  struct Entry {
    Entry(std::string c, int64_t o);
    std::string container;
    int64_t offset;
    bool repaired;
  };
  std::vector<Entry> entries;
};

// Results of a Kudu filesystem-wide check. The report contains general
// statistics about the filesystem as well as a series of "checks" that
// describe possible on-disk inconsistencies.
//
// A check is a list of occurrences of a particular inconsistency, each
// annotated with relevant information (i.e. an entry for a missing block will
// include that block's ID). The contents of a check can be converted to a
// string for logging, and can be merged with another check of the same type
// for aggregation. Each check is encapsulated in a boost::optional to
// emphasize that, depending on the context, it may not have been performed.
//
// Inconsistencies fall into one of these categories:
// - Fatal and irreparable. The block manager cannot repair or work around
//   these inconsistencies. Just one will cause the *FatalErrors() functions
//   to return an error.
// - Fatal and repairable. The block manager must repair these inconsistencies
//   in order to function properly.
// - Non-fatal and irreparable. These are "interesting" inconsistencies that
//   cannot be repaired but may be worked around or safely ignored.
// - Non-fatal and repairable. These inconsistencies may be repaired
//   opportunistically, but can also be ignored or worked around.
struct FsReport {

  // Merges the contents of another FsReport into this one.
  void MergeFrom(const FsReport& other);

  // Returns a multi-line string representation of this report, including all
  // performed checks (skipped checks will be listed as such).
  //
  // Inconsistencies that are both fatal and irreparable are detailed in full
  // while others are aggregated for brevity.
  std::string ToString() const;

  // Returns whether this report describes at least one fatal and irreparable
  // inconsistency.
  bool HasFatalErrors() const;

  // Like HasFatalErrors(), but returns a Status::Corruption() instead.
  //
  // Useful for RETURN_NOT_OK().
  Status CheckForFatalErrors() const;

  // Like CheckForFatalErrors(), but also writes the report to LOG(INFO).
  Status LogAndCheckForFatalErrors() const;

  // Like CheckForFatalErrors(), but also writes the report to stdout.
  Status PrintAndCheckForFatalErrors() const;

  // General statistics about the block manager.
  struct Stats {

    // Merges the contents of another Stats into this one.
    void MergeFrom(const Stats& other);

    // Returns a multi-line string representation of the stats.
    std::string ToString() const;

    // Number of live (i.e. not yet deleted) data blocks.
    int64_t live_block_count = 0;

    // Total space usage of all live data blocks.
    int64_t live_block_bytes = 0;

    // Total space usage of all live data blocks after accounting for any block
    // manager alignment requirements. Guaranteed to be >= 'live_block_bytes'.
    // Useful for calculating LBM external fragmentation.
    int64_t live_block_bytes_aligned = 0;

    // Total number of LBM containers.
    int64_t lbm_container_count = 0;

    // Total number of full LBM containers.
    int64_t lbm_full_container_count = 0;
  };
  Stats stats;

  // Data directories described by this report.
  std::vector<std::string> data_dirs;

  // General inconsistency checks.
  boost::optional<MissingBlockCheck> missing_block_check;
  boost::optional<OrphanedBlockCheck> orphaned_block_check;

  // LBM-specific inconsistency checks.
  boost::optional<LBMFullContainerSpaceCheck> full_container_space_check;
  boost::optional<LBMIncompleteContainerCheck> incomplete_container_check;
  boost::optional<LBMMalformedRecordCheck> malformed_record_check;
  boost::optional<LBMMisalignedBlockCheck> misaligned_block_check;
  boost::optional<LBMPartialRecordCheck> partial_record_check;
};

} // namespace fs
} // namespace kudu
