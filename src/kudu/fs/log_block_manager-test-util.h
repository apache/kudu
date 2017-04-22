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

#include <memory>
#include <string>
#include <vector>

#include "kudu/fs/block_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/env.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

namespace pb_util {
class WritablePBContainerFile;
} // namespace pb_util

namespace fs {

// Corrupts various log block manager on-disk data structures.
class LBMCorruptor {
 public:
  LBMCorruptor(Env* env, std::vector<std::string> data_dirs, uint32_t rand_seed);

  // Initializes a the corruptor, parsing all data directories for containers.
  //
  // Containers created after the call to Init() will not be visible to the
  // corruptor.
  Status Init();

  // Preallocates extra space in a full container (chosen at random). This
  // inconsistency is non-fatal and repairable.
  //
  // Returns an error if a full container could not be found.
  Status PreallocateFullContainer();

  // Adds an "unpunched block" to a full container (chosen at random). An
  // unpunched block is one that has been deleted but whose space was not
  // reclaimed.
  //
  // Returns an error if a container could not be found.
  Status AddUnpunchedBlockToFullContainer();

  // Creates a new incomplete container. This inconsistency is non-fatal and
  // repairable.
  Status CreateIncompleteContainer();

  // Adds a malformed record to a container (chosen at random). This
  // inconsistency is fatal and irreparable.
  //
  // Returns an error if a container could not be found.
  Status AddMalformedRecordToContainer();

  // Adds a misaligned block to a container (chosen at random). The block
  // contains repeated 8-byte sequences of its block id. This inconsistency is
  // non-fatal and irreparable.
  //
  // Returns an error if a container could not be found.
  Status AddMisalignedBlockToContainer();

  // Adds a partial LBM record to a container (chosen at random). This
  // inconsistency is non-fatal and repairable.
  //
  // Once a container has this inconsistency, no future inconsistencies will be
  // added to it.
  //
  // Returns an error if a container could not be found.
  Status AddPartialRecordToContainer();

  // Injects one of the above non-fatal inconsistencies (chosen at random).
  Status InjectRandomNonFatalInconsistency();

 private:
  // Describes an on-disk LBM container.
  struct Container {
    std::string name;
    std::string data_filename;
    std::string metadata_filename;
  };

  // Opens the metadata writer belonging to 'container' for additional writing.
  Status OpenMetadataWriter(
      const Container& container,
      std::unique_ptr<pb_util::WritablePBContainerFile>* writer);

  // Appends a CREATE record to 'writer'.
  static Status AppendCreateRecord(pb_util::WritablePBContainerFile* writer,
                                   BlockId block_id,
                                   int64_t block_offset,
                                   int64_t block_length);

  // Appends a DELETE record to 'writer'.
  static Status AppendDeleteRecord(pb_util::WritablePBContainerFile* writer,
                                   BlockId block_id);

  // Preallocates space at the end of a container's data file for a new block.
  //
  // On success, writes the initial data file's size to 'old_data_file_size'.
  static Status PreallocateForBlock(RWFile* data_file,
                                    RWFile::PreAllocateMode mode,
                                    int64_t block_length,
                                    int64_t* old_data_file_size);

  // Gets a random container subject to the restriction in 'mode'.
  //
  // Returns an error if no such container could be found.
  enum FindContainerMode {
    ANY,
    FULL,
  };
  Status GetRandomContainer(FindContainerMode mode,
                            const Container** container) const;

  // Gets a data directory chosen at random.
  const std::string& GetRandomDataDir() const;

  // Initialized in the constructor.
  Env* env_;
  const std::vector<std::string> data_dirs_;
  mutable Random rand_;
  ObjectIdGenerator oid_generator_;

  // Initialized in Init().
  std::vector<Container> all_containers_;
  std::vector<Container> full_containers_;

  DISALLOW_COPY_AND_ASSIGN(LBMCorruptor);
};

} // namespace fs
} // namespace kudu
