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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/env.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class BlockRecordPB;

namespace pb_util {
class WritablePBContainerFile;
} // namespace pb_util

namespace fs {
class DataDirManager;

// Corrupts various log block manager on-disk data structures.
class LBMCorruptor {
 public:
  // Create a LBMCorruptor according to the --block_manager flag.
  static std::unique_ptr<LBMCorruptor> Create(
      Env* env, DataDirManager* dd_manager, uint32_t rand_seed);

  virtual ~LBMCorruptor() = default;

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
  virtual Status CreateIncompleteContainer() = 0;

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

  enum class NonFatalInconsistency : uint32_t {
    kMisalignedBlockToContainer = 0,
    kIncompleteContainer,
    kPreallocateFullContainer,
    kUnpunchedBlockToFullContainer,
    kPartialRecordToContainer,
    kMaxNonFatalInconsistency
  };
  // Injects one of the above non-fatal inconsistencies (chosen at random).
  Status InjectRandomNonFatalInconsistency();

  // Resets the DataDirManager when the bound DataDirManager changes.
  void ResetDataDirManager(DataDirManager* dd_manager) {
    dd_manager_ = dd_manager;
  }

 protected:
  LBMCorruptor(Env* env, DataDirManager* dd_manager, uint32_t rand_seed);

  // Describes an on-disk LBM container.
  struct Container {
    std::string dir;
    std::string name;
    std::string data_filename;
    std::string metadata_filename;
  };

  // Opens the metadata writer belonging to 'container' for additional writing.
  Status OpenMetadataWriter(
      const Container& container,
      std::unique_ptr<pb_util::WritablePBContainerFile>* writer);

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
  std::string GetRandomDataDir() const;

  // Appends a CREATE-DELETE pair of records to the container 'c', the newly
  // created record has a unique id of 'block_id' and is located at
  // 'block_offset' with a size of 'block_length'.
  virtual Status AppendRecord(const Container* c,
                              BlockId block_id,
                              int64_t block_offset,
                              int64_t block_length) = 0;

  // Similar to the above, but only appends the CREATE record.
  virtual Status AppendCreateRecord(const Container* c,
                                    BlockId block_id,
                                    uint64_t block_offset,
                                    int64_t block_length) = 0;

  // Appends a partial CREATE record to the metadata part of container 'c', the
  // newly created record has a unique id of 'block_id'. The record is corrupted
  // by truncating one byte off the end of it.
  virtual Status AppendPartialRecord(const Container* c, BlockId block_id) = 0;

  // Appends a 'record' to the metadata part of container 'c'.
  virtual Status AppendMetadataRecord(const Container* c, const BlockRecordPB& record) = 0;

  // Corrupt the 'record' in some way. Kinds of malformed records (as per the
  // malformed record checking code in log_block_manager.cc).
  enum class MalformedRecordType : uint32_t {
    // All LBMCorruptor derive classes have these kind of errors.
    kNoBlockOffset = 0,
    kNoBlockLength,
    kNegativeBlockOffset,
    kNegativeBlockLength,
    kMetadataSizeLargerThanDataSize,
    kUnrecognizedOpType,
    // Only NativeMetadataLBMCorruptor class has these kind of errors.
    kDeleteWithoutFirstMatchingCreate,
    kTwoCreatesForSameBlockId
  };
  // Returns an error if a container could not be found.
  virtual Status CreateMalformedRecord(const Container* c,
                                       MalformedRecordType error_type,
                                       BlockRecordPB* record);

  Status CreateContainerDataPart(const std::string& unsuffixed_path);

  // Initialized in the constructor.
  Env* env_;
  // Use DataDirManager to access the Dir objects conveniently.
  DataDirManager* dd_manager_;
  mutable Random rand_;
  ObjectIdGenerator oid_generator_;
  MalformedRecordType max_malformed_types_;

  // Initialized in Init().
  std::vector<Container> all_containers_;
  std::vector<Container> full_containers_;

  DISALLOW_COPY_AND_ASSIGN(LBMCorruptor);
};

} // namespace fs
} // namespace kudu
