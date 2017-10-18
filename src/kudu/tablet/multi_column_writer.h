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
#ifndef KUDU_TABLET_MULTI_COLUMN_WRITER_H
#define KUDU_TABLET_MULTI_COLUMN_WRITER_H

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/fs/block_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;
class RowBlock;
class Schema;
struct ColumnId;

namespace cfile {
class CFileWriter;
} // namespace cfile

namespace fs {
class BlockCreationTransaction;
} // namespace fs

namespace tablet {

// Wrapper which writes several columns in parallel corresponding to some
// Schema. Written blocks will fall in the tablet_id's data dir group.
class MultiColumnWriter {
 public:
  MultiColumnWriter(FsManager* fs,
                    const Schema* schema,
                    std::string tablet_id);

  virtual ~MultiColumnWriter();

  // Open and start writing the columns.
  Status Open();

  // Append the given block to the output columns.
  //
  // Note that the selection vector here is ignored.
  Status AppendBlock(const RowBlock& block);

  // Close the in-progress CFiles, finalizing the underlying writable
  // blocks and releasing them to 'transaction'.
  Status FinishAndReleaseBlocks(fs::BlockCreationTransaction* transaction);

  // Return the number of bytes written so far.
  size_t written_size() const;

  cfile::CFileWriter* writer_for_col_idx(int i) {
    DCHECK_LT(i, cfile_writers_.size());
    return cfile_writers_[i];
  }

  // Return the block IDs of the written columns, keyed by column ID.
  //
  // REQUIRES: Finish() already called.
  void GetFlushedBlocksByColumnId(std::map<ColumnId, BlockId>* ret) const;

 private:
  FsManager* const fs_;
  const Schema* const schema_;

  bool finished_;

  const std::string tablet_id_;

  std::vector<cfile::CFileWriter *> cfile_writers_;
  std::vector<BlockId> block_ids_;

  DISALLOW_COPY_AND_ASSIGN(MultiColumnWriter);
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_MULTI_COLUMN_WRITER_H */
