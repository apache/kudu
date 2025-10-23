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

#include "kudu/tablet/multi_column_writer.h"

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"

using kudu::cfile::CFileWriter;
using kudu::fs::BlockCreationTransaction;
using kudu::fs::CreateBlockOptions;
using kudu::fs::WritableBlock;
using std::map;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace tablet {

MultiColumnWriter::MultiColumnWriter(FsManager* fs,
                                     const Schema* schema,
                                     string tablet_id)
    : fs_(fs),
      schema_(DCHECK_NOTNULL(schema)),
      tablet_id_(std::move(tablet_id)),
      open_(false),
      finished_(false) {
  cfile_writers_.reserve(schema_->num_columns());
  block_ids_.reserve(schema_->num_columns());
}

Status MultiColumnWriter::Open() {
  DCHECK(!open_) << "already open";
  DCHECK(cfile_writers_.empty()); // this method isn't re-entrant after failures

  // Open columns.
  const CreateBlockOptions block_opts({ tablet_id_ });
  for (auto i = 0; i < schema_->num_columns(); ++i) {
    const auto& col = schema_->column(i);

    cfile::WriterOptions opts;

    // Index all columns by ordinal position, so we can match up
    // the corresponding rows.
    opts.write_posidx = true;

    // Set the column storage attributes.
    opts.storage_attributes = col.attributes();

    // If the schema has a single PK and this is the PK col
    if (i == 0 && schema_->num_key_columns() == 1) {
      opts.write_validx = true;
    }

    // Open file for writing.
    unique_ptr<WritableBlock> block;
    RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(block_opts, &block),
        Substitute("tablet $0: unable to open output file for column $1",
                   tablet_id_, col.ToString()));
    BlockId block_id(block->id());

    // Create the CFile writer itself.
    unique_ptr<CFileWriter> writer(new CFileWriter(std::move(opts),
                                                   col.type_info(),
                                                   col.is_nullable(),
                                                   std::move(block)));
    RETURN_NOT_OK_PREPEND(writer->Start(),
        Substitute("tablet $0: unable to start writer for column $1",
                   tablet_id_, col.ToString()));
    cfile_writers_.emplace_back(std::move(writer));
    block_ids_.emplace_back(block_id);
  }
  open_ = true;
  VLOG(1) << Substitute("Opened CFile writers for $0 column(s)",
                        cfile_writers_.size());

  return Status::OK();
}

Status MultiColumnWriter::AppendBlock(const RowBlock& block) {
  DCHECK(open_);
  for (auto i = 0; i < schema_->num_columns(); ++i) {
    ColumnBlock column = block.column_block(i);
    if (column.type_info()->is_array()) {
      RETURN_NOT_OK(cfile_writers_[i]->AppendNullableArrayEntries(
          column.non_null_bitmap(), column.data(), column.nrows()));
    } else {
      if (column.is_nullable()) {
        RETURN_NOT_OK(cfile_writers_[i]->AppendNullableEntries(
            column.non_null_bitmap(), column.data(), column.nrows()));
      } else {
        RETURN_NOT_OK(cfile_writers_[i]->AppendEntries(column.data(), column.nrows()));
      }
    }
  }
  return Status::OK();
}

Status MultiColumnWriter::FinishAndReleaseBlocks(
    BlockCreationTransaction* transaction) {
  DCHECK(open_);
  DCHECK(!finished_);
  for (auto i = 0; i < schema_->num_columns(); ++i) {
    auto s = cfile_writers_[i]->FinishAndReleaseBlock(transaction);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << Substitute(
          "tablet $0: unable to finialize writer for column $1",
          tablet_id_, schema_->column(i).ToString());
      return s;
    }
  }
  finished_ = true;
  return Status::OK();
}

void MultiColumnWriter::GetFlushedBlocksByColumnId(map<ColumnId, BlockId>* ret) const {
  DCHECK(finished_);
  auto& r = *ret;
  r.clear();
  for (auto i = 0; i < schema_->num_columns(); ++i) {
    r[schema_->column_id(i)] = block_ids_[i];
  }
}

size_t MultiColumnWriter::written_size() const {
  DCHECK(open_);
  size_t size = 0;
  for (const auto& writer: cfile_writers_) {
    size += writer->written_size();
  }
  return size;
}

} // namespace tablet
} // namespace kudu
