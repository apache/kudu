// Copyright (c) 2014, Cloudera, inc.

#include "tablet/multi_column_writer.h"

#include "common/rowblock.h"
#include "common/schema.h"
#include "cfile/cfile.h"
#include "fs/block_id.h"
#include "gutil/stl_util.h"

namespace kudu {
namespace tablet {

MultiColumnWriter::MultiColumnWriter(FsManager* fs,
                                     const Schema* schema)
  : fs_(fs),
    schema_(schema),
    finished_(false) {
}

MultiColumnWriter::~MultiColumnWriter() {
  STLDeleteElements(&cfile_writers_);
}

Status MultiColumnWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Open columns.
  for (int i = 0; i < schema_->num_columns(); i++) {
    const ColumnSchema &col = schema_->column(i);

    // TODO: allow options to be configured, perhaps on a per-column
    // basis as part of the schema. For now use defaults.
    //
    // Also would be able to set encoding here, or do something smart
    // to figure out the encoding on the fly.
    cfile::WriterOptions opts;

    // Index all columns by ordinal position, so we can match up
    // the corresponding rows.
    opts.write_posidx = true;

    /// Set the column storage attributes.
    opts.storage_attributes = col.attributes();

    // If the schema has a single PK and this is the PK col
    if (i == 0 && schema_->num_key_columns() == 1) {
      opts.write_validx = true;
    }

    // Open file for write.
    shared_ptr<WritableFile> data_writer;
    BlockId block_id;
    RETURN_NOT_OK_PREPEND(fs_->CreateNewBlock(&data_writer, &block_id),
                          "Unable to open output file for column " + col.ToString());

    // Create the CFile writer itself.
    gscoped_ptr<cfile::Writer> writer(new cfile::Writer(
                                        opts,
                                        col.type_info()->type(),
                                        col.is_nullable(),
                                        data_writer));
    RETURN_NOT_OK_PREPEND(writer->Start(),
                          "Unable to Start() writer for column " + col.ToString());

    LOG(INFO) << "Opened CFile writer for column " << col.ToString();
    cfile_writers_.push_back(writer.release());
    block_ids_.push_back(block_id);
  }

  return Status::OK();
}

Status MultiColumnWriter::AppendBlock(const RowBlock& block) {
  for (int i = 0; i < schema_->num_columns(); i++) {
    ColumnBlock column = block.column_block(i);
    if (column.is_nullable()) {
      RETURN_NOT_OK(cfile_writers_[i]->AppendNullableEntries(column.null_bitmap(),
          column.data(), column.nrows()));
    } else {
      RETURN_NOT_OK(cfile_writers_[i]->AppendEntries(column.data(), column.nrows()));
    }
  }
  return Status::OK();
}

Status MultiColumnWriter::Finish() {
  CHECK(!finished_);
  for (int i = 0; i < schema_->num_columns(); i++) {
    cfile::Writer *writer = cfile_writers_[i];
    Status s = writer->Finish();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Finish writer for column " <<
        schema_->column(i).ToString() << ": " << s.ToString();
      return s;
    }
  }
  finished_ = true;
  return Status::OK();
}

std::vector<BlockId> MultiColumnWriter::FlushedBlocks() const {
  CHECK(finished_);
  return block_ids_;
}

size_t MultiColumnWriter::written_size() const {
  size_t size = 0;
  BOOST_FOREACH(const cfile::Writer *writer, cfile_writers_) {
    size += writer->written_size();
  }
  return size;
}

} // namespace tablet
} // namespace kudu
