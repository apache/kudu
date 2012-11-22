// Copyright (c) 2012, Cloudera, inc.

#include "cfile/cfile.h"
#include "tablet/deltafile.h"
#include "util/coding-inl.h"
#include "util/env.h"
#include "util/hexdump.h"

namespace kudu {
namespace tablet {

DeltaFileWriter::DeltaFileWriter(const Schema &schema,
                                 const shared_ptr<WritableFile> &file) :
  schema_(schema)
#ifndef NDEBUG
  ,last_row_idx_(~0)
#endif
{
  cfile::WriterOptions opts;
  opts.write_validx = true;
  writer_.reset(new cfile::Writer(opts, STRING, cfile::PREFIX, file));
}


Status DeltaFileWriter::Start() {
  return writer_->Start();
}

Status DeltaFileWriter::Finish() {
  return writer_->Finish();
}

Status DeltaFileWriter::AppendDelta(
  uint32_t row_idx, const RowDelta &delta) {

#ifndef NDEBUG
  // Sanity check insertion order in debug mode.
  if (last_row_idx_ != ~0) {
    DCHECK_GT(row_idx, last_row_idx_) <<
      "must insert deltas in sorted order!";
  }
  last_row_idx_ = row_idx;
#endif

  tmp_buf_.clear();
  InlinePutFixed32(&tmp_buf_, row_idx);
  delta.SerializeToBuffer(schema_, &tmp_buf_);
  VLOG(1) << "Encoded delta for row " << row_idx << ":" << std::endl
          << HexDump(tmp_buf_);

  Slice data_slice(tmp_buf_);
  return writer_->AppendEntries(&data_slice, 1);
}

} // namespace tablet
} // namespace kudu
