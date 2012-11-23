// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAFILE_H
#define KUDU_TABLET_DELTAFILE_H

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "common/columnblock.h"
#include "common/schema.h"
#include "tablet/deltamemstore.h"

namespace kudu {

class WritableFile;
class RandomAccessFile;
class Env;

namespace cfile {
class Writer;
class CFileReader;
}

namespace tablet {

using std::tr1::shared_ptr;

class DeltaFileWriter : boost::noncopyable {
public:
  explicit DeltaFileWriter(const Schema &schema,
                           const shared_ptr<WritableFile> &file);

  Status Start();
  Status Finish();

  Status AppendDelta(uint32_t row_idx, const RowDelta &delta);

private:
  const Schema schema_;

  scoped_ptr<cfile::Writer> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  // The index of the previously written row.
  // This is used in debug mode to make sure that rows are appended
  // in order.
  uint32_t last_row_idx_;
  #endif
};


class DeltaFileReader : boost::noncopyable {
public:
  // Open the Delta File at the given path.
  static Status Open(Env *env, const string &path,
                     const Schema &schema,
                     DeltaFileReader **reader);

  Status ApplyUpdates(size_t col_idx, uint32_t start_row, ColumnBlock *dst) const;

private:
  DeltaFileReader(const shared_ptr<RandomAccessFile> &file,
                  uint64_t file_size,
                  const Schema &schema);

  Status Init();
  Status ApplyEncodedDelta(const Slice &s, size_t col_idx, 
                           uint32_t start_row, ColumnBlock *dst,
                           bool *done) const;

  scoped_ptr<cfile::CFileReader> reader_;
  const Schema schema_;
};

} // namespace tablet
} // namespace kudu

#endif
