// Copyright (c) 2012, Cloudera, inc.

#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include "common/schema.h"
#include "cfile/cfile.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/status.h"

namespace kudu { namespace tablet {

using cfile::CFileReader;
using cfile::ReaderOptions;
using std::auto_ptr;
using std::string;
using std::tr1::shared_ptr;

// Return the path at which the given column's cfile
// is stored within the layer directory.
static string GetColumnPath(const string &dir,
                            int col_idx) {
  return dir + "/col_" + boost::lexical_cast<string>(col_idx);
}

Status LayerWriter::Open() {
  CHECK(cfile_writers_.empty());

  // Create the directory for the new layer
  RETURN_NOT_OK(env_->CreateDir(dir_));

  // TODO: allow options to be configured, perhaps on a per-column
  // basis as part of the schema. For now use defaults.
  //
  // Also would be able to set encoding here, or do something smart
  // to figure out the encoding on the fly.
  cfile::WriterOptions opts;

  for (int i = 0; i < schema_.num_columns(); i++) {
    const ColumnSchema &col = schema_.column(i);

    string path = GetColumnPath(dir_, i);

    // Open file for write.
    WritableFile *out;
    Status s = env_->NewWritableFile(path, &out);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open output file for column " <<
        col.ToString() << " at path " << path << ": " << 
        s.ToString();
      return s;
    }

    // Construct a shared_ptr so that, if the writer construction
    // fails, we don't leak the file descriptor.
    shared_ptr<WritableFile> out_shared(out);

    // Create the CFile writer itself.
    std::auto_ptr<cfile::Writer> writer(new cfile::Writer(
                                          opts,
                                          col.type_info().type(),
                                          cfile::GetDefaultEncoding(col.type_info().type()),
                                          out_shared));

    s = writer->Start();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to Start() writer for column " <<
        col.ToString() << " at path " << path << ": " << 
        s.ToString();
      return s;
    }

    LOG(INFO) << "Opened CFile writer for column " <<
      col.ToString() << " at path " << path;
    cfile_writers_.push_back(writer.release());
  }

  return Status::OK();
}

Status LayerWriter::Finish() {
  for (int i = 0; i < schema_.num_columns(); i++) {
    RETURN_NOT_OK(cfile_writers_[i].Finish());
  }
  return Status::OK();
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status LayerReader::Open() {
  CHECK(!open_) << "Already open!";
  CHECK(cfile_readers_.empty()) << "Invalid state: should have no readers";

  // TODO: somehow pass reader options in schema
  ReaderOptions opts;
  for (int i = 0; i < schema_.num_columns(); i++) {
    string path = GetColumnPath(dir_, i);

    RandomAccessFile *raf_ptr;
    Status s = env_->NewRandomAccessFile(path, &raf_ptr);
    if (!s.ok()) {
      LOG(WARNING) << "Could not open cfile at path "
                   << path << ": " << s.ToString();
      return s;
    }
    shared_ptr<RandomAccessFile> raf(raf_ptr);

    uint64_t file_size;
    s = env_->GetFileSize(path, &file_size);
    if (!s.ok()) {
      LOG(WARNING) << "Could not get cfile length at path "
                   << path << ": " << s.ToString();
      return s;
    }

    auto_ptr<CFileReader> reader(
      new CFileReader(opts, raf, file_size));
    s = reader->Init();
    if (!s.ok()) {
      LOG(WARNING) << "Failed to Init() cfile reader for "
                   << path << ": " << s.ToString();
      return s;
    }

    cfile_readers_.push_back(reader.release());
    LOG(INFO) << "Successfully opened cfile for column " <<
      schema_.column(i).ToString() << " at " << path;
  }

  open_ = true;
  return Status::OK();
}


LayerReader::RowIterator *LayerReader::NewRowIterator(const Schema &projection) const {
  return new RowIterator(this, projection);
}

Status LayerReader::NewColumnIterator(size_t col_idx, CFileIterator **iter) const {
  CHECK(open_);
  CHECK_LT(col_idx, cfile_readers_.size());

  return cfile_readers_[col_idx].NewIterator(iter);
}


Status LayerReader::RowIterator::Init() {
  CHECK(!initted_);

  RETURN_NOT_OK(projection_.GetProjectionFrom(
                  reader_->schema(), &projection_mapping_));

  // Setup Key Iterator.

  // Only support single key column for now.
  CHECK_EQ(reader_->schema().num_key_columns(), 1);
  int key_col = 0;

  CFileIterator *iter;
  RETURN_NOT_OK(reader_->NewColumnIterator(key_col, &iter));
  key_iter_.reset(iter);

  // Setup column iterators.

  for (size_t i = 0; i < projection_.num_columns(); i++) {
    size_t col_in_layer = projection_mapping_[i];

    CFileIterator *iter;
    RETURN_NOT_OK(reader_->NewColumnIterator(col_in_layer, &iter));
    col_iters_.push_back(iter);
  }

  initted_ = true;
  return Status::OK();
}

Status LayerReader::RowIterator::CopyNextRows(
  size_t *nrows, char *dst, Arena *dst_arena)
{
  DCHECK(initted_);
  DCHECK(dst) << "null dst";
  DCHECK(dst_arena) << "null dst_arena";

  // Copy the projected columns into 'dst'
  size_t stride = projection_.byte_size();
  char *ptr = dst;
  int proj_idx = 0;

  int fetched_prev_col = -1;

  BOOST_FOREACH(CFileIterator &col_iter, col_iters_) {

    size_t fetched = *nrows;
    RETURN_NOT_OK(col_iter.CopyNextValuesStrided(
                    &fetched, ptr, stride, dst_arena));

    if (proj_idx > 0) {
      CHECK(fetched == fetched_prev_col) <<
        "Column " << proj_idx << " only fetched "
                  << fetched << " rows whereas the previous "
                  << "columns fetched " << fetched_prev_col;
    }
    fetched_prev_col = fetched;

    if (fetched == 0) {
      DCHECK_EQ(proj_idx, 0) << "all columns should end at the same time!";
      return Status::NotFound("end of input");
    }

    const TypeInfo &tinfo = projection_.column(proj_idx).type_info();
    ptr += tinfo.size();
    proj_idx++;
  }

  *nrows = fetched_prev_col;
  return Status::OK();
}


} // namespace tablet
} // namespace kudu
