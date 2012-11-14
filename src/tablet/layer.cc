// Copyright (c) 2012, Cloudera, inc.

#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <tr1/memory>

#include "cfile/cfile.h"
#include "tablet/layer.h"
#include "tablet/schema.h"
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
                                          col.type_info().default_encoding(),
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


} // namespace tablet
} // namespace kudu
