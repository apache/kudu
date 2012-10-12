// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_CFILE_READER_H
#define KUDU_CFILE_CFILE_READER_H

#include <boost/scoped_ptr.hpp>
#include <tr1/memory>

#include "util/status.h"

namespace kudu {

class RandomAccessFile;

namespace cfile {

class CFileHeaderPB;
class CFileFooterPB;

using boost::scoped_ptr;
using std::tr1::shared_ptr;


struct ReaderOptions {
};

class CFileReader {
public:
  CFileReader(const ReaderOptions &options,
              const shared_ptr<RandomAccessFile> &file,
              uint64_t file_size) :
    options_(options),
    file_(file),
    file_size_(file_size),
    state_(kUninitialized) {
  }

  Status Init();

private:
  Status ReadMagicAndLength(uint64_t offset, uint32_t *len);
  Status ReadAndParseHeader();
  Status ReadAndParseFooter();

  const ReaderOptions options_;
  const shared_ptr<RandomAccessFile> file_;
  const uint64_t file_size_;

  enum State {
    kUninitialized,
    kInitialized
  };
  State state_;

  scoped_ptr<CFileHeaderPB> header_;
  scoped_ptr<CFileFooterPB> footer_;
};


} // namespace cfile
} // namespace kudu

#endif
