// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_CFILE_READER_H
#define KUDU_CFILE_CFILE_READER_H

#include <boost/noncopyable.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <tr1/memory>
#include <string>

#include "util/status.h"

namespace kudu {

class RandomAccessFile;

namespace cfile {

class CFileHeaderPB;
class CFileFooterPB;

using std::string;
using boost::shared_array;
using boost::scoped_ptr;
using std::tr1::shared_ptr;


struct ReaderOptions {
};

class CFileIterator;
class BlockPointer;

class BlockData {
public:
  BlockData() {}

  BlockData(const Slice &data,
            shared_array<char> data_for_free) :
    data_(data),
    data_for_free_(data_for_free) {
  }

  const Slice &slice() const {
    return data_;
  }

private:
  Slice data_;
  shared_array<char> data_for_free_;
};

class CFileReader : boost::noncopyable {
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

  CFileIterator *NewIterator() const;

  Status ReadBlock(const BlockPointer &ptr,
                   BlockData *ret) const;

  Status SearchPosition(uint32_t pos,
                        BlockPointer *ptr,
                        uint32_t *ret_key);

private:
  Status ReadMagicAndLength(uint64_t offset, uint32_t *len);
  Status ReadAndParseHeader();
  Status ReadAndParseFooter();

  Status GetIndexRootBlock(const string &identifier, BlockPointer *ptr);

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
