// Copyright (c) 2012, Cloudera, inc

#ifndef KUDU_CFILE_CFILE_H
#define KUDU_CFILE_CFILE_H

#include <boost/scoped_ptr.hpp>
#include <boost/utility.hpp>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <stdint.h>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "cfile/block_encodings.h"
#include "cfile/cfile.pb.h"
#include "common/types.h"
#include "util/status.h"

namespace kudu {

class WritableFile;

namespace cfile {

using std::string;
using std::tr1::shared_ptr;
using boost::scoped_ptr;
using std::vector;

typedef uint32_t OrdinalIndex;

class BlockPointer;
class BTreeInfoPB;
class IntBlockBuilder;
class StringBlockBuilder;
class IndexTreeBuilder;

// Magic used in header/footer
extern const string kMagicString;

const int kCFileMajorVersion = 1;
const int kCFileMinorVersion = 0;


struct WriterOptions {
  // Approximate size of user data packed per block.  Note that the
  // block size specified here corresponds to uncompressed data.  The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled.  This parameter can be changed dynamically.
  //
  // Default: 256K
  size_t block_size;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.
  //
  // This is currently only used by StringBlockBuilder
  //
  // Default: 16
  int block_restart_interval;

  // Whether the file needs a positional index.
  bool write_posidx;

  // Whether the file needs a value index
  bool write_validx;

  WriterOptions();
};

// Main class used to write a CFile.
class Writer : boost::noncopyable {
public:
  explicit Writer(const WriterOptions &options,
                  DataType type,
                  EncodingType encoding,
                  shared_ptr<WritableFile> file);
  Status Start();
  Status Finish();

  // Append a set of values to the file.
  Status AppendEntries(const void *entries, int count);

  ~Writer();

private:
  friend class IndexTreeBuilder;

  // Append the given block into the file. Returns
  // the offset in the file at which it was stored
  Status AddBlock(const Slice &data, uint64_t *offset_out,
                  const char *name_for_log);


  Status FinishCurDataBlock();


  Status CreateBlockBuilder(BlockBuilder **builder) const;

  // File being written.
  shared_ptr<WritableFile> file_;

  // Current file offset.
  uint64_t off_;

  // Current number of values that have been appended.
  int value_count_;

  WriterOptions options_;

  // Type of data being written
  DataType datatype_;
  const TypeInfo &typeinfo_;
  EncodingType encoding_type_;

  scoped_ptr<BlockBuilder> data_block_;
  scoped_ptr<IndexTreeBuilder> posidx_builder_;
  scoped_ptr<IndexTreeBuilder> validx_builder_;

  enum State {
    kWriterInitialized,
    kWriterWriting,
    kWriterFinished
  };
  State state_;
};


} // namespace cfile
} // namespace kudu

#endif
