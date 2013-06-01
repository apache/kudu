// Copyright (c) 2012, Cloudera, inc

#ifndef KUDU_CFILE_CFILE_H
#define KUDU_CFILE_CFILE_H

#include <boost/utility.hpp>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <stdint.h>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "cfile/block_encodings.h"
#include "cfile/block_compression.h"
#include "cfile/cfile.pb.h"
#include "common/types.h"
#include "gutil/gscoped_ptr.h"
#include "util/status.h"

namespace kudu {

class WritableFile;

namespace cfile {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using google::protobuf::RepeatedPtrField;

class BlockPointer;
class BTreeInfoPB;
class GVIntBlockBuilder;
class StringPrefixBlockBuilder;
class IndexTreeBuilder;

// Magic used in header/footer
extern const char kMagicString[];

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

  // Approximate size of index blocks.
  //
  // Default: 32KB.
  size_t index_block_size;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.
  //
  // This is currently only used by StringPrefixBlockBuilder
  //
  // Default: 16
  int block_restart_interval;

  // Whether the file needs a positional index.
  bool write_posidx;

  // Whether the file needs a value index
  bool write_validx;

  // Block compression codec type
  //
  // Default: specified by --cfile_default_compression_codec
  CompressionType compression;

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

  bool finished() {
    return state_ == kWriterFinished;
  }

  // Add a key-value pair of metadata to the file. Keys should be human-readable,
  // values may be arbitrary binary.
  //
  // If this is called prior to Start(), then the metadata pairs will be added in
  // the header. Otherwise, the pairs will be added in the footer during Finish().
  void AddMetadataPair(const Slice &key, const Slice &value);

  // Append a set of values to the file.
  Status AppendEntries(const void *entries, size_t count);

  // Append a raw block to the file, adding it to the various indexes.
  //
  // The Slices in 'data_slices' are concatenated to form the block.
  //
  // validx_key may be NULL if this file writer has not been configured with
  // value indexing.
  Status AppendRawBlock(const vector<Slice> &data_slices,
                        size_t ordinal_pos,
                        const void *validx_key,
                        const char *name_for_log);

  ~Writer();

private:
  friend class IndexTreeBuilder;

  // Append the given block into the file.
  //
  // Sets *block_ptr to correspond to the newly inserted block.
  Status AddBlock(const vector<Slice> &data_slices,
                  BlockPointer *block_ptr,
                  const char *name_for_log);

  Status WriteRawData(const Slice& data);

  Status FinishCurDataBlock();

  // Flush the current unflushed_metadata_ entries into the given protobuf
  // field, clearing the buffer.
  void FlushMetadataToPB(RepeatedPtrField<FileMetadataPairPB> *field);

  Status CreateBlockBuilder(BlockBuilder **builder) const;

  // File being written.
  shared_ptr<WritableFile> file_;

  // Current file offset.
  uint64_t off_;

  // Current number of values that have been appended.
  rowid_t value_count_;

  WriterOptions options_;

  // Type of data being written
  DataType datatype_;
  const TypeInfo &typeinfo_;
  EncodingType encoding_type_;

  // Metadata which has been added to the writer but not yet flushed.
  vector<pair<string, string> > unflushed_metadata_;

  gscoped_ptr<BlockBuilder> data_block_;
  gscoped_ptr<IndexTreeBuilder> posidx_builder_;
  gscoped_ptr<IndexTreeBuilder> validx_builder_;
  gscoped_ptr<CompressedBlockBuilder> block_compressor_;

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
