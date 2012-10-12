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
#include "util/status.h"
#include "int_block.h"

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
class BTreeMetaPB;
class TreeBuilder;
class IntBlockBuilder;
class StringBlockBuilder;

template <class KeyType> class IndexTreeBuilder;

// Magic used in header/footer
extern const string kMagicString;
extern const string kPositionalIndexIdentifier;

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

  WriterOptions();
};

// Main class used to write a CFile.
class Writer : boost::noncopyable {
public:
  explicit Writer(const WriterOptions &options,
                  shared_ptr<WritableFile> file);
  Status Start();
  Status Finish();

  Status AddTree(const BTreeMetaPB &metadata,
                 shared_ptr<TreeBuilder> *tree_out);

  ~Writer();


private:
  friend class TreeBuilder;

  template <class K>
  friend class IndexTreeBuilder;

  // Append the given block into the file. Returns
  // the offset in the file at which it was stored
  Status AddBlock(const Slice &data, uint64_t *offset_out,
                  const string &name_for_log);

  // Map of TreeBuilders which have been opened, keyed by identifier.
  std::tr1::unordered_map<string, shared_ptr<TreeBuilder> > trees_;

  // File being written.
  shared_ptr<WritableFile> file_;

  // Current file offset.
  uint64_t off_;

  WriterOptions options_;

  enum State {
    kWriterInitialized,
    kWriterWriting,
    kWriterFinished
  };
  State state_;
};



class StringBlockBuilder : boost::noncopyable {
public:
  explicit StringBlockBuilder(const WriterOptions *options);

  void Add(const Slice &val);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish();

  void Reset();

  // Return an estimate of the number
  uint64_t EstimateEncodedSize() const;

  size_t Count() const;
private:
  string buffer_;
  string last_val_;

  vector<uint32_t> restarts_;    // Restart points
  int counter_;
  bool finished_;

  const WriterOptions *options_;
};


class TreeBuilder : boost::noncopyable {
public:
  TreeBuilder(const WriterOptions *options,
              Writer *writer);

  Status Append(IntType val);
  Status Finish(BTreeInfoPB *info);

private:
  // TODO: inconsistent "value block" vs "data block"
  Status FinishCurValueBlock();

  const WriterOptions *options_;
  Writer *writer_;

  IntBlockBuilder value_block_;
  scoped_ptr<IndexTreeBuilder<uint32_t> > posidx_builder_;

  int value_count_;
};

} // namespace cfile
} // namespace kudu

#endif
