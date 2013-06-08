// Copyright (c) 2013, Cloudera, inc.
//
// Simplistic block encoding for strings.
//
// The block consists of:
// Header:
//   ordinal_pos (32-bit fixed)
//   num_elems (32-bit fixed)
//   offsets_pos (32-bit fixed): position of the first offset, relative to block start
// Strings:
//   raw strings that were written
// Offsets:  [pointed to by offsets_pos]
//   gvint-encoded offsets pointing to the beginning of each string
#ifndef KUDU_CFILE_STRING_PLAIN_BLOCK_H
#define KUDU_CFILE_STRING_PLAIN_BLOCK_H

#include "cfile/block_encodings.h"
#include "util/faststring.h"

namespace kudu {
namespace cfile {

struct WriterOptions;

class StringPlainBlockBuilder : public BlockBuilder {
public:
  explicit StringPlainBlockBuilder(const WriterOptions *options);

  int Add(const uint8_t *vals, size_t count);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish(rowid_t ordinal_pos);

  void Reset();

  uint64_t EstimateEncodedSize() const;

  size_t Count() const;

  // Return the first added key.
  // key should be a Slice *
  Status GetFirstKey(void *key) const;

  // Length of a header.
  static const size_t kHeaderSize = sizeof(uint32_t) * 3;

private:
  faststring buffer_;

  size_t end_of_data_offset_;
  size_t size_estimate_;

  // Offsets of each entry, relative to the start of the block
  vector<uint32_t> offsets_;

  bool finished_;

  const WriterOptions *options_;

};


class StringPlainBlockDecoder : public BlockDecoder {
public:
  explicit StringPlainBlockDecoder(const Slice &slice);

  virtual Status ParseHeader();
  virtual void SeekToPositionInBlock(uint pos);
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match);
  Status CopyNextValues(size_t *n, ColumnDataView *dst);

  virtual bool HasNext() const {
    DCHECK(parsed_);
    return cur_idx_ < num_elems_;
  }

  virtual size_t Count() const {
    DCHECK(parsed_);
    return num_elems_;
  }

  virtual rowid_t ordinal_pos() const {
    DCHECK(parsed_);
    return ordinal_pos_base_ + cur_idx_;
  }

  Slice string_at_index(size_t indx) const;

private:
  Slice data_;
  bool parsed_;

  // The parsed offsets.
  // This array also contains one extra offset at the end, pointing
  // _after_ the last entry. This makes the code much simpler.
  vector<uint32_t> offsets_;

  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;

  // Index of the currently seeked element in the block.
  uint32_t cur_idx_;
};


}
}

#endif
