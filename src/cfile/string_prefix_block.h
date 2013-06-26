// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_STRING_PREFIX_BLOCK_H
#define KUDU_CFILE_STRING_PREFIX_BLOCK_H

#include <vector>

#include "common/rowid.h"

namespace kudu {

class Arena;

namespace cfile {

// Encoding for data blocks of strings.
// This encodes in a manner similar to LevelDB (prefix coding)
class StringPrefixBlockBuilder : public BlockBuilder {
 public:
  explicit StringPrefixBlockBuilder(const WriterOptions *options);

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

 private:
  faststring buffer_;
  faststring last_val_;

  // Restart points, offsets relative to start of block
  vector<uint32_t> restarts_;

  int val_count_;
  int vals_since_restart_;
  bool finished_;

  const WriterOptions *options_;

  // Maximum length of a header.
  // We leave this much space at the start of the buffer before
  // accumulating any data, so we can later fill in the variable-length
  // header.
  // Currently two varints, so max 10 bytes
  static const size_t kHeaderReservedLength = 10;
};



// Decoder for STRING type, PREFIX encoding
class StringPrefixBlockDecoder : public BlockDecoder {
 public:
  explicit StringPrefixBlockDecoder(const Slice &slice);

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

 private:
  Status SkipForward(int n);
  Status CheckNextPtr();
  Status ParseNextValue();
  Status ParseNextIntoArena(Slice prev_val, Arena *dst, Slice *copied);

  const uint8_t *DecodeEntryLengths(const uint8_t *ptr,
                           uint32_t *shared,
                           uint32_t *non_shared) const;

  const uint8_t *GetRestartPoint(uint32_t idx) const;
  void SeekToRestartPoint(uint32_t idx);

  void SeekToStart();

  Slice data_;

  bool parsed_;

  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;

  uint32_t num_restarts_;
  const uint32_t *restarts_;
  uint32_t restart_interval_;

  const uint8_t *data_start_;

  // Index of the next row to be returned by CopyNextValues, relative to
  // the block's base offset.
  // When the block is exhausted, cur_idx_ == num_elems_
  uint32_t cur_idx_;

  // The first value to be returned by the next CopyNextValues().
  faststring cur_val_;

  // The ptr pointing to the next element to parse. This is for the entry
  // following cur_val_
  // This is advanced by ParseNextValue()
  const uint8_t *next_ptr_;
};

} // namespace cfile
} // namespace kudu
#endif
