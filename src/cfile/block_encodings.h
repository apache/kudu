// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_BLOCK_ENCODINGS_H
#define KUDU_CFILE_BLOCK_ENCODINGS_H

#include <stdint.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/rowid.h"
#include "cfile/cfile.pb.h"
#include "gutil/macros.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
class ColumnDataView;

namespace cfile {

// Return the default encoding to use for the given data type.
// TODO: this probably won't stay around too long - in a real Flush
// situation, we can look at the content in the memstore and pick the
// most effective coding.
inline EncodingType GetDefaultEncoding(DataType type) {
  switch (type) {
    case STRING:
      return PREFIX;
    case INT32:
      return PLAIN;
    case UINT32:
      return GROUP_VARINT;
    default:
      CHECK(0) << "unknown type: " << type;
  }
}


class BlockBuilder {
 public:
  BlockBuilder() { }

  // Add a sequence of values to the block.
  // Returns the number of values actually added, which may be less
  // than requested if the block is full.
  virtual int Add(const uint8_t *vals, size_t count) = 0;

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  virtual Slice Finish(rowid_t ordinal_pos) = 0;

  // Reset the internal state of the encoder.
  //
  // Any data previously returned by Finish or by GetFirstKey
  // may be invalidated by this call.
  //
  // Postcondition: Count() == 0
  virtual void Reset() = 0;

  // Return an estimate of the number of bytes that this block
  // will require once encoded. This is not necessarily
  // an upper or lower bound.
  virtual uint64_t EstimateEncodedSize() const = 0;

  // Return the number of entries that have been added to the
  // block.
  virtual size_t Count() const = 0;

  // Return the key of the first entry in this index block.
  // For pointer-based types (such as strings), the pointed-to
  // data is only valid until the next call to Reset().
  //
  // If no keys have been added, returns Status::NotFound
  virtual Status GetFirstKey(void *key) const = 0;

  virtual ~BlockBuilder() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(BlockBuilder);
};


class BlockDecoder {
 public:
  BlockDecoder() { }

  virtual Status ParseHeader() = 0;

  // Seek the decoder to the given positional index of the block.
  // For example, SeekToPositionInBlock(0) seeks to the first
  // stored entry.
  //
  // It is an error to call this with a value larger than Count().
  // Doing so has undefined results.
  //
  // TODO: Since we know the actual file position, maybe we
  // should just take the actual ordinal in the file
  // instead of the position in the block?
  virtual void SeekToPositionInBlock(uint pos) = 0;

  // Seek the decoder to the given value in the block, or the
  // lowest value which is greater than the given value.
  //
  // If the decoder was able to locate an exact match, then
  // sets *exact_match to true. Otherwise sets *exact_match to
  // false, to indicate that the seeked value is _after_ the
  // requested value.
  //
  // If the given value is less than the lowest value in the block,
  // seeks to the start of the block. If it is higher than the highest
  // value in the block, then returns Status::NotFound
  //
  // This will only return valid results when the data block
  // consists of values in sorted order.
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match) = 0;

  // Fetch the next set of values from the block into 'dst'.
  // The output block must have space for up to n cells.
  //
  // Modifies *n to contain the number of values fetched.
  //
  // In the case that the values are themselves references
  // to other memory (eg Slices), the referred-to memory is
  // allocated in the dst block's arena.
  virtual Status CopyNextValues(size_t *n, ColumnDataView *dst) = 0;

  // Return true if there are more values remaining to be iterated.
  // (i.e that the next call to CopyNextValues will return at least 1
  // element)
  // TODO: change this to a Remaining() call?
  virtual bool HasNext() const = 0;

  // Return the number of elements in this block.
  virtual size_t Count() const = 0;

  // Return the ordinal position in the file of the currently seeked
  // entry (ie the entry that will next be returned by CopyNextValues())
  virtual rowid_t ordinal_pos() const = 0;

  virtual ~BlockDecoder() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(BlockDecoder);
};

} // namespace cfile
} // namespace kudu

#endif
