// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_INDEX_BLOCK_H
#define KUDU_CFILE_INDEX_BLOCK_H

#include "block_pointer.h"
#include "util/coding.h"

#include <boost/foreach.hpp>
#include <boost/utility.hpp>
#include <string>
#include <vector>

namespace kudu {
namespace cfile {

using std::string;
using std::vector;

// KeyEncoding template specializations determine
// how to encode/decode various types into index
// blocks.
template <class KeyType>
class KeyEncoding {};


// uint32_t specialization simply uses varint32s
template <>
class KeyEncoding<uint32_t> {
public:

  // Append the encoded value onto the buffer.
  void Encode(const uint32_t i, string *buf) const {
    PutVarint32(buf, i);
  }

  // Compare an encoded key against 'cmp_against'.
  // The encoded key is a direct pointer into the index block
  // structure, so that, if it is possible to compare directly
  // against the encoded value, it will be more efficient.
  //
  //   limit: an upper bound on how far the key could possibly
  //          stretch. This is not in any way a _tight_ bound.
  //          That is to say, it may be far into the next key
  //          or even to the end of the entire block data.
  int Compare(const char *encoded_ptr, const char *limit,
              const uint32_t cmp_against) const {
    uint32_t result;
    GetVarint32Ptr(encoded_ptr, limit, &result);

    if (result < cmp_against) {
      return -1;
    } else if (result > cmp_against) {
      return 1;
    } else {
      return 0;
    }
  }

  bool Decode(const char *encoded_ptr, const char *limit,
              uint32_t *ret) const {
    const char *next = GetVarint32Ptr(encoded_ptr, limit, ret);
    return next != NULL;
  }

  const char *SkipKey(const char *encoded_ptr, const char *limit) const {
    uint32_t unused;
    return GetVarint32Ptr(encoded_ptr, limit, &unused);
  }
};


// Index Block Builder for a particular key type.
// This works like the rest of the builders in the cfile package.
// After repeatedly calling Add(), call Finish() to encode it
// into a Slice, then you may Reset to re-use buffers.
template <class KeyType>
class IndexBlockBuilder : boost::noncopyable {
public:
  explicit IndexBlockBuilder(const WriterOptions *options) :
    options_(options),
    finished_(false)
  {
  }

  // Append an entry into the index.
  void Add(const KeyType &key,
           const BlockPointer &ptr) {
    DCHECK(!finished_) <<
      "Must Reset() after Finish() before more Add()";

    size_t entry_offset = buffer_.size();
    encoding_.Encode(key, &buffer_);
    ptr.EncodeTo(&buffer_);
    entry_offsets_.push_back(entry_offset);
  }

  // Finish the current index block.
  // Returns a fully encoded Slice including the data
  // as well as any necessary footer.
  // The Slice is only valid until the next call to
  // Reset().
  Slice Finish() {
    CHECK(!finished_) << "already called Finish()";

    BOOST_FOREACH(uint32_t off, entry_offsets_) {
      PutFixed32(&buffer_, off);
    }

    IndexBlockTrailerPB trailer;
    trailer.set_num_entries(entry_offsets_.size());
    trailer.set_type(IndexBlockTrailerPB::INTERNAL);
    trailer.AppendToString(&buffer_);

    PutFixed32(&buffer_, trailer.GetCachedSize());

    finished_ = true;
    return Slice(buffer_);
  }

  // Return the key of the first entry in this index block.
  Status GetFirstKey(KeyType *key) const {
    if (entry_offsets_.empty()) {
      return Status::NotFound("no keys in builder");
    }

    bool success = encoding_.Decode(
      buffer_.c_str(),
      buffer_.c_str() + buffer_.size(),
      key);
    if (success) {
      return Status::OK();
    } else {
      return Status::Corruption("Unable to decode first key");
    }
  }

  // Return an estimate of the post-encoding size of this
  // index block. This estimate should be conservative --
  // it will over-estimate rather than under-estimate, and
  // should be accurate to within a reasonable threshold,
  // but is not exact.
  size_t EstimateEncodedSize() const {
    // the actual encoded index entries
    int size = buffer_.size();

    // entry offsets
    size += sizeof(uint32_t) * entry_offsets_.size();

    // estimate trailer cheaply -- not worth actually constructing
    // a trailer to determine the size.
    size += 16;

    return size;
  }

  void Reset() {
    buffer_.clear();
    entry_offsets_.clear();
    finished_ = false;
  }

private:
  const WriterOptions *options_;

  // Is the builder currently between Finish() and Reset()
  bool finished_;

  KeyEncoding<KeyType> encoding_;

  string buffer_;
  vector<uint32_t> entry_offsets_;
};



template <class KeyType>
class IndexBlockReader : boost::noncopyable {
public:
  IndexBlockReader(const Slice &data) :
    data_(data),
    parsed_(false)
  {}

  Status Parse() {
    CHECK(!parsed_) << "already parsed";

    if (data_.size() < sizeof(uint32_t)) {
      return Status::Corruption("index block too small");
    }

    const char *trailer_size_ptr =
      data_.data() + data_.size() - sizeof(uint32_t);
    uint32_t trailer_size = DecodeFixed32(trailer_size_ptr);

    size_t max_size = trailer_size_ptr - data_.data();
    if (trailer_size <= 0 ||
        trailer_size > max_size) {
      string err = "invalid trailer size: " + trailer_size;
      return Status::Corruption(err);
    }

    const char *trailer_ptr = trailer_size_ptr - trailer_size;

    bool success = trailer_.ParseFromArray(trailer_ptr, trailer_size);
    if (!success) {
      return Status::Corruption(
        "unable to parse trailer",
        trailer_.InitializationErrorString());
    }

    key_offsets_ = trailer_ptr - sizeof(uint32_t) * trailer_.num_entries();
    CHECK(trailer_ptr >= data_.data());

    parsed_ = true;
    return Status::OK();
  }

  size_t Count() const {
    CHECK(parsed_) << "not parsed";
    return trailer_.num_entries();
  }

  // Find the highest block pointer in this index
  // block which has a value <= the given key.
  // If such a block is found, returns OK status.
  // If no such block is found (i.e the smallest key in the
  // index is still larger than the provided key), then
  // 'ret' is left unmodified, and Status::NotFound is returned.
  Status Search(const KeyType &search_key, BlockPointer *ret) const {
    CHECK(parsed_) << "not parsed";

    size_t left = 0;
    size_t right = Count() - 1;
    int compare = 0;
    int mid = 0;
    while (left < right) {
      mid = (left + right + 1) / 2;
      const char *mid_key_ptr = GetKeyPointer(mid);

      compare = encoding_.Compare(mid_key_ptr,
                                  mid_key_ptr + 16, // conservative limit
                                  search_key);
      if (compare < 0) { // mid < search
        left = mid;
      } else if (compare > 0) { // mid > search
        right = mid - 1;
      } else { // mid == search
        left = mid;
        break;
      }
    }

    // closest is now 'left'
    const char *ptr = GetKeyPointer(left);
    compare = encoding_.Compare(ptr, ptr + 16, search_key);
    if (compare > 0) {
      // The last midpoint was still greather then the
      // provided key, which implies that the key is
      // lower than the lowest in the block.
      return Status::NotFound("key not present");
    }

    // At 'ptr', data is encoded as follows:
    // <key> <block offset> <block length>
    // We need to skip over the key itself
    ptr = encoding_.SkipKey(ptr, data_.data() + data_.size());
    if (ptr == NULL) {
      return Status::Corruption("Invalid key in index");
    }

    return ret->DecodeFrom(ptr, data_.data() + data_.size());
  }

private:
  const char *GetKeyPointer(int idx_in_block) const {
    size_t offset_in_block = DecodeFixed32(
      &key_offsets_[idx_in_block * sizeof(uint32_t)]);
    return data_.data() + offset_in_block;
  }

  KeyEncoding<KeyType> encoding_;

  static const int kMaxTrailerSize = 64*1024;
  const Slice data_;

  IndexBlockTrailerPB trailer_;
  const char *key_offsets_;
  bool parsed_;
};


} // namespace kudu
} // namespace cfile
#endif
