// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_INDEX_BLOCK_H
#define KUDU_CFILE_INDEX_BLOCK_H

#include "common/types.h"
#include "cfile/block_pointer.h"
#include "gutil/port.h"
#include "util/coding-inl.h"

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/utility.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>

namespace kudu {
namespace cfile {

using std::string;
using std::vector;
using boost::scoped_ptr;
using kudu::DataTypeTraits;

// Forward decl.
template <DataType KeyTypeEnum> class IndexBlockIterator;

struct WriterOptions;


class KeyEncoding {
public:
  // Append the encoded value onto the buffer.
  virtual void Encode(const void *key, faststring *buf) const = 0;

  // Compare an encoded key against 'cmp_against'.
  // The encoded key is a direct pointer into the index block
  // structure, so that, if it is possible to compare directly
  // against the encoded value, it will be more efficient.
  //
  //   limit: an upper bound on how far the key could possibly
  //          stretch. This is not in any way a _tight_ bound.
  //          That is to say, it may be far into the next key
  //          or even to the end of the entire block data.
  virtual int Compare(
    const uint8_t *encoded_ptr, const uint8_t *limit,
    const void *cmp_against) const = 0;

  virtual const uint8_t *Decode(const uint8_t *encoded_ptr, const uint8_t *limit,
                                void *ret) const = 0;

  virtual const uint8_t *SkipKey(const uint8_t *encoded_ptr, const uint8_t *limit) const = 0;

  virtual ~KeyEncoding() {};

  static KeyEncoding *Create(DataType type);
};


// Static polymorphism pattern:
// For performance in the templatized IndexReader code, we want to avod
// virtual calls for Compare and Decode. But, on the encoding side, we currently
// depend on vcalls. So, this allows the subclasses to implement the static methods
// and this conforms them to the virtual interface.
template<class SUBCLASS>
class KeyEncodingStaticForwarders : public KeyEncoding {
public:
  void Encode(const void *key, faststring *buf) const {
    derived()->StaticEncode(key, buf);
  }

  const uint8_t *Decode(const uint8_t *encoded_ptr, const uint8_t *limit,
              void *retptr) const {
    return derived()->StaticDecode(encoded_ptr, limit, retptr);
  }

  const uint8_t *SkipKey(const uint8_t *encoded_ptr, const uint8_t *limit) const {
    return derived()->StaticSkipKey(encoded_ptr, limit);
  }

  int Compare(const uint8_t *encoded_ptr, const uint8_t *limit,
              const void *cmp_against_ptr) const {
    return derived()->StaticCompare(encoded_ptr, limit, cmp_against_ptr);
  }

private:
  SUBCLASS *derived() {
    return static_cast<SUBCLASS *>(this);
  }
  const SUBCLASS *derived() const {
    return static_cast<const SUBCLASS *>(this);
  }
};

class UInt32KeyEncoding : public KeyEncodingStaticForwarders<UInt32KeyEncoding> {
public:
  void StaticEncode(const void *key, faststring *buf) const {
    InlinePutVarint32(buf, Deref(key));
  }

  int StaticCompare(const uint8_t *encoded_ptr, const uint8_t *limit,
                    const void *cmp_against_ptr) const {
    uint32_t cmp_against = Deref(cmp_against_ptr);

    uint32_t result;
    CHECK_NOTNULL(GetVarint32Ptr(encoded_ptr, limit, &result));

    if (result < cmp_against) {
      return -1;
    } else if (result > cmp_against) {
      return 1;
    } else {
      return 0;
    }
  }

  const uint8_t *StaticDecode(const uint8_t *encoded_ptr, const uint8_t *limit,
              void *retptr) const {
    uint32_t *ret = reinterpret_cast<uint32_t *>(retptr);
    return GetVarint32Ptr(encoded_ptr, limit, ret);
  }

  const uint8_t *StaticSkipKey(const uint8_t *encoded_ptr, const uint8_t *limit) const {
    uint32_t unused;
    return Decode(encoded_ptr, limit, &unused);
  }

private:
  const uint32_t Deref(const void *p) const {
    return *reinterpret_cast<const uint32_t *>(p);
  }
};

class StringKeyEncoding : public KeyEncodingStaticForwarders<StringKeyEncoding> {
public:
  void StaticEncode(const void *key, faststring *buf) const {
    const Slice *s = reinterpret_cast<const Slice *>(key);
    InlinePutVarint32(buf, s->size());
    buf->append(s->data(), s->size());
  }

  // Decode the string from the index into retptr, which must
  // point to a Slice object.
  //
  // NOTE: This function does not copy any data. The slice which
  // is returned points into some section of encoded_ptr,
  // and thus is only valid as long as that data is.
  const uint8_t *StaticDecode(const uint8_t *encoded_ptr, const uint8_t *limit,
                           void *retptr) const {
    Slice *ret = reinterpret_cast<Slice *>(retptr);

    uint32_t len;
    const uint8_t *data_start = GetVarint32Ptr(encoded_ptr, limit, &len);
    if (data_start == NULL) {
      // bad varint
      return NULL;
    }

    if (data_start + len > limit) {
      // length extends past end of valid area
      return NULL;
    }

    *ret = Slice(data_start, len);
    return data_start + len;
  }


  int StaticCompare(const uint8_t *encoded_ptr, const uint8_t *limit,
              const void *cmp_against_ptr) const {
    DCHECK(cmp_against_ptr);
    DCHECK(encoded_ptr);

    const Slice *cmp_against = reinterpret_cast<const Slice *>(cmp_against_ptr);

    Slice this_slice;
    if (PREDICT_FALSE(StaticDecode(encoded_ptr, limit, &this_slice) == NULL)) {
      LOG(WARNING) << "Invalid data in block!";
      return 0;
    }

    return this_slice.compare(*cmp_against);
  }

  const uint8_t *StaticSkipKey(const uint8_t *encoded_ptr, const uint8_t *limit) const {
    Slice unused;
    return Decode(encoded_ptr, limit, &unused);
  }
};


// Template to statically select the correct implementation
// of the key encoding
template<DataType Type>
struct KeyEncodingResolver {};

template<>
struct KeyEncodingResolver<UINT32> {
  typedef UInt32KeyEncoding Encoding;
};

template<>
struct KeyEncodingResolver<STRING> {
  typedef StringKeyEncoding Encoding;
};


// Index Block Builder for a particular key type.
// This works like the rest of the builders in the cfile package.
// After repeatedly calling Add(), call Finish() to encode it
// into a Slice, then you may Reset to re-use buffers.
class IndexBlockBuilder : boost::noncopyable {
public:
  explicit IndexBlockBuilder(const WriterOptions *options,
                             DataType data_type,
                             bool is_leaf);

  // Append an entry into the index.
  void Add(const void *key, const BlockPointer &ptr);

  // Finish the current index block.
  // Returns a fully encoded Slice including the data
  // as well as any necessary footer.
  // The Slice is only valid until the next call to
  // Reset().
  Slice Finish();

  // Return the key of the first entry in this index block.
  // For pointer-based types (such as strings), the pointed-to
  // data is only valid until the next call to Reset().
  Status GetFirstKey(void *key) const;

  size_t Count() const;

  // Return an estimate of the post-encoding size of this
  // index block. This estimate should be conservative --
  // it will over-estimate rather than under-estimate, and
  // should be accurate to within a reasonable threshold,
  // but is not exact.
  size_t EstimateEncodedSize() const;

  void Reset();

private:
#ifdef __clang__
  __attribute__((__unused__))
#endif
  const WriterOptions *options_;

  // Is the builder currently between Finish() and Reset()
  bool finished_;

  // Is this a leaf block?
  bool is_leaf_;

  scoped_ptr<KeyEncoding> encoding_;

  faststring buffer_;
  vector<uint32_t> entry_offsets_;
};


template <DataType KeyTypeEnum>
class IndexBlockReader : boost::noncopyable {
public:
  typedef DataTypeTraits<KeyTypeEnum> KeyTypeTraits;
  typedef typename KeyTypeTraits::cpp_type KeyType;

  // Construct a reader.
  // After construtoin, call 
  IndexBlockReader() :
    parsed_(false)
  {}

  void Reset() {
    data_ = Slice();
    parsed_ = false;
  }

  // Parse the given index block.
  //
  // This function may be called repeatedly to "reset" the reader to process
  // a new block.
  //
  // Note: this does not copy the data, so the slice must
  // remain valid for the lifetime of the reader (or until the next Parse cal)
  Status Parse(const Slice &data) {
    parsed_ = false;
    data_ = data;


    if (data_.size() < sizeof(uint32_t)) {
      return Status::Corruption("index block too small");
    }

    const uint8_t *trailer_size_ptr =
      data_.data() + data_.size() - sizeof(uint32_t);
    uint32_t trailer_size = DecodeFixed32(trailer_size_ptr);

    size_t max_size = trailer_size_ptr - data_.data();
    if (trailer_size <= 0 ||
        trailer_size > max_size) {
      string err = "invalid index block trailer size: " +
        boost::lexical_cast<string>(trailer_size);
      return Status::Corruption(err);
    }

    const uint8_t *trailer_ptr = trailer_size_ptr - trailer_size;

    bool success = trailer_.ParseFromArray(trailer_ptr, trailer_size);
    if (!success) {
      return Status::Corruption(
        "unable to parse trailer",
        trailer_.InitializationErrorString());
    }

    key_offsets_ = trailer_ptr - sizeof(uint32_t) * trailer_.num_entries();
    CHECK(trailer_ptr >= data_.data());

    VLOG(2) << "Parsed index trailer: " << trailer_.DebugString();

    parsed_ = true;
    return Status::OK();
  }

  size_t Count() const {
    CHECK(parsed_) << "not parsed";
    return trailer_.num_entries();
  }

  IndexBlockIterator<KeyTypeEnum> *NewIterator() const {
    CHECK(parsed_) << "not parsed";
    return new IndexBlockIterator<KeyTypeEnum>(this);
  }

  bool IsLeaf() {
    return trailer_.type() == IndexBlockTrailerPB::LEAF;
  }

private:
  friend class IndexBlockIterator<KeyTypeEnum>;

  int CompareKey(int idx_in_block, const KeyType &search_key) const {
    const uint8_t *key_ptr, *limit;
    GetKeyPointer(idx_in_block, &key_ptr, &limit);
    return encoding_.StaticCompare(key_ptr, limit, &search_key);
  }

  Status ReadEntry(size_t idx, KeyType *key, BlockPointer *block_ptr) const {
    if (idx >= trailer_.num_entries()) {
      return Status::NotFound("Invalid index");
    }

    // At 'ptr', data is encoded as follows:
    // <key> <block offset> <block length>

    const uint8_t *ptr, *limit;
    GetKeyPointer(idx, &ptr, &limit);

    ptr = encoding_.StaticDecode(ptr, limit, key);
    if (ptr == NULL) {
      return Status::Corruption("Invalid key in index");
    }

    return block_ptr->DecodeFrom(ptr, data_.data() + data_.size());
  }

  // Set *ptr to the beginning of the index data for the given index
  // entry.
  // Set *limit to the 'limit' pointer for that entry (i.e a pointer
  // beyond which the data no longer is part of that entry).
  //   - *limit can be used to prevent overrunning in the case of a
  //     corrupted length varint or length prefix
  void GetKeyPointer(int idx_in_block, const uint8_t **ptr, const uint8_t **limit) const {
    size_t offset_in_block = DecodeFixed32(
      &key_offsets_[idx_in_block * sizeof(uint32_t)]);
    *ptr = data_.data() + offset_in_block;

    int next_idx = idx_in_block + 1;

    if (PREDICT_FALSE(next_idx >= trailer_.num_entries())) {
      DCHECK(next_idx == Count()) << "Bad index: " << idx_in_block
                                  << " Count: " << Count();
      // last key in block: limit is the beginning of the offsets array
      *limit = key_offsets_;
    } else {
      // otherwise limit is the beginning of the next key
      offset_in_block = DecodeFixed32(
        &key_offsets_[next_idx * sizeof(uint32_t)]);
      *limit = data_.data() + offset_in_block;
    }
  }

  typename KeyEncodingResolver<KeyTypeEnum>::Encoding encoding_;

  static const int kMaxTrailerSize = 64*1024;
  Slice data_;

  IndexBlockTrailerPB trailer_;
  const uint8_t *key_offsets_;
  bool parsed_;
};

template <DataType KeyTypeEnum>
class IndexBlockIterator : boost::noncopyable {
public:
  typedef DataTypeTraits<KeyTypeEnum> KeyTypeTraits;
  typedef typename KeyTypeTraits::cpp_type KeyType;

  explicit IndexBlockIterator(const IndexBlockReader<KeyTypeEnum> *reader) :
    reader_(reader),
    cur_idx_(-1),
    seeked_(false)
  {
  }

  // Reset the state of this iterator. This should be used
  // after the associated 'reader' object parses a different block.
  void Reset() {
    seeked_ = false;
    cur_idx_ = -1;
  }

  // Find the highest block pointer in this index
  // block which has a value <= the given key.
  // If such a block is found, returns OK status.
  // If no such block is found (i.e the smallest key in the
  // index is still larger than the provided key), then
  // Status::NotFound is returned.
  //
  // If this function returns an error, then the state of this
  // iterator is undefined (i.e it may or may not have moved
  // since the previous call)
  Status SeekAtOrBefore(const KeyType &search_key) {
    size_t left = 0;
    size_t right = reader_->Count() - 1;
    while (left < right) {
      int mid = (left + right + 1) / 2;

      int compare = reader_->CompareKey(mid, search_key);
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
    int compare = reader_->CompareKey(left, search_key);
    if (compare > 0) {
      // The last midpoint was still greather then the
      // provided key, which implies that the key is
      // lower than the lowest in the block.
      return Status::NotFound("key not present");
    }

    return SeekToIndex(left);
  }

  Status SeekToIndex(size_t idx) {
    cur_idx_ = idx;
    Status s = reader_->ReadEntry(idx, &cur_key_, &cur_ptr_);
    seeked_ = s.ok();
    return s;
  }

  bool HasNext() const {
    return cur_idx_ + 1 < reader_->Count();
  }

  Status Next() {
    return SeekToIndex(cur_idx_ + 1);
  }

  const BlockPointer &GetCurrentBlockPointer() const {
    CHECK(seeked_) << "not seeked";
    return cur_ptr_;
  }

  const KeyType *GetCurrentKey() const {
    CHECK(seeked_) << "not seeked";
    return &cur_key_;
  }

private:
  const IndexBlockReader<KeyTypeEnum> *reader_;
  size_t cur_idx_;
  KeyType cur_key_;
  BlockPointer cur_ptr_;
  bool seeked_;
};

} // namespace kudu
} // namespace cfile
#endif
