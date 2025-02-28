// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/cfile/index_block.h"

#include <cstdint>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/protobuf_util.h"

using strings::Substitute;

namespace kudu {
namespace cfile {

inline void SliceEncode(const Slice& key, faststring* buf) {
  InlinePutVarint32(buf, key.size());
  buf->append(key.data(), key.size());
}

inline const uint8_t* SliceDecode(
    const uint8_t* encoded_ptr,
    const uint8_t* limit,
    Slice* retptr) {
  uint32_t len;
  const uint8_t* data_start = GetVarint32Ptr(encoded_ptr, limit, &len);
  if (PREDICT_FALSE(!data_start)) {
    // bad varint
    return nullptr;
  }

  if (PREDICT_FALSE(data_start + len > limit)) {
    // length extends past end of valid area
    return nullptr;
  }

  *retptr = Slice(data_start, len);
  return data_start + len;
}

IndexBlockBuilder::IndexBlockBuilder(bool is_leaf)
    : finished_(false),
      is_leaf_(is_leaf) {
}

void IndexBlockBuilder::Add(const Slice& keyptr,
                            const BlockPointer& ptr) {
  DCHECK(!finished_) << "Must Reset() after Finish() before more Add()";

  const size_t entry_offset = buffer_.size();
  SliceEncode(keyptr, &buffer_);
  ptr.EncodeTo(&buffer_);
  entry_offsets_.push_back(entry_offset);
}

Slice IndexBlockBuilder::Finish() {
  DCHECK(!finished_) << "already called Finish()";

  for (uint32_t off : entry_offsets_) {
    InlinePutFixed32(&buffer_, off);
  }

  IndexBlockTrailerPB trailer;
  trailer.set_num_entries(entry_offsets_.size());
  trailer.set_type(is_leaf_ ? IndexBlockTrailerPB::LEAF
                            : IndexBlockTrailerPB::INTERNAL);
  AppendPBToString(trailer, &buffer_);

  InlinePutFixed32(&buffer_, trailer.GetCachedSize());

  finished_ = true;
  return Slice(buffer_);
}

// Return the key of the first entry in this index block.
Status IndexBlockBuilder::GetFirstKey(Slice* key) const {
  // TODO(todd): going to need to be able to pass an arena or something
  // for slices, which need to copy
  if (PREDICT_FALSE(entry_offsets_.empty())) {
    return Status::NotFound("no keys in builder");
  }
  if (PREDICT_FALSE(!SliceDecode(
          buffer_.data(), buffer_.data() + buffer_.size(), key))) {
    return Status::Corruption("unable to decode first key");
  }
  return Status::OK();
}

size_t IndexBlockBuilder::EstimateEncodedSize() const {
  // the actual encoded index entries
  auto size = buffer_.size();

  // entry offsets
  size += sizeof(uint32_t) * entry_offsets_.size();

  // estimate trailer cheaply -- not worth actually constructing
  // a trailer to determine the size.
  size += 16;

  return size;
}

// Construct a reader.
// After construction, call Parse() to read the data.
IndexBlockReader::IndexBlockReader()
    : key_offsets_(nullptr),
      parsed_(false) {
}

void IndexBlockReader::Reset() {
  data_.clear();
  trailer_.Clear();
  key_offsets_ = nullptr;
  parsed_ = false;
}

Status IndexBlockReader::Parse(const Slice& data) {
  data_ = data;

  // The code below parses the data of an index block, checking for invariants
  // based on index block layout and protobuf serialization and encoding rules.
  //
  // For details on protobuf's serialization and encoding, see [2] and [3].
  // For details on Kudu's index block layout, see [3].
  //
  // [1] https://protobuf.dev/programming-guides/encoding/
  // [2] https://stackoverflow.com/questions/30915704/maximum-serialized-protobuf-message-size
  // [3] https://github.com/apache/kudu/blob/master/docs/design-docs/cfile.md#cfile-index
  if (PREDICT_FALSE(data_.size() < sizeof(uint32_t))) {
    return Status::Corruption("index block too small");
  }

  const uint8_t* trailer_size_ptr =
      data_.data() + data_.size() - sizeof(uint32_t);
  const size_t trailer_size = DecodeFixed32(trailer_size_ptr);

  // A serialized IndexBlockTrailerPB message cannot be shorter than four bytes.
  // In protobuf, each serialized field contains a tag followed by some data.
  // The tag is at least one byte. As for the serialized fields of the
  // IndexBlockTrailerPB message, it contains at least two required fields now:
  //   * int32_t num_entries: at least one byte serialized with varint encoding
  //   * BlockType type: enum, at least one byte serialized
  // So, the total for the minimum length of these two fields serialized
  // in protobuf format is four bytes: (1 + 1) + (1 + 1).
  if (PREDICT_FALSE(trailer_size < 4 ||
                    trailer_size > trailer_size_ptr - data_.data())) {
    return Status::Corruption(Substitute(
        "$0: invalid index block trailer size", trailer_size));
  }

  const uint8_t* trailer_ptr = trailer_size_ptr - trailer_size;
  if (PREDICT_FALSE(!trailer_.ParseFromArray(trailer_ptr, trailer_size))) {
    return Status::Corruption("unable to parse trailer",
                              trailer_.InitializationErrorString());
  }

  const auto num_entries = trailer_.num_entries();
  if (PREDICT_FALSE(num_entries < 0)) {
    return Status::Corruption(Substitute(
        "$0: bad number of entries in trailer", num_entries));
  }

  key_offsets_ = trailer_ptr - sizeof(uint32_t) * num_entries;
  // Each entry is three bytes at least (integers use varint encoding):
  //   * key
  //     ** the size of the key: at least one byte
  //     ** the key's data: zero or more bytes (zero bytes for an empty key)
  //   * the offset of the block: at least one byte
  //   * the size of the block: at least one byte
  if (PREDICT_FALSE(key_offsets_ < data_.data() + 3 * num_entries)) {
    return Status::Corruption(Substitute(
        "$0: too many entries in trailer", num_entries));
  }

  VLOG(2) << "Parsed index trailer: " << pb_util::SecureDebugString(trailer_);

  parsed_ = true;
  return Status::OK();
}

size_t IndexBlockReader::Count() const {
  DCHECK(parsed_) << "not parsed";
  return trailer_.num_entries();
}

IndexBlockIterator* IndexBlockReader::NewIterator() const {
  DCHECK(parsed_) << "not parsed";
  return new IndexBlockIterator(this);
}

bool IndexBlockReader::IsLeaf() const {
  DCHECK(parsed_) << "not parsed";
  return trailer_.type() == IndexBlockTrailerPB::LEAF;
}

int IndexBlockReader::CompareKey(size_t idx_in_block,
                                 const Slice& search_key) const {
  const uint8_t* key_ptr = nullptr;
  const uint8_t* limit = nullptr;
  if (auto s = GetKeyPointer(idx_in_block, &key_ptr, &limit);
      PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute("failed to position in block: $0", s.ToString());
    return 0;
  }
  Slice this_slice;
  if (PREDICT_FALSE(!SliceDecode(key_ptr, limit, &this_slice))) {
    LOG(WARNING) << Substitute("invalid data in block at index $0", idx_in_block);
    return 0;
  }

  return this_slice.compare(search_key);
}

Status IndexBlockReader::ReadEntry(size_t idx,
                                   Slice* key,
                                   BlockPointer* block_ptr) const {
  DCHECK(parsed_) << "not parsed";
  if (PREDICT_FALSE(idx >= trailer_.num_entries())) {
    return Status::NotFound(Substitute("$0: invalid index", idx));
  }

  // At 'ptr', data is encoded as follows:
  // <key> <block offset> <block length>

  const uint8_t* ptr = nullptr;
  const uint8_t* limit = nullptr;
  RETURN_NOT_OK(GetKeyPointer(idx, &ptr, &limit));

  ptr = SliceDecode(ptr, limit, key);
  if (PREDICT_FALSE(!ptr)) {
    return Status::Corruption(Substitute("invalid key in index $0", idx));
  }

  return block_ptr->DecodeFrom(ptr, data_.data() + data_.size());
}

Status IndexBlockReader::GetKeyPointer(size_t idx_in_block,
                                       const uint8_t** ptr,
                                       const uint8_t** limit) const {
  DCHECK(parsed_) << "not parsed";
  if (PREDICT_FALSE(trailer_.num_entries() <= idx_in_block)) {
    return Status::NotFound(Substitute("$0: no such index", idx_in_block));
  }
  DCHECK(key_offsets_);
  const size_t offset_in_block = DecodeFixed32(
      &key_offsets_[idx_in_block * sizeof(uint32_t)]);
  if (PREDICT_FALSE(data_.data() + offset_in_block >= key_offsets_)) {
    return Status::Corruption(Substitute(
        "$0: invalid block offset at index $1", offset_in_block, idx_in_block));
  }
  *ptr = data_.data() + offset_in_block;

  const size_t next_idx = idx_in_block + 1;
  if (PREDICT_TRUE(next_idx < trailer_.num_entries())) {
    // The limit is the beginning of the next key.
    const size_t offset_in_block = DecodeFixed32(
        &key_offsets_[next_idx * sizeof(uint32_t)]);
    if (PREDICT_FALSE(data_.data() + offset_in_block >= key_offsets_)) {
      return Status::Corruption(Substitute(
          "$0: invalid block offset at index $1", offset_in_block, next_idx));
    }
    *limit = data_.data() + offset_in_block;
  } else {
    // For the last key in block, the limit is the beginning of the offsets array.
    DCHECK(next_idx == Count()) << Substitute("bad index: $0 count: $1",
                                              idx_in_block, Count());
    *limit = key_offsets_;
  }
  return Status::OK();
}

void IndexBlockBuilder::Reset() {
  buffer_.clear();
  entry_offsets_.clear();
  finished_ = false;
}

IndexBlockIterator::IndexBlockIterator(const IndexBlockReader* reader)
    : reader_(reader),
      cur_idx_(-1),
      seeked_(false) {
}

void IndexBlockIterator::Reset() {
  cur_idx_ = -1;
  cur_key_.clear();
  cur_ptr_ = BlockPointer();
  seeked_ = false;
}

Status IndexBlockIterator::SeekAtOrBefore(const Slice& search_key) {
  const auto num_entries = reader_->Count();
  size_t left = 0;
  size_t right = num_entries > 0 ? num_entries - 1 : 0;
  while (left < right) {
    size_t mid = (left + right + 1) / 2;

    int compare = reader_->CompareKey(mid, search_key);
    if (compare < 0) {  // mid < search
      left = mid;
    } else if (compare > 0) {  // mid > search
      right = mid - 1;
    } else {  // mid == search
      left = mid;
      break;
    }
  }

  // closest is now 'left'
  if (reader_->CompareKey(left, search_key) > 0) {
    // The last midpoint was still greater than the
    // provided key, which implies that the key is
    // lower than the lowest in the block.
    return Status::NotFound("key not present");
  }

  return SeekToIndex(left);
}

Status IndexBlockIterator::SeekToIndex(size_t idx) {
  cur_idx_ = idx;
  Status s = reader_->ReadEntry(idx, &cur_key_, &cur_ptr_);
  seeked_ = s.ok();
  return s;
}

// Unsigned cur_idx_ overflow is intended and works as expected when
// cur_idx_ has its initial value, so quell UBSAN warnings.
ATTRIBUTE_NO_SANITIZE_INTEGER
bool IndexBlockIterator::HasNext() const {
  return cur_idx_ + 1 < reader_->Count();
}

// Unsigned cur_idx_ overflow is intended and works as expected when
// cur_idx_ has its initial value, so quell UBSAN warnings.
ATTRIBUTE_NO_SANITIZE_INTEGER
Status IndexBlockIterator::Next() {
  return SeekToIndex(cur_idx_ + 1);
}

const BlockPointer& IndexBlockIterator::GetCurrentBlockPointer() const {
  DCHECK(seeked_) << "not seeked";
  return cur_ptr_;
}

const Slice& IndexBlockIterator::GetCurrentKey() const {
  DCHECK(seeked_) << "not seeked";
  return cur_key_;
}

} // namespace cfile
} // namespace kudu
