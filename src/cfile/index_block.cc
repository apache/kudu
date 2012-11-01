// Copyright (c) 2012, Cloudera, inc.
#include "index_block.h"
#include "boost/foreach.hpp"
#include "cfile.h"
#include "util/protobuf_util.h"

namespace kudu {
namespace cfile {

KeyEncoding *KeyEncoding::Create(DataType data_type) {
  switch (data_type) {
    case UINT32:
      return new UInt32KeyEncoding();
      break;
    case STRING:
      return new StringKeyEncoding();
      break;
    default:
      // TODO: change this to be factory method
      // so we can return a Status instead of barfing?
      CHECK(0) << "Bad data type: " << data_type;
      return NULL;
  }
}

IndexBlockBuilder::IndexBlockBuilder(
  const WriterOptions *options,
  DataType data_type,
  bool is_leaf)
  : options_(options),
    finished_(false),
    is_leaf_(is_leaf),
    encoding_(KeyEncoding::Create(data_type))
{
}


void IndexBlockBuilder::Add(const void * keyptr,
                            const BlockPointer &ptr) {
  DCHECK(!finished_) <<
    "Must Reset() after Finish() before more Add()";

  size_t entry_offset = buffer_.size();
  encoding_->Encode(keyptr, &buffer_);
  ptr.EncodeTo(&buffer_);
  entry_offsets_.push_back(entry_offset);
}

Slice IndexBlockBuilder::Finish() {
  CHECK(!finished_) << "already called Finish()";

  BOOST_FOREACH(uint32_t off, entry_offsets_) {
    InlinePutFixed32(&buffer_, off);
  }

  IndexBlockTrailerPB trailer;
  trailer.set_num_entries(entry_offsets_.size());
  trailer.set_type(
    is_leaf_ ? IndexBlockTrailerPB::LEAF : IndexBlockTrailerPB::INTERNAL);
  AppendPBToString(trailer, &buffer_);

  InlinePutFixed32(&buffer_, trailer.GetCachedSize());

  finished_ = true;
  return Slice(buffer_);
}


// Return the key of the first entry in this index block.
Status IndexBlockBuilder::GetFirstKey(void *key) const {
  // TODO: going to need to be able to pass an arena or something
  // for slices, which need to copy

  if (entry_offsets_.empty()) {
    return Status::NotFound("no keys in builder");
  }

  bool success = NULL != encoding_->Decode(
    buffer_.data(),
    buffer_.data() + buffer_.size(),
    key);
  if (success) {
    return Status::OK();
  } else {
    return Status::Corruption("Unable to decode first key");
  }
}

size_t IndexBlockBuilder::EstimateEncodedSize() const {
  // the actual encoded index entries
  int size = buffer_.size();

  // entry offsets
  size += sizeof(uint32_t) * entry_offsets_.size();

  // estimate trailer cheaply -- not worth actually constructing
  // a trailer to determine the size.
  size += 16;

  return size;
}

size_t IndexBlockBuilder::Count() const {
  return entry_offsets_.size();
}

void IndexBlockBuilder::Reset() {
  buffer_.clear();
  entry_offsets_.clear();
  finished_ = false;
}


} // namespace cfile
} // namespace kudu
