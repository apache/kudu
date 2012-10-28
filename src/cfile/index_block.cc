// Copyright (c) 2012, Cloudera, inc.
#include "index_block.h"
#include "boost/foreach.hpp"
#include "cfile.h"

namespace kudu {
namespace cfile {

IndexBlockBuilder::IndexBlockBuilder(
  const WriterOptions *options, bool is_leaf)
  : options_(options),
    finished_(false),
    is_leaf_(is_leaf)
{

  // TODO: instantiate the right encoding based on type
  encoding_.reset(new UInt32KeyEncoding());
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
    PutFixed32(&buffer_, off);
  }

  IndexBlockTrailerPB trailer;
  trailer.set_num_entries(entry_offsets_.size());
  trailer.set_type(
    is_leaf_ ? IndexBlockTrailerPB::LEAF : IndexBlockTrailerPB::INTERNAL);
  trailer.AppendToString(&buffer_);

  PutFixed32(&buffer_, trailer.GetCachedSize());

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
    buffer_.c_str(),
    buffer_.c_str() + buffer_.size(),
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

void IndexBlockBuilder::Reset() {
  buffer_.clear();
  entry_offsets_.clear();
  finished_ = false;
}


} // namespace cfile
} // namespace kudu
