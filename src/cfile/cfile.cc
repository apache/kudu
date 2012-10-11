// Copyright (c) 2012, Cloudera, inc

#include <algorithm>
#include <boost/foreach.hpp>
#include <endian.h>
#include <string>
#include <glog/logging.h>

#include "cfile.h"
#include "cfile.pb.h"
#include "block_pointer.h"
#include "index_block.h"
#include "util/env.h"
#include "util/coding.h"
#include "util/logging.h"

using std::string;

namespace kudu { namespace cfile {

static const string kMagicString = "kuducfil";


////////////////////////////////////////////////////////////
// Options
////////////////////////////////////////////////////////////
WriterOptions::WriterOptions() :
  block_size(256*1024),
  block_restart_interval(16)
{}


////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////


Writer::Writer(const WriterOptions &options,
               shared_ptr<WritableFile> file) :
  file_(file),
  off_(0),
  options_(options),
  state_(kWriterInitialized)
{
}

Status Writer::Start() {
  CHECK(state_ == kWriterInitialized) <<
    "bad state for Start(): " << state_;

  Slice header(kMagicString);
  file_->Append(header);
  off_ += header.size();
  state_ = kWriterWriting;
  return Status::OK();
}

Status Writer::Finish() {
  CHECK(state_ == kWriterWriting) <<
    "Bad state for Finish(): " << state_;
  CFileFooterPB footer;

  // Finish all trees in progress -- they may have pending
  // writes.
  typedef std::pair<string, shared_ptr<TreeBuilder> > Entry;
  BOOST_FOREACH(Entry entry, trees_) {
    shared_ptr<TreeBuilder> tree = entry.second;
    BTreeInfoPB *info = footer.add_btrees();
    Status s = tree->Finish(info);
    if (!s.ok()) {
      return s;
    }
  }

  string footer_str;
  if (!footer.SerializeToString(&footer_str)) {
    return Status::Corruption("unable to serialize footer");
  }

  PutFixed32(&footer_str, footer.GetCachedSize());
  footer_str.append(kMagicString);

  Status s = file_->Append(footer_str);
  if (!s.ok()) {
    return s;
  }

  s = file_->Flush();
  if (!s.ok()) {
    return s;
  }

  return file_->Close();
}

Status Writer::AddTree(const BTreeMetaPB &meta, shared_ptr<TreeBuilder> *tree_out) {
  CHECK(state_ == kWriterWriting) <<
    "Bad state for AddTree(): " << state_;

  if (trees_.find(meta.identifier()) != trees_.end()) {
    return Status::InvalidArgument("identifier already used");
  }

  shared_ptr<TreeBuilder> builder(new TreeBuilder(&options_, this));
  trees_[meta.identifier()] = builder;

  (*tree_out) = builder;
  return Status::OK();
}

Status Writer::AddBlock(const Slice &data, uint64_t *offset_out,
                        const string &name_for_log) {
  *offset_out = off_;
  Status s = file_->Append(data);
  if (s.ok()) {
    VLOG(1) << "Appended block " << name_for_log
            << " with " << data.size() << " bytes at " << off_;
    VLOG(2) << "trace:\n" << kudu::GetStackTrace();
    off_ += data.size();
  }
  return s;
}

Writer::~Writer() {
}

////////////////////////////////////////////////////////////
// TreeBuilder
////////////////////////////////////////////////////////////

TreeBuilder::TreeBuilder(const WriterOptions *options,
                         Writer *writer) :
  writer_(writer),
  value_block_(options),
  value_count_(0),
  options_(options)
{
  posidx_blocks_.push_back(
    shared_ptr<PosIndexBuilder>(new PosIndexBuilder(options)));
}

Status TreeBuilder::Append(IntType val) {
  value_block_.Add(val);
  value_count_++;

  size_t est_size = value_block_.EstimateEncodedSize();
  if (est_size > options_->block_size) {
    return FinishCurValueBlock();
  }
  return Status::OK();
}

Status TreeBuilder::Finish(BTreeInfoPB *info) {
  // Write out any pending values as the last
  // data block.
  if (value_block_.Count() > 0) {
    Status s = FinishCurValueBlock();
    if (!s.ok()) {
      return s;
    }
  }

  // Now do the same for the positional index blocks, starting
  // with leaf
  LOG(INFO) << "flushing tree, b-tree has " <<
    posidx_blocks_.size() << " levels";

  // Flush all but the root of the index.
  for (size_t i = 0; i < posidx_blocks_.size() - 1; i++) {
    Status s = FinishPosIndexBlockAndPropagate(i);
    if (!s.ok()) {
      return s;
    }
  }

  // Flush the root
  int root_level = posidx_blocks_.size() - 1;
  BlockPointer ptr;
  Status s = FinishPosIndexBlock(root_level, &ptr);
  if (!s.ok()) {
    LOG(ERROR) << "Unable to flush root index block";
    return s;
  }

  LOG(INFO) << "Flushed root index block: " << ptr.ToString();

  ptr.CopyToPB(info->mutable_root_block());
  return Status::OK();
}


Status TreeBuilder::FinishCurValueBlock() {
  size_t num_elems_in_block = value_block_.Count();
  OrdinalIndex first_elem_ord = value_count_ - num_elems_in_block;

  // The current data block is full, need to push it
  // into the file, and add to index
  Slice data = value_block_.Finish();
  uint64_t inserted_off;
  Status s = writer_->AddBlock(data, &inserted_off, "data");
  if (!s.ok()) {
    LOG(ERROR) << "Unable to append block to file";
    return s;
  }

  BlockPointer ptr(inserted_off, data.size());
  // Now add to the index blocks
  s = AppendPosIndexEntry(first_elem_ord, ptr);
  value_block_.Reset();

  return s;
}

// Append an entry into the positional index.
// TODO: templatize this on index type so it works for
// key indexes as well
Status TreeBuilder::AppendPosIndexEntry(
  OrdinalIndex ordinal_idx, const BlockPointer &block_ptr,
  size_t level) {

  if (level >= posidx_blocks_.size()) {
    // Need to create a new level
    CHECK(level == posidx_blocks_.size()) <<
      "trying to create level " << level << " but size is only "
                                << posidx_blocks_.size();
    VLOG(1) << "Creating level-" << level << " in index b-tree";
    posidx_blocks_.push_back(
      shared_ptr<PosIndexBuilder>(new PosIndexBuilder(options_)));
  }

  shared_ptr<PosIndexBuilder> idx_block = posidx_blocks_[level];
  idx_block->Add(ordinal_idx, block_ptr);

  size_t est_size = idx_block->EstimateEncodedSize();
  if (est_size > options_->block_size) {
    // This index block is full, flush it.
    BlockPointer index_block_ptr;
    Status s = FinishPosIndexBlockAndPropagate(level);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}


// Append the index block at the given level to the file,
// and insert it into the higher-level b-tree node, recursing
// as necessary.
// After doing so, resets this index block level so it is
// fresh.
Status TreeBuilder::FinishPosIndexBlockAndPropagate(size_t level)
{
  shared_ptr<PosIndexBuilder> idx_block = posidx_blocks_[level];

  OrdinalIndex first_in_idx_block;
  Status s = idx_block->GetFirstKey(&first_in_idx_block);
  if (!s.ok()) {
    LOG(ERROR) << "Unable to get first key of level-" << level
               << " index block" << GetStackTrace();
    return s;
  }

  // Write to file.
  BlockPointer idx_block_ptr;
  s = FinishPosIndexBlock(level, &idx_block_ptr);
  if (!s.ok()) {
    // TODO: this leaves us in a weird state, double-check that we aren't
    // leaking anything here.
    return s;
  }

  // Add to higher-level index.
  s = AppendPosIndexEntry(first_in_idx_block, idx_block_ptr,
                          level + 1);
  if (!s.ok()) {
    // TODO: this leaves us in a weird state, double-check that we aren't
    // leaking anything here.
    return s;
  }

  return Status::OK();
}

Status TreeBuilder::FinishPosIndexBlock(size_t level, BlockPointer *written)
{
  shared_ptr<PosIndexBuilder> idx_block = posidx_blocks_[level];
  Slice data = idx_block->Finish();
  uint64_t inserted_off;
  Status s = writer_->AddBlock(data, &inserted_off, "idx");
  if (!s.ok()) {
    LOG(ERROR) << "Unable to append level-" << level << " index "
               << "block to file";
    return s;
  }

  *written = BlockPointer(inserted_off, data.size());

  // Reset this level block.
  idx_block->Reset();

  return Status::OK();
}

////////////////////////////////////////////////////////////
// StringBlockBuilder
////////////////////////////////////////////////////////////

StringBlockBuilder::StringBlockBuilder(const WriterOptions *options) :
  counter_(0),
  finished_(false),
  options_(options)
{}

void StringBlockBuilder::Reset() {
  finished_ = false;
  counter_ = 0;
  buffer_.clear();
  last_val_.clear();
}

Slice StringBlockBuilder::Finish() {
  finished_ = true;
  return Slice(buffer_);
}

void StringBlockBuilder::Add(const Slice &val) {
  Slice last_val_piece(last_val_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_val_piece.size(), val.size());
    while ((shared < min_length) && (last_val_piece[shared] == val[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = val.size() - shared;

  // Add "<shared><non_shared>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);

  // Add string delta to buffer_
  buffer_.append(val.data() + shared, non_shared);

  // Update state
  last_val_.resize(shared);
  last_val_.append(val.data() + shared, non_shared);
  assert(Slice(last_val_) == val);
  counter_++;
}



////////////////////////////////////////////////////////////
// IntBlockBuilder
////////////////////////////////////////////////////////////

static size_t CalcRequiredBytes32(uint32_t i) {
  if (i == 0) return 1;

  return sizeof(long) - __builtin_clzl(i)/8;
}

IntBlockBuilder::IntBlockBuilder(const WriterOptions *options) :
  estimated_raw_size_(0),
  options_(options)
{}

void IntBlockBuilder::AppendShorterInt(
  std::string *s, uint32_t i, size_t bytes) {

  assert(bytes > 0 && bytes <= 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  // LSBs come first, so we can just reinterpret-cast
  // and set the right length
  s->append(reinterpret_cast<char *>(&i), bytes);
#else
#error dont support big endian currently
#endif
}

void IntBlockBuilder::Reset() {
  ints_.clear();
  buffer_.clear();
  estimated_raw_size_ = 0;
}

void IntBlockBuilder::Add(IntType val) {
  ints_.push_back(val);
  estimated_raw_size_ += CalcRequiredBytes32(val);
}

uint64_t IntBlockBuilder::EstimateEncodedSize() const {
  return estimated_raw_size_ + ints_.size() / 4
    + kEstimatedHeaderSizeBytes;
}

size_t IntBlockBuilder::Count() const {
  return ints_.size();
}

void IntBlockBuilder::AppendGroupVarInt32(
  std::string *s,
  uint32_t a, uint32_t b, uint32_t c, uint32_t d) {

  uint8_t a_req = CalcRequiredBytes32(a);
  uint8_t b_req = CalcRequiredBytes32(b);
  uint8_t c_req = CalcRequiredBytes32(c);
  uint8_t d_req = CalcRequiredBytes32(d);

  uint8_t prefix_byte =
    ((a_req - 1) << 6) |
    ((b_req - 1) << 4) |
    ((c_req - 1) << 2) |
    (d_req - 1);

  s->push_back(prefix_byte);
  AppendShorterInt(s, a, a_req);
  AppendShorterInt(s, b, b_req);
  AppendShorterInt(s, c, c_req);
  AppendShorterInt(s, d, d_req);
}

Slice IntBlockBuilder::Finish() {
  // TODO: negatives and big ints

  IntType min = 0;
  size_t size = ints_.size();

  if (size > 0) {
    min = *std::min_element(ints_.begin(), ints_.end());
  }

  buffer_.clear();
  AppendGroupVarInt32(&buffer_,
                      (uint32_t)size, (uint32_t)min, 0, 0);

  IntType *p = &ints_[0];
  while (size >= 4) {
    AppendGroupVarInt32(
      &buffer_,
      p[0] - min, p[1] - min, p[2] - min, p[3] - min);
    size -= 4;
    p += 4;
  }


  IntType trailer[4] = {0, 0, 0, 0};
  IntType *trailer_p = &trailer[0];

  if (size > 0) {
    p = &trailer[0];
    while (size > 0) {
      *trailer_p++ = *p++ - min;
      size--;
    }

    AppendGroupVarInt32(&buffer_, trailer[0], trailer[1], trailer[2], trailer[3]);
  }
  return Slice(buffer_);
}


}
}
