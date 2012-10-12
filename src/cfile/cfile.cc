// Copyright (c) 2012, Cloudera, inc

#include <boost/foreach.hpp>
#include <endian.h>
#include <string>
#include <glog/logging.h>

#include "cfile.h"
#include "cfile.pb.h"
#include "block_pointer.h"
#include "index_block.h"
#include "index_btree.h"
#include "util/env.h"
#include "util/coding.h"
#include "util/logging.h"

using std::string;

namespace kudu { namespace cfile {

const string kMagicString = "kuducfil";

// TODO: should use a proto enum instead probably
// TODO: weird conflation of columns and index trees (a given column can have 2 trees)
// fix this weird n->1 relationship of types of data per file, or change to true PAX
const string kPositionalIndexIdentifier = "posidx";


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

  CFileHeaderPB header;
  header.set_major_version(kCFileMajorVersion);
  header.set_minor_version(kCFileMinorVersion);
  uint32_t pb_size = header.ByteSize();


  string buf;
  // First the magic.
  buf.append(kMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, pb_size);
  if (!header.AppendToString(&buf)) {
    return Status::Corruption("unable to encode header");
  }

  file_->Append(Slice(buf));
  off_ += buf.size();
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
    RETURN_NOT_OK(tree->Finish(info));

    // TODO: should track through the whole metadata object,
    // not just use the identifier here
    info->mutable_metadata()->set_identifier(entry.first);
  }

  string footer_str;
  if (!footer.SerializeToString(&footer_str)) {
    return Status::Corruption("unable to serialize footer");
  }

  footer_str.append(kMagicString);
  PutFixed32(&footer_str, footer.GetCachedSize());

  RETURN_NOT_OK(file_->Append(footer_str));
  RETURN_NOT_OK(file_->Flush());

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
  options_(options),
  writer_(writer),
  value_block_(options),
  posidx_builder_(new IndexTreeBuilder<uint32_t>(options, writer)),
  value_count_(0)
{
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
    RETURN_NOT_OK(FinishCurValueBlock());
  }

  return posidx_builder_->Finish(info);
}


Status TreeBuilder::FinishCurValueBlock() {
  size_t num_elems_in_block = value_block_.Count();
  OrdinalIndex first_elem_ord = value_count_ - num_elems_in_block;

  VLOG(1) << "Appending data block for values " <<
    first_elem_ord << "-" << (first_elem_ord + num_elems_in_block);

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
  s = posidx_builder_->Append(first_elem_ord, ptr);
  value_block_.Reset();

  return s;
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




}
}
