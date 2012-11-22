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
#include "util/hexdump.h"

using std::string;

namespace kudu { namespace cfile {

const string kMagicString = "kuducfil";


////////////////////////////////////////////////////////////
// Options
////////////////////////////////////////////////////////////
WriterOptions::WriterOptions() :
  block_size(256*1024),
  block_restart_interval(16),
  write_posidx(false),
  write_validx(false)
{}


////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////


Writer::Writer(const WriterOptions &options,
               DataType type,
               EncodingType encoding,
               shared_ptr<WritableFile> file) :
  file_(file),
  off_(0),
  value_count_(0),
  options_(options),
  datatype_(type),
  typeinfo_(GetTypeInfo(type)),
  encoding_type_(encoding),
  state_(kWriterInitialized)
{
  if (options.write_posidx) {
    posidx_builder_.reset(new IndexTreeBuilder(&options_,
                                               UINT32,
                                               this));
  }

  if (options.write_validx) {
    validx_builder_.reset(new IndexTreeBuilder(&options_,
                                               datatype_,
                                               this));
  }
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

  BlockBuilder *bb;
  RETURN_NOT_OK( CreateBlockBuilder(&bb) );
  data_block_.reset(bb);

  state_ = kWriterWriting;

  return Status::OK();
}

// TODO: refactor this into some kind of block factory
// module, with its equivalent in CFileReader
Status Writer::CreateBlockBuilder(BlockBuilder **bb) const {
  *bb = NULL;
  switch (datatype_) {
    case UINT32:
      switch (encoding_type_) {
        case GROUP_VARINT:
          *bb = new IntBlockBuilder(&options_);
          break;
        default:
          return Status::NotFound("bad int encoding");
      }
      break;
    case STRING:
      switch (encoding_type_) {
        case PREFIX:
          // TODO: this should be called PREFIX_DELTA or something
          *bb = new StringBlockBuilder(&options_);
          break;
        default:
          return Status::NotFound("bad string encoding");
      }
      break;
    default:
      return Status::NotFound("bad datatype");
  }

  CHECK(*bb != NULL); // sanity check postcondition
  return Status::OK();
}

Status Writer::Finish() {
  CHECK(state_ == kWriterWriting) <<
    "Bad state for Finish(): " << state_;

  // Write out any pending values as the last data block.
  RETURN_NOT_OK(FinishCurDataBlock());

  // Start preparing the footer.
  CFileFooterPB footer;
  footer.set_data_type(datatype_);
  footer.set_encoding(encoding_type_);

  // Write out any pending positional index blocks.
  if (options_.write_posidx) {
    BTreeInfoPB posidx_info;
    posidx_builder_->Finish(&posidx_info);
    footer.mutable_posidx_info()->CopyFrom(posidx_info);
  }

  if (options_.write_validx) {
    BTreeInfoPB validx_info;
    validx_builder_->Finish(&validx_info);
    footer.mutable_validx_info()->CopyFrom(validx_info);
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

Status Writer::AppendEntries(const void *entries, int count) {
  int rem = count;

  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(entries);

  while (rem > 0) {
    int n = data_block_->Add(ptr, rem);
    DCHECK_GE(n, 0);

    ptr += typeinfo_.size() * n;
    rem -= n;
    value_count_ += n;

    size_t est_size = data_block_->EstimateEncodedSize();
    if (est_size > options_.block_size) {
      RETURN_NOT_OK(FinishCurDataBlock());
    }
  }

  DCHECK_EQ(rem, 0);
  return Status::OK();
}

Status Writer::FinishCurDataBlock() {
  size_t num_elems_in_block = data_block_->Count();
  if (num_elems_in_block == 0) {
    return Status::OK();
  }

  OrdinalIndex first_elem_ord = value_count_ - num_elems_in_block;

  VLOG(1) << "Appending data block for values " <<
    first_elem_ord << "-" << (first_elem_ord + num_elems_in_block);

  // The current data block is full, need to push it
  // into the file, and add to index
  Slice data = data_block_->Finish((uint32_t)first_elem_ord);
  uint64_t inserted_off;
  VLOG(2) << "estimated size=" << data_block_->EstimateEncodedSize()
          << " actual=" << data.size();

  Status s = AddBlock(data, &inserted_off, "data");
  if (!s.ok()) {
    LOG(ERROR) << "Unable to append block to file";
    return s;
  }

  BlockPointer ptr(inserted_off, data.size());

  // Now add to the index blocks
  if (posidx_builder_ != NULL) {
    RETURN_NOT_OK(posidx_builder_->Append(&first_elem_ord, ptr));
  }

  if (validx_builder_ != NULL) {
    // Allocate a single datum on the stack.
    char tmp[typeinfo_.size()];

    RETURN_NOT_OK(data_block_->GetFirstKey(tmp));
    VLOG(1) << "Appending validx entry\n" <<
      kudu::HexDump(Slice(tmp, typeinfo_.size()));
    s = validx_builder_->Append(tmp, ptr);
  }

  data_block_->Reset();

  return s;
}

Status Writer::AddBlock(const Slice &data, uint64_t *offset_out,
                        const char *name_for_log) {
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


}
}
