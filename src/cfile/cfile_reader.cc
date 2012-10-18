// Copyright (c) 2012, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/shared_array.hpp>
#include <glog/logging.h>

#include "block_pointer.h"
#include "cfile_reader.h"
#include "cfile.h"
#include "cfile.pb.h"
#include "index_block.h"
#include "index_btree.h"

#include "util/coding.h"
#include "util/env.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

// Magic+Length: 8-byte magic, followed by 4-byte header size
static const size_t kMagicAndLengthSize = 12;
static const size_t kMaxHeaderFooterPBSize = 64*1024;

static Status ParseMagicAndLength(const Slice &data,
                                  uint32_t *parsed_len) {
  if (data.size() != kMagicAndLengthSize) {
    return Status::Corruption("Bad size data");
  }

  if (memcmp(kMagicString.c_str(), data.data(), kMagicString.size()) != 0) {
    return Status::Corruption("bad magic");
  }

  *parsed_len = DecodeFixed32(data.data() + kMagicString.size());
  if (*parsed_len <= 0 || *parsed_len > kMaxHeaderFooterPBSize) {
    return Status::Corruption("invalid data size");
  }

  return Status::OK();
}

Status CFileReader::ReadMagicAndLength(uint64_t offset, uint32_t *len) {
  char scratch[kMagicAndLengthSize];
  Slice slice;

  RETURN_NOT_OK(file_->Read(offset, kMagicAndLengthSize,
                            &slice, scratch));

  return ParseMagicAndLength(slice, len);
}

Status CFileReader::Init() {
  CHECK(state_ == kUninitialized) <<
    "should be uninitialized before Init()";

  RETURN_NOT_OK(ReadAndParseHeader());

  RETURN_NOT_OK(ReadAndParseFooter());

  state_ = kInitialized;

  return Status::OK();
}

Status CFileReader::ReadAndParseHeader() {
  CHECK(state_ == kUninitialized) << "bad state: " << state_;

  // First read and parse the "pre-header", which lets us know
  // that it is indeed a CFile and tells us the length of the
  // proper protobuf header.
  uint32_t header_size;
  RETURN_NOT_OK(ReadMagicAndLength(0, &header_size));

  // Now read the protobuf header.
  char header_space[header_size];
  Slice header_slice;
  header_.reset(new CFileHeaderPB());

  RETURN_NOT_OK(file_->Read(kMagicAndLengthSize, header_size,
                            &header_slice, header_space));
  if (!header_->ParseFromArray(header_slice.data(), header_size)) {
    return Status::Corruption("Invalid cfile pb header");
  }

  VLOG(1) << "Read header: " << header_->DebugString();

  return Status::OK();
}


Status CFileReader::ReadAndParseFooter() {
  CHECK(state_ == kUninitialized) << "bad state: " << state_;
  CHECK(file_size_ > kMagicAndLengthSize * 2) <<
    "file too short: " << file_size_;

  // First read and parse the "post-footer", which has magic
  // and the length of the actual protobuf footer
  uint32_t footer_size;
  ReadMagicAndLength(file_size_ - kMagicAndLengthSize,
                     &footer_size);

  // Now read the protobuf footer.
  footer_.reset(new CFileFooterPB());
  char footer_space[footer_size];
  Slice footer_slice;
  uint64_t off = file_size_ - kMagicAndLengthSize - footer_size;
  RETURN_NOT_OK(file_->Read(off, footer_size, &footer_slice, footer_space));
  if (!footer_->ParseFromArray(footer_slice.data(), footer_size)) {
    return Status::Corruption("Invalid cfile pb footer");
  }

  VLOG(1) << "Read footer: " << footer_->DebugString();

  return Status::OK();
}

Status CFileReader::ReadBlock(const BlockPointer &ptr,
                              BlockData *ret) const {
  CHECK(state_ == kInitialized) << "bad state: " << state_;
  CHECK(ptr.offset() > 0 &&
        ptr.offset() + ptr.size() < file_size_) <<
    "bad offset " << ptr.ToString() << " in file of size "
                  << file_size_;

  shared_array<char> scratch(new char[ptr.size()]);
  Slice s;
  RETURN_NOT_OK( file_->Read(ptr.offset(), ptr.size(),
                             &s, scratch.get()) );

  if (s.size() != ptr.size()) {
    return Status::IOError("Could not read full block length");
  }

  *ret = BlockData(s, scratch);

  return Status::OK();
}

Status CFileReader::NewIteratorByPos(CFileIterator **iter) const {
  BlockPointer posidx_root(footer_->posidx_info().root_block());
  *iter = new CFileIterator(this, posidx_root);
  return Status::OK();
}


template <typename KeyType>
static Status SearchDownward(const CFileReader *reader,
                             const KeyType &search_key,
                             const BlockPointer &in_block,
                             BlockPointer *ret,
                             KeyType *ret_key) {
  BlockData data;
  RETURN_NOT_OK( reader->ReadBlock(in_block, &data) );

  IndexBlockReader<KeyType> idx_reader(data.slice());
  RETURN_NOT_OK(idx_reader.Parse());

  BlockPointer result;
  RETURN_NOT_OK(idx_reader.Search(search_key, &result, ret_key));

  if (idx_reader.IsLeaf()) {
    *ret = result;
    return Status::OK();
  } else {
    // Otherwise we just got a pointer to another internal node.
    // Follow it.
    return SearchDownward(reader, search_key, result, ret, ret_key);
  }
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileIterator::CFileIterator(const CFileReader *reader,
                             const BlockPointer &posidx_root) :
  reader_(reader),
  idx_iter_(new IndexTreeIterator<uint32_t>(reader, posidx_root)),
  seeked_(false)
{
}

Status CFileIterator::SeekToOrdinal(uint32_t ord_idx) {
  seeked_ = false;

  RETURN_NOT_OK(idx_iter_->SeekAtOrBefore(ord_idx));

  // TODO: fast seek within block (without reseeking index)
  ReadCurrentDataBlock();

  // If the data block doesn't actually contain the data
  // we're looking for, then we're probably in the last
  // block in the file.
  // TODO: could assert that each of the index layers is
  // at its last entry (ie HasNext() is false for each)
  if (ord_idx >= dblk_->ordinal_pos() + dblk_->Count()) {
    return Status::NotFound("trying to seek past highest ordinal in file");
  }

  // Seek data block to correct index
  DCHECK(ord_idx >= dblk_->ordinal_pos() &&
         ord_idx < dblk_->ordinal_pos() + dblk_->Count())
    << "got wrong data block. looking for ord_idx=" << ord_idx
    << " but dblk spans " << dblk_->ordinal_pos() << "-"
    << (dblk_->ordinal_pos() + dblk_->Count());
  dblk_->SeekToPositionInBlock(ord_idx - dblk_->ordinal_pos());

  DCHECK(ord_idx == dblk_->ordinal_pos()) <<
    "failed seek, aimed for " << ord_idx << " got to " <<
    dblk_->ordinal_pos();

  seeked_ = true;
  return Status::OK();
}

uint32_t CFileIterator::GetCurrentOrdinal() const {
  CHECK(seeked_) << "not seeked";

  return dblk_->ordinal_pos();
}

Status CFileIterator::ReadCurrentDataBlock() {
  BlockPointer dblk_ptr = idx_iter_->GetCurrentBlockPointer();
  RETURN_NOT_OK(reader_->ReadBlock(dblk_ptr, &dblk_data_));

  dblk_.reset(new IntBlockDecoder(dblk_data_.slice()));
  RETURN_NOT_OK(dblk_->ParseHeader());
  return Status::OK();
}

bool CFileIterator::HasNext() {
  CHECK(seeked_) << "not seeked";

  return dblk_->HasNext() || idx_iter_->HasNext();
}

Status CFileIterator::GetNextValues(
  int n, void *out, int *returned)
{
  CHECK(seeked_) << "not seeked";

  *returned = 0;

  while (n > 0) {
    // Fetch as many as we can from the current datablock.

    // TODO: have GetNextValues return count returned?
    int returned_in_batch = dblk_->GetNextValues(n, out);
    n -= returned_in_batch;
    *returned += returned_in_batch;

    // If we didn't fetch as many as requested, then it should
    // be because the current data block ran out.
    if (n > 0) {
      DCHECK(!dblk_->HasNext()) <<
        "dblk stopped yielding values before it was empty";
    }

    // Pull in next datablock.
    Status s = idx_iter_->Next();

    if (s.IsNotFound()) {
      // No next datablock

      if (*returned == 0) {
        // No more data, and this call didn't return any
        return s;
      } else {
        // Otherwise we did successfully add some to the vector.
        return Status::OK();
      }
    } else if (!s.ok()) {
      return s; // actual error
    }

    // Fill in the data for the next block.
    RETURN_NOT_OK(ReadCurrentDataBlock());
  }

  return Status::OK();
}



} // namespace cfile
} // namespace kudu
