// Copyright (c) 2012, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "cfile/block_cache.h"
#include "cfile/block_pointer.h"
#include "cfile/cfile_reader.h"
#include "cfile/cfile.h"
#include "cfile/cfile.pb.h"
#include "cfile/gvint_block.h"
#include "cfile/index_block.h"
#include "cfile/index_btree.h"
#include "cfile/string_prefix_block.h"
#include "gutil/gscoped_ptr.h"
#include "util/coding.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/object_pool.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

// Magic+Length: 8-byte magic, followed by 4-byte header size
static const size_t kMagicAndLengthSize = 12;
static const size_t kMaxHeaderFooterPBSize = 64*1024;

static const size_t kBlockSizeLimit = 16 * 1024 * 1024; // 16MB

static Status ParseMagicAndLength(const Slice &data,
                                  uint32_t *parsed_len) {
  if (data.size() != kMagicAndLengthSize) {
    return Status::Corruption("Bad size data");
  }

  if (memcmp(kMagicString, data.data(), strlen(kMagicString)) != 0) {
    return Status::Corruption("bad magic");
  }

  *parsed_len = DecodeFixed32(data.data() + strlen(kMagicString));
  if (*parsed_len <= 0 || *parsed_len > kMaxHeaderFooterPBSize) {
    return Status::Corruption("invalid data size");
  }

  return Status::OK();
}

CFileReader::CFileReader(const ReaderOptions &options,
                         const shared_ptr<RandomAccessFile> &file,
                         uint64_t file_size) :
  options_(options),
  file_(file),
  file_size_(file_size),
  state_(kUninitialized),
  cache_(BlockCache::GetSingleton()),
  cache_id_(cache_->GenerateFileId())
{
}


Status CFileReader::Open(Env *env, const string &path,
                         const ReaderOptions &options,
                         gscoped_ptr<CFileReader> *reader) {
  shared_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env, path, &file));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(path, &size));

  gscoped_ptr<CFileReader> reader_local(new CFileReader(options, file, size));
  RETURN_NOT_OK(reader_local->Init());
  reader->reset(reader_local.release());
  return Status::OK();
}


Status CFileReader::ReadMagicAndLength(uint64_t offset, uint32_t *len) {
  uint8_t scratch[kMagicAndLengthSize];
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

  type_info_ = &GetTypeInfo(footer_->data_type());
  VLOG(1) << "Initialized CFile reader. "
          << "Header: " << header_->DebugString()
          << " Footer: " << footer_->DebugString()
          << " Type: " << type_info_->name();

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
  uint8_t header_space[header_size];
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
  uint8_t footer_space[footer_size];
  Slice footer_slice;
  uint64_t off = file_size_ - kMagicAndLengthSize - footer_size;
  RETURN_NOT_OK(file_->Read(off, footer_size, &footer_slice, footer_space));
  if (!footer_->ParseFromArray(footer_slice.data(), footer_size)) {
    return Status::Corruption("Invalid cfile pb footer");
  }

  // Verify if the compression codec is available
  if (footer_->compression() != NO_COMPRESSION) {
    shared_ptr<CompressionCodec> compression_codec;
    RETURN_NOT_OK(GetCompressionCodec(footer_->compression(), &compression_codec));
    block_uncompressor_.reset(new CompressedBlockDecoder(compression_codec, kBlockSizeLimit));
  }

  VLOG(1) << "Read footer: " << footer_->DebugString();

  return Status::OK();
}

// Some of the File implementations from LevelDB attempt to be tricky
// and just return a Slice into an mmapped region (or in-memory region).
// But, this is hard to program against in terms of cache management, etc,
// and in practice won't be useful in a libhdfs context.
//
// This function detects this case, where slice->data() doesn't match
// the given buffer, and if so, memcpys the data into the buffer and
// adjusts the Slice accordingly
static void MatchReadSliceWithBuffer(Slice *slice,
                                     uint8_t *buffer) {
  if (slice->data() != buffer) {
    memcpy(buffer, slice->data(), slice->size());
    *slice = Slice(buffer, slice->size());
  }
}

Status CFileReader::ReadBlock(const BlockPointer &ptr,
                              BlockCacheHandle *ret) const {
  CHECK(state_ == kInitialized) << "bad state: " << state_;
  CHECK(ptr.offset() > 0 &&
        ptr.offset() + ptr.size() < file_size_) <<
    "bad offset " << ptr.ToString() << " in file of size "
                  << file_size_;

  if (cache_->Lookup(cache_id_, ptr.offset(), ret)) {
    // Cache hit
    return Status::OK();
  }

  // Cache miss: need to read ourselves.
  gscoped_array<uint8_t> scratch(new uint8_t[ptr.size()]);
  Slice block;
  RETURN_NOT_OK( file_->Read(ptr.offset(), ptr.size(),
                             &block, scratch.get()) );
  if (block.size() != ptr.size()) {
    return Status::IOError("Could not read full block length");
  }

  // Decompress the block
  if (block_uncompressor_ != NULL) {
    Slice ublock;
    Status s = block_uncompressor_->Uncompress(block, &ublock);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to uncompress block at " << ptr.offset()
                   << " of size " << ptr.size() << ": " << s.ToString();
      return s;
    }

    // free scratch, now that we have the uncompressed block
    block = ublock;
    scratch.reset(NULL);
  } else {
    MatchReadSliceWithBuffer(&block, scratch.get());
  }

  cache_->Insert(cache_id_, ptr.offset(), block, ret);

  // The cache now has ownership over the memory, so release
  // the scoped pointer.
  ignore_result(scratch.release());

  return Status::OK();
}

Status CFileReader::CountRows(rowid_t *count) const {
  CHECK_EQ(state_, kInitialized);
  *count = footer_->num_values();
  return Status::OK();
}

bool CFileReader::GetMetadataEntry(const string &key, string *val) {
  CHECK_EQ(state_, kInitialized);
  BOOST_FOREACH(const FileMetadataPairPB &pair, header_->metadata()) {
    if (pair.key() == key) {
      *val = pair.value();
      return true;
    }
  }
  BOOST_FOREACH(const FileMetadataPairPB &pair, footer_->metadata()) {
    if (pair.key() == key) {
      *val = pair.value();
      return true;
    }
  }
  return false;
}


// TODO: perhaps decoders should be able to be Reset
// to point to a different slice? any benefit to that?
Status CFileReader::CreateBlockDecoder(
  BlockDecoder **bd, const Slice &slice) const {
  *bd = NULL;
  switch (footer_->data_type()) {
    case UINT32:
      switch (footer_->encoding()) {
        case GROUP_VARINT:
          *bd = new GVIntBlockDecoder(slice);
          break;
        default:
          return Status::NotFound("bad int encoding");
      }
      break;
    case STRING:
      switch (footer_->encoding()) {
        case PREFIX:
          *bd = new StringPrefixBlockDecoder(slice);
          break;
        default:
          return Status::NotFound("bad string encoding");
      }
      break;
    default:
      return Status::NotFound("bad datatype");
  }

  CHECK(*bd != NULL); // sanity check postcondition
  return Status::OK();
}

Status CFileReader::NewIterator(CFileIterator **iter) const {
  gscoped_ptr<BlockPointer> posidx_root;
  if (footer_->has_posidx_info()) {
    posidx_root.reset(new BlockPointer(footer_->posidx_info().root_block()));
  }

  // If there is a value index in the file, pass it to the iterator
  gscoped_ptr<BlockPointer> validx_root;
  if (footer_->has_validx_info()) {
    validx_root.reset(new BlockPointer(footer_->validx_info().root_block()));
  }

  *iter = new CFileIterator(this, posidx_root.get(), validx_root.get());
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileIterator::CFileIterator(const CFileReader *reader,
                             const BlockPointer *posidx_root,
                             const BlockPointer *validx_root) :
  reader_(reader),
  seeked_(NULL),
  prepared_(false),
  last_prepare_idx_(-1),
  last_prepare_count_(-1)
{
  if (posidx_root != NULL) {
    posidx_iter_.reset(IndexTreeIterator::Create(
                         reader, UINT32, *posidx_root));
  }

  if (validx_root != NULL) {
    validx_iter_.reset(IndexTreeIterator::Create(
                         reader, reader->type_info()->type(), *validx_root));
  }
}

CFileIterator::IOStatistics::IOStatistics() :
  data_blocks_read(0),
  rows_read(0)
{}

string CFileIterator::IOStatistics::ToString() const {
  return StringPrintf("data_blocks_read=%d rows_read=%ld",
                      data_blocks_read, rows_read);
}

Status CFileIterator::SeekToOrdinal(rowid_t ord_idx) {
  Unseek();
  if (PREDICT_FALSE(posidx_iter_ == NULL)) {
    return Status::NotSupported("no positional index in file");
  }

  RETURN_NOT_OK(posidx_iter_->SeekAtOrBefore(&ord_idx));

  // TODO: fast seek within block (without reseeking index)
  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*posidx_iter_, b.get()));

  // If the data block doesn't actually contain the data
  // we're looking for, then we're probably in the last
  // block in the file.
  // TODO: could assert that each of the index layers is
  // at its last entry (ie HasNext() is false for each)
  if (PREDICT_FALSE(ord_idx > b->last_row_idx())){
    return Status::NotFound("trying to seek past highest ordinal in file");
  }

  // Seek data block to correct index
  DCHECK(ord_idx >= b->first_row_idx_ &&
         ord_idx <= b->last_row_idx())
    << "got wrong data block. looking for ord_idx=" << ord_idx
    << " but got dblk " << b->ToString();
  b->dblk_->SeekToPositionInBlock(ord_idx - b->first_row_idx_);

  DCHECK_EQ(ord_idx, b->dblk_->ordinal_pos()) << "failed seek";

  prepared_blocks_.push_back(b.release());
  last_prepare_idx_ = ord_idx;
  last_prepare_count_ = 0;
  seeked_ = posidx_iter_.get();
  return Status::OK();
}

Status CFileIterator::SeekAtOrAfter(const void *key,
                                    bool *exact_match) {
  Unseek();
  if (PREDICT_FALSE(validx_iter_ == NULL)) {
    return Status::NotSupported("no value index present");
  }

  Status s = validx_iter_->SeekAtOrBefore(key);
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seeking to a value before the first value in the file
    // will return NotFound, due to the way the index seek
    // works. We need to special-case this and have the
    // iterator seek all the way down its leftmost branches
    // to get the correct reslt.
    s = validx_iter_->SeekToFirst();
  }
  RETURN_NOT_OK(s);

  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*validx_iter_, b.get()));

  RETURN_NOT_OK(b->dblk_->SeekAtOrAfterValue(key, exact_match));

  last_prepare_idx_ = b->dblk_->ordinal_pos();
  last_prepare_count_ = 0;

  prepared_blocks_.push_back(b.release());

  seeked_ = validx_iter_.get();
  return Status::OK();
}

void CFileIterator::Unseek() {
  seeked_ = NULL;
  BOOST_FOREACH(PreparedBlock *pb, prepared_blocks_) {
    prepared_block_pool_.Destroy(pb);
  }
  prepared_blocks_.clear();
}

rowid_t CFileIterator::GetCurrentOrdinal() const {
  CHECK(seeked_) << "not seeked";
  DCHECK(!prepared_blocks_.empty());

  return prepared_blocks_.front()->dblk_->ordinal_pos();
}

string CFileIterator::PreparedBlock::ToString() const {
  return StringPrintf("dblk(%s, rows=%d-%d)",
                      dblk_ptr_.ToString().c_str(),
                      first_row_idx_,
                      last_row_idx());
}

Status CFileIterator::ReadCurrentDataBlock(const IndexTreeIterator &idx_iter,
                                           PreparedBlock *prep_block) {
  prep_block->dblk_ptr_ = idx_iter.GetCurrentBlockPointer();
  RETURN_NOT_OK(reader_->ReadBlock(prep_block->dblk_ptr_, &prep_block->dblk_data_));

  io_stats_.data_blocks_read++;

  BlockDecoder *bd;
  RETURN_NOT_OK(reader_->CreateBlockDecoder(
                  &bd, prep_block->dblk_data_.data()));
  prep_block->dblk_.reset(bd);
  RETURN_NOT_OK(prep_block->dblk_->ParseHeader());

  io_stats_.rows_read += bd->Count();

  prep_block->first_row_idx_ = prep_block->dblk_->ordinal_pos();

  prep_block->needs_rewind_ = false;
  prep_block->rewind_idx_ = 0;

  DVLOG(2) << "Read dblk " << prep_block->ToString();

  return Status::OK();
}

Status CFileIterator::QueueCurrentDataBlock(const IndexTreeIterator &idx_iter) {
  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(idx_iter, b.get()));
  prepared_blocks_.push_back(b.release());
  return Status::OK();
}

bool CFileIterator::HasNext() const {
  CHECK(seeked_) << "not seeked";
  CHECK(!prepared_) << "Cannot call HasNext() mid-batch";

  return !prepared_blocks_.empty() || posidx_iter_->HasNext();
}

Status CFileIterator::PrepareBatch(size_t *n) {
  CHECK(!prepared_) << "Should call FinishBatch() first";
  CHECK(seeked_ != NULL) << "must be seeked";

  CHECK(!prepared_blocks_.empty());

  rowid_t start_idx = last_prepare_idx_ + last_prepare_count_;
  rowid_t end_idx = start_idx + *n;

  // Read blocks until all blocks covering the requested range are in the
  // prepared_blocks_ queue.
  while (prepared_blocks_.back()->last_row_idx() < end_idx) {
    Status s = seeked_->Next();
    if (PREDICT_FALSE(s.IsNotFound())) {
      VLOG(1) << "Reached EOF";
      break;
    } else if (!s.ok()) {
      return s;
    }
    RETURN_NOT_OK(QueueCurrentDataBlock(*seeked_));
  }

  // Seek the first block in the queue such that the first value to be read
  // corresponds to start_idx
  {
    PreparedBlock *front = prepared_blocks_.front();
    front->rewind_idx_ = start_idx - front->first_row_idx_;
    front->needs_rewind_ = true;
  }

  uint32_t size_covered_by_prep_blocks = prepared_blocks_.back()->last_row_idx() - start_idx + 1;
  if (PREDICT_FALSE(size_covered_by_prep_blocks < *n)) {
    *n = size_covered_by_prep_blocks;
  }

  last_prepare_idx_ = start_idx;
  last_prepare_count_ = *n;
  prepared_ = true;

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    VLOG(1) << "Prepared for " << (*n) << " rows"
            << " (" << start_idx << "-" << (start_idx + *n - 1) << ")";
    BOOST_FOREACH(PreparedBlock *b, prepared_blocks_) {
      VLOG(1) << "  " << b->ToString();
    }
    VLOG(1) << "-------------";
  }



  return Status::OK();
}

Status CFileIterator::FinishBatch() {
  CHECK(prepared_) << "no batch prepared";
  prepared_ = false;

  DVLOG(1) << "Finishing batch " << last_prepare_idx_ << "-" << (last_prepare_idx_ + last_prepare_count_ - 1);

  // Release all blocks except for the last one, which may still contain
  // relevent data for the next batch.
  for (int i = 0; i < prepared_blocks_.size() - 1; i++) {
    PreparedBlock *b = prepared_blocks_[i];
    prepared_block_pool_.Destroy(b);
  }

  PreparedBlock *back = prepared_blocks_.back();
  DVLOG(1) << "checking last block " << back->ToString() << " vs "
           << last_prepare_idx_ << " + " << last_prepare_count_
           << " (" << (last_prepare_idx_ + last_prepare_count_) << ")";
  if (back->last_row_idx() < last_prepare_idx_ + last_prepare_count_) {
    // Last block is irrelevant
    prepared_block_pool_.Destroy(back);
    prepared_blocks_.clear();
  } else {
    prepared_blocks_[0] = back;
    prepared_blocks_.resize(1);
  }

  #ifndef NDEBUG
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Left around following blocks:";
    BOOST_FOREACH(PreparedBlock *b, prepared_blocks_) {
      VLOG(1) << "  " << b->ToString();
    }
    VLOG(1) << "-------------";
  }
  #endif

  return Status::OK();
}


Status CFileIterator::Scan(ColumnBlock *dst)
{
  CHECK(seeked_) << "not seeked";

  // Make a local copy of the destination block so we can
  // Advance it as we read into it.
  ColumnBlock remaining_dst = *dst;

  size_t rem = last_prepare_count_;
  DCHECK_LE(rem, dst->size());

  BOOST_FOREACH(PreparedBlock *pb, prepared_blocks_) {
    // Fetch as many as we can from the current datablock.

    size_t this_batch = rem;

    if (pb->needs_rewind_) {
      // Seek back to the saved position.
      pb->dblk_->SeekToPositionInBlock(pb->rewind_idx_);
      // TODO: we could add a mark/reset like interface in BlockDecoder interface
      // that might be more efficient (allowing the decoder to save internal state
      // instead of having to reconstruct it)
    }
    RETURN_NOT_OK(pb->dblk_->CopyNextValues(&this_batch, &remaining_dst));

    pb->needs_rewind_ = true;

    DCHECK_LE(this_batch, rem);
    rem -= this_batch;
    remaining_dst.Advance(this_batch);

    // If we didn't fetch as many as requested, then it should
    // be because the current data block ran out.
    if (rem > 0) {
      DCHECK_EQ(pb->dblk_->ordinal_pos(), pb->last_row_idx() + 1) <<
        "dblk stopped yielding values before it was empty";
    } else {
      break;
    }
  }

  DCHECK_EQ(rem, 0) << "Should have fetched exactly the number of prepared rows";

  return Status::OK();
}

Status CFileIterator::CopyNextValues(size_t *n, ColumnBlock *cb) {
  RETURN_NOT_OK(PrepareBatch(n));
  RETURN_NOT_OK(Scan(cb));
  RETURN_NOT_OK(FinishBatch());
  return Status::OK();
}


} // namespace cfile
} // namespace kudu
