// Copyright (c) 2012, Cloudera, inc.

#include "cfile/cfile_reader.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include <algorithm>

#include "cfile/block_cache.h"
#include "cfile/block_pointer.h"
#include "cfile/cfile.h"
#include "cfile/cfile.pb.h"
#include "cfile/gvint_block.h"
#include "cfile/index_block.h"
#include "cfile/index_btree.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/mathlimits.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/object_pool.h"
#include "util/rle-encoding.h"
#include "util/slice.h"
#include "util/status.h"

using strings::Substitute;

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
  cache_id_(cache_->GenerateFileId()) {
}


Status CFileReader::Open(Env *env, const string &path,
                         const ReaderOptions &options,
                         gscoped_ptr<CFileReader> *reader) {
  shared_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env, path, &file));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(path, &size));
  return Open(file, size, options, reader);
}

Status CFileReader::Open(const shared_ptr<RandomAccessFile>& file,
                         uint64_t file_size,
                         const ReaderOptions& options,
                         gscoped_ptr<CFileReader> *reader) {
  gscoped_ptr<CFileReader> reader_local(new CFileReader(options, file, file_size));
  RETURN_NOT_OK(reader_local->Init());
  reader->reset(reader_local.release());
  return Status::OK();
}


Status CFileReader::ReadMagicAndLength(uint64_t offset, uint32_t *len) {
  uint8_t scratch[kMagicAndLengthSize];
  Slice slice;

  RETURN_NOT_OK(env_util::ReadFully(file_.get(), offset, kMagicAndLengthSize,
                                    &slice, scratch));

  return ParseMagicAndLength(slice, len);
}

Status CFileReader::Init() {
  CHECK(state_ == kUninitialized) <<
    "should be uninitialized before Init()";

  RETURN_NOT_OK(ReadAndParseHeader());

  RETURN_NOT_OK(ReadAndParseFooter());

  type_info_ = &GetTypeInfo(footer_->data_type());

  RETURN_NOT_OK(TypeEncodingInfo::Get(footer_->data_type(),
                                      footer_->encoding(),
                                      &type_encoding_info_));

  key_encoder_ = &GetKeyEncoder(footer_->data_type());
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

  RETURN_NOT_OK(env_util::ReadFully(file_.get(), kMagicAndLengthSize, header_size,
                                    &header_slice, header_space));
  if (!header_->ParseFromArray(header_slice.data(), header_size)) {
    return Status::Corruption("Invalid cfile pb header");
  }

  VLOG(1) << "Read header: " << header_->DebugString();

  return Status::OK();
}


Status CFileReader::ReadAndParseFooter() {
  CHECK(state_ == kUninitialized) << "bad state: " << state_;
  CHECK_GT(file_size_, kMagicAndLengthSize) <<
    "file too short: " << file_size_;

  // First read and parse the "post-footer", which has magic
  // and the length of the actual protobuf footer
  uint32_t footer_size;
  RETURN_NOT_OK_PREPEND(ReadMagicAndLength(file_size_ - kMagicAndLengthSize, &footer_size),
                        "Failed to read magic and length from end of file");

  // Now read the protobuf footer.
  footer_.reset(new CFileFooterPB());
  uint8_t footer_space[footer_size];
  Slice footer_slice;
  uint64_t off = file_size_ - kMagicAndLengthSize - footer_size;
  RETURN_NOT_OK(env_util::ReadFully(file_.get(), off, footer_size, &footer_slice, footer_space));
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
  RETURN_NOT_OK(env_util::ReadFully(file_.get(), ptr.offset(), ptr.size(),
                                    &block, scratch.get()));
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
// Default Column Value Iterator
////////////////////////////////////////////////////////////
Status DefaultColumnValueIterator::SeekToOrdinal(rowid_t ord_idx) {
  ordinal_ = ord_idx;
  return Status::OK();
}

Status DefaultColumnValueIterator::PrepareBatch(size_t *n) {
  batch_ = *n;
  return Status::OK();
}

Status DefaultColumnValueIterator::Scan(ColumnBlock *dst)  {
  if (dst->is_nullable()) {
    ColumnDataView dst_view(dst);
    dst_view.SetNullBits(dst->nrows(), value_ != NULL);
  }
  if (value_ != NULL) {
    if (type_ == STRING) {
      const Slice *src_slice = reinterpret_cast<const Slice *>(value_);
      Slice dst_slice;
      if (PREDICT_FALSE(!dst->arena()->RelocateSlice(*src_slice, &dst_slice))) {
        return Status::IOError("out of memory copying slice", src_slice->ToString());
      }
      for (size_t i = 0; i < dst->nrows(); ++i) {
        dst->SetCellValue(i, &dst_slice);
      }
    } else {
      for (size_t i = 0; i < dst->nrows(); ++i) {
        dst->SetCellValue(i, value_);
      }
    }
  }
  io_stats_.rows_read += dst->nrows();
  return Status::OK();
}

Status DefaultColumnValueIterator::FinishBatch() {
  ordinal_ += batch_;
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileIterator::CFileIterator(const CFileReader *reader,
                             const BlockPointer *posidx_root,
                             const BlockPointer *validx_root)
  : reader_(reader),
    seeked_(NULL),
    prepared_(false),
    last_prepare_idx_(-1),
    last_prepare_count_(-1) {
  if (posidx_root != NULL) {
    posidx_iter_.reset(IndexTreeIterator::Create(
                         reader, UINT32, *posidx_root));
  }

  if (validx_root != NULL) {
    validx_iter_.reset(IndexTreeIterator::Create(
                         reader, reader->type_info()->type(), *validx_root));
  }
}

CFileIterator::IOStatistics::IOStatistics()
 : data_blocks_read(0),
   rows_read(0) {
}

string CFileIterator::IOStatistics::ToString() const {
  return StringPrintf("data_blocks_read=%d rows_read=%ld",
                      data_blocks_read, rows_read);
}

Status CFileIterator::SeekToOrdinal(rowid_t ord_idx) {
  Unseek();
  if (PREDICT_FALSE(posidx_iter_ == NULL)) {
    return Status::NotSupported("no positional index in file");
  }

  tmp_buf_.clear();
  KeyEncoderTraits<UINT32>::Encode(ord_idx, &tmp_buf_);
  RETURN_NOT_OK(posidx_iter_->SeekAtOrBefore(Slice(tmp_buf_)));

  // TODO: fast seek within block (without reseeking index)
  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*posidx_iter_, b.get()));

  // If the data block doesn't actually contain the data
  // we're looking for, then we're probably in the last
  // block in the file.
  // TODO: could assert that each of the index layers is
  // at its last entry (ie HasNext() is false for each)
  if (PREDICT_FALSE(ord_idx > b->last_row_idx())) {
    return Status::NotFound("trying to seek past highest ordinal in file");
  }

  // Seek data block to correct index
  DCHECK(ord_idx >= b->first_row_idx() &&
         ord_idx <= b->last_row_idx())
    << "got wrong data block. looking for ord_idx=" << ord_idx
    << " but got dblk " << b->ToString();
         SeekToPositionInBlock(b.get(), ord_idx - b->first_row_idx());

  prepared_blocks_.push_back(b.release());
  last_prepare_idx_ = ord_idx;
  last_prepare_count_ = 0;
  seeked_ = posidx_iter_.get();

  CHECK_EQ(ord_idx, GetCurrentOrdinal());
  return Status::OK();
}

void CFileIterator::SeekToPositionInBlock(PreparedBlock *pb, uint32_t idx_in_block) {
  // Since the data block only holds the non-null values,
  // we need to translate from 'ord_idx' (the absolute row id)
  // to the index within the non-null entries.
  uint32_t index_within_nonnulls;
  if (reader_->is_nullable()) {
    if (PREDICT_TRUE(pb->idx_in_block_ <= idx_in_block)) {
      // We are seeking forward. Skip from the current position in the RLE decoder
      // instead of going back to the beginning of the block.
      uint32_t nskip = idx_in_block - pb->idx_in_block_;
      size_t cur_blk_idx = pb->dblk_->GetCurrentIndex();
      index_within_nonnulls = cur_blk_idx + pb->rle_decoder_.Skip(nskip);
    } else {
      // Seek backward - have to start from the start of the block.
      pb->rle_decoder_ = RleDecoder<bool>(pb->rle_bitmap.data(), pb->rle_bitmap.size(), 1);
      index_within_nonnulls = pb->rle_decoder_.Skip(idx_in_block);
    }
  } else {
    index_within_nonnulls = idx_in_block;
  }

  pb->dblk_->SeekToPositionInBlock(index_within_nonnulls);
  DCHECK_EQ(index_within_nonnulls, pb->dblk_->GetCurrentIndex()) << "failed seek";
  pb->idx_in_block_ = idx_in_block;
}

Status CFileIterator::SeekToFirst() {
  Unseek();
  IndexTreeIterator *idx_iter;
  if (PREDICT_TRUE(posidx_iter_ != NULL)) {
    RETURN_NOT_OK(posidx_iter_->SeekToFirst());
    idx_iter = posidx_iter_.get();
  } else if (PREDICT_TRUE(validx_iter_ != NULL)) {
    RETURN_NOT_OK(validx_iter_->SeekToFirst());
    idx_iter = validx_iter_.get();
  } else {
    return Status::NotSupported("no value or positional index present");
  }

  pblock_pool_scoped_ptr b = prepared_block_pool_.make_scoped_ptr(
    prepared_block_pool_.Construct());
  RETURN_NOT_OK(ReadCurrentDataBlock(*idx_iter, b.get()));
  b->dblk_->SeekToPositionInBlock(0);
  last_prepare_idx_ = 0;
  last_prepare_count_ = 0;

  prepared_blocks_.push_back(b.release());

  seeked_ = idx_iter;
  return Status::OK();
}



Status CFileIterator::SeekAtOrAfter(const EncodedKey &key,
                                    bool *exact_match) {
  DCHECK_EQ(reader_->is_nullable(), false);

  Unseek();
  if (PREDICT_FALSE(validx_iter_ == NULL)) {
    return Status::NotSupported("no value index present");
  }

  Status s = validx_iter_->SeekAtOrBefore(key.encoded_key());
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

  if (key.num_key_columns() > 1) {
    Slice slice = key.encoded_key();
    RETURN_NOT_OK(b->dblk_->SeekAtOrAfterValue(&slice, exact_match));
  } else {
    RETURN_NOT_OK(b->dblk_->SeekAtOrAfterValue(key.raw_keys()[0],
                                               exact_match));
  }

  last_prepare_idx_ = b->first_row_idx() + b->dblk_->GetCurrentIndex();
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
  return last_prepare_idx_;
}

string CFileIterator::PreparedBlock::ToString() const {
  return StringPrintf("dblk(%s, rows=%d-%d)",
                      dblk_ptr_.ToString().c_str(),
                      first_row_idx(),
                      last_row_idx());
}

// Decode the null header in the beginning of the data block
Status DecodeNullInfo(Slice *data_block, uint32_t *num_rows_in_block, Slice *null_bitmap) {
  if (!GetVarint32(data_block, num_rows_in_block)) {
    return Status::Corruption("bad null header, num elements in block");
  }

  uint32_t null_bitmap_size;
  if (!GetVarint32(data_block, &null_bitmap_size)) {
    return Status::Corruption("bad null header, bitmap size");
  }

  *null_bitmap = Slice(data_block->data(), null_bitmap_size);
  data_block->remove_prefix(null_bitmap_size);
  return Status::OK();
}

Status CFileIterator::ReadCurrentDataBlock(const IndexTreeIterator &idx_iter,
                                           PreparedBlock *prep_block) {
  prep_block->dblk_ptr_ = idx_iter.GetCurrentBlockPointer();
  RETURN_NOT_OK(reader_->ReadBlock(prep_block->dblk_ptr_, &prep_block->dblk_data_));

  io_stats_.data_blocks_read++;

  uint32_t num_rows_in_block = 0;
  Slice data_block = prep_block->dblk_data_.data();
  if (reader_->is_nullable()) {
    RETURN_NOT_OK(DecodeNullInfo(&data_block, &num_rows_in_block, &(prep_block->rle_bitmap)));
    prep_block->rle_decoder_ = RleDecoder<bool>(prep_block->rle_bitmap.data(),
                                                prep_block->rle_bitmap.size(), 1);
  }

  BlockDecoder *bd;
  RETURN_NOT_OK(reader_->type_encoding_info()->CreateBlockDecoder(&bd, data_block));
  prep_block->dblk_.reset(bd);
  RETURN_NOT_OK(prep_block->dblk_->ParseHeader());

  // For nullable blocks, we filled in the row count from the null information above,
  // since the data block decoder only knows about the non-null values.
  // For non-nullable ones, we use the information from the block decoder.
  if (!reader_->is_nullable()) {
    num_rows_in_block = bd->Count();
  }

  io_stats_.rows_read += num_rows_in_block;

  prep_block->idx_in_block_ = 0;
  prep_block->num_rows_in_block_ = num_rows_in_block;
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

  return !prepared_blocks_.empty() || seeked_->HasNext();
}

Status CFileIterator::PrepareBatch(size_t *n) {
  CHECK(!prepared_) << "Should call FinishBatch() first";
  CHECK(seeked_ != NULL) << "must be seeked";

  CHECK(!prepared_blocks_.empty());

  rowid_t start_idx = last_prepare_idx_;
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
    front->rewind_idx_ = start_idx - front->first_row_idx();
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

  DVLOG(1) << "Finishing batch " << last_prepare_idx_ << "-"
           << (last_prepare_idx_ + last_prepare_count_ - 1);

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

  last_prepare_idx_ += last_prepare_count_;
  last_prepare_count_ = 0;
  return Status::OK();
}


Status CFileIterator::Scan(ColumnBlock *dst) {
  CHECK(seeked_) << "not seeked";

  // Use a column data view to been able to advance it as we read into it.
  ColumnDataView remaining_dst(dst);

  uint32_t rem = last_prepare_count_;
  DCHECK_LE(rem, dst->nrows());

  BOOST_FOREACH(PreparedBlock *pb, prepared_blocks_) {
    if (pb->needs_rewind_) {
      // Seek back to the saved position.
      SeekToPositionInBlock(pb, pb->rewind_idx_);
      // TODO: we could add a mark/reset like interface in BlockDecoder interface
      // that might be more efficient (allowing the decoder to save internal state
      // instead of having to reconstruct it)
    }

    if (reader_->is_nullable()) {
      DCHECK(dst->is_nullable());

      size_t nrows = std::min(rem, pb->num_rows_in_block_ - pb->idx_in_block_);

      // Fill column bitmap
      size_t count = nrows;
      while (count > 0) {
        bool not_null = false;
        size_t nblock = pb->rle_decoder_.GetNextRun(&not_null, count);
        DCHECK_LE(nblock, count);
        if (PREDICT_FALSE(nblock == 0)) {
          return Status::Corruption(
            Substitute("Unexpected EOF on NULL bitmap read. Expected at least $0 more rows",
                       count));
        }

        size_t this_batch = nblock;
        if (not_null) {
          // TODO: Maybe copy all and shift later?
          RETURN_NOT_OK(pb->dblk_->CopyNextValues(&this_batch, &remaining_dst));
          DCHECK_EQ(nblock, this_batch);
          pb->needs_rewind_ = true;
        } else {
#ifndef NDEBUG
          kudu::OverwriteWithPattern(reinterpret_cast<char *>(remaining_dst.data()),
                                     remaining_dst.stride() * nblock,
                                     "NULLNULLNULLNULLNULL");
#endif
        }

        // Set the ColumnBlock bitmap
        remaining_dst.SetNullBits(this_batch, not_null);

        rem -= this_batch;
        count -= this_batch;
        pb->idx_in_block_ += this_batch;
        remaining_dst.Advance(this_batch);
      }
    } else {
      // Fetch as many as we can from the current datablock.
      size_t this_batch = rem;
      RETURN_NOT_OK(pb->dblk_->CopyNextValues(&this_batch, &remaining_dst));
      pb->needs_rewind_ = true;
      DCHECK_LE(this_batch, rem);

      // If the column is nullable, set all bits to true
      if (dst->is_nullable()) {
        remaining_dst.SetNullBits(this_batch, true);
      }

      rem -= this_batch;
      pb->idx_in_block_ += this_batch;
      remaining_dst.Advance(this_batch);
    }

    // If we didn't fetch as many as requested, then it should
    // be because the current data block ran out.
    if (rem > 0) {
      DCHECK_EQ(pb->dblk_->Count(), pb->dblk_->GetCurrentIndex()) <<
        "dblk stopped yielding values before it was empty.";
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
