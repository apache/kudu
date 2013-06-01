// Copyright (c) 2012, Cloudera, inc

#include <boost/foreach.hpp>
#include <endian.h>
#include <string>
#include <glog/logging.h>

#include "cfile/cfile.h"
#include "cfile/cfile.pb.h"
#include "cfile/block_pointer.h"
#include "cfile/gvint_block.h"
#include "cfile/string_prefix_block.h"
#include "cfile/string_plain_block.h"
#include "cfile/index_block.h"
#include "cfile/index_btree.h"
#include "util/env.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/pb_util.h"
#include "util/hexdump.h"

using std::string;

DEFINE_int32(cfile_default_block_size, 256*1024, "The default block size to use in cfiles");
DEFINE_string(cfile_default_compression_codec, "none",
              "Default cfile block compression codec.");

namespace kudu { namespace cfile {

const char kMagicString[] = "kuducfil";

static const size_t kBlockSizeLimit = 16 * 1024 * 1024; // 16MB

static CompressionType GetDefaultCompressionCodec() {
  if (FLAGS_cfile_default_compression_codec.compare("snappy") == 0)
    return SNAPPY;
  if (FLAGS_cfile_default_compression_codec.compare("lz4") == 0)
    return LZ4;
  if (FLAGS_cfile_default_compression_codec.compare("zlib") == 0)
    return ZLIB;
  if (FLAGS_cfile_default_compression_codec.compare("none") == 0)
    return NO_COMPRESSION;

  LOG(WARNING) << "Unable to recognize the compression codec '"
               << FLAGS_cfile_default_compression_codec
               << "' using no compression as default.";
  return NO_COMPRESSION;
}

////////////////////////////////////////////////////////////
// Options
////////////////////////////////////////////////////////////
WriterOptions::WriterOptions() :
  block_size(FLAGS_cfile_default_block_size),
  index_block_size(32*1024),
  block_restart_interval(16),
  write_posidx(false),
  write_validx(false),
  compression(GetDefaultCompressionCodec())
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

  if (options_.compression != NO_COMPRESSION) {
    shared_ptr<CompressionCodec> compression_codec;
    RETURN_NOT_OK(GetCompressionCodec(options_.compression, &compression_codec));
    block_compressor_ .reset(new CompressedBlockBuilder(compression_codec, kBlockSizeLimit));
  }

  CFileHeaderPB header;
  header.set_major_version(kCFileMajorVersion);
  header.set_minor_version(kCFileMinorVersion);
  FlushMetadataToPB(header.mutable_metadata());

  uint32_t pb_size = header.ByteSize();

  faststring buf;
  // First the magic.
  buf.append(kMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, pb_size);
  if (!pb_util::AppendToString(header, &buf)) {
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
          *bb = new GVIntBlockBuilder(&options_);
          break;
        default:
          return Status::NotFound("bad int encoding");
      }
      break;
    case STRING:
      switch (encoding_type_) {
        case PREFIX:
          *bb = new StringPrefixBlockBuilder(&options_);
          break;
        case PLAIN:
          *bb = new StringPlainBlockBuilder(&options_);
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

  state_ = kWriterFinished;

  // Start preparing the footer.
  CFileFooterPB footer;
  footer.set_data_type(datatype_);
  footer.set_encoding(encoding_type_);
  footer.set_num_values(value_count_);
  footer.set_compression(options_.compression);

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

  // Flush metadata.
  FlushMetadataToPB(footer.mutable_metadata());

  faststring footer_str;
  if (!pb_util::SerializeToString(footer, &footer_str)) {
    return Status::Corruption("unable to serialize footer");
  }

  footer_str.append(kMagicString);
  PutFixed32(&footer_str, footer.GetCachedSize());

  RETURN_NOT_OK(file_->Append(footer_str));
  RETURN_NOT_OK(file_->Flush());

  return file_->Close();
}

void Writer::AddMetadataPair(const Slice &key, const Slice &value) {
  CHECK_NE(state_, kWriterFinished);

  unflushed_metadata_.push_back(make_pair(key.ToString(), value.ToString()));
}

void Writer::FlushMetadataToPB(RepeatedPtrField<FileMetadataPairPB> *field) {
  typedef pair<string, string> ss_pair;
  BOOST_FOREACH(const ss_pair &entry, unflushed_metadata_) {
    FileMetadataPairPB *pb = field->Add();
    pb->set_key(entry.first);
    pb->set_value(entry.second);
  }
  unflushed_metadata_.clear();
}

Status Writer::AppendEntries(const void *entries, size_t count) {
  int rem = count;

  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(entries);

  while (rem > 0) {
    int n = data_block_->Add(ptr, rem, typeinfo_.size());
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

  rowid_t first_elem_ord = value_count_ - num_elems_in_block;

  VLOG(1) << "Appending data block for values " <<
    first_elem_ord << "-" << (first_elem_ord + num_elems_in_block);

  // The current data block is full, need to push it
  // into the file, and add to index
  Slice data = data_block_->Finish(first_elem_ord);
  VLOG(2) << "estimated size=" << data_block_->EstimateEncodedSize()
          << " actual=" << data.size();

  uint8_t key_tmp_space[typeinfo_.size()];

  if (validx_builder_ != NULL) {
    // If we're building an index, we need to copy the first
    // key from the block locally, so we can write it into that index.
    RETURN_NOT_OK(data_block_->GetFirstKey(key_tmp_space));
    VLOG(1) << "Appending validx entry\n" <<
      kudu::HexDump(Slice(key_tmp_space, typeinfo_.size()));
  }

  vector<Slice> v;
  v.push_back(data);
  Status s = AppendRawBlock(v, first_elem_ord,
                            reinterpret_cast<const void *>(key_tmp_space),
                            "data block");

  data_block_->Reset();

  return s;
}

Status Writer::AppendRawBlock(const vector<Slice> &data_slices,
                              size_t ordinal_pos,
                              const void *validx_key,
                              const char *name_for_log) {
  CHECK_EQ(state_, kWriterWriting);

  BlockPointer ptr;
  Status s = AddBlock(data_slices, &ptr, "data");
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append block to file: " << s.ToString();
    return s;
  }


  // Now add to the index blocks
  if (posidx_builder_ != NULL) {
    RETURN_NOT_OK(posidx_builder_->Append(&ordinal_pos, ptr));
  }

  if (validx_builder_ != NULL) {
    CHECK(validx_key != NULL) <<
      "must pass a  key for raw block if validx is configured";
    VLOG(1) << "Appending validx entry\n" <<
      kudu::HexDump(Slice(reinterpret_cast<const uint8_t *>(validx_key),
                          typeinfo_.size()));
    s = validx_builder_->Append(validx_key, ptr);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to append to value index: " << s.ToString();
      return s;
    }
  }

  return s;
}

Status Writer::AddBlock(const vector<Slice> &data_slices,
                        BlockPointer *block_ptr,
                        const char *name_for_log) {
  uint64_t start_offset = off_;

  if (block_compressor_ != NULL) {
    // Write compressed block
    Slice cdata;
    Status s = block_compressor_->Compress(data_slices, &cdata);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to compress slice of size "
                   << cdata.size() << " at offset " << off_
                   << ": " << s.ToString();
      return(s);
    }

    RETURN_NOT_OK(WriteRawData(cdata));
  } else {
    // Write uncompressed block
    BOOST_FOREACH(const Slice &data, data_slices) {
      RETURN_NOT_OK(WriteRawData(data));
    }
  }

  uint64_t total_size = off_ - start_offset;

  *block_ptr = BlockPointer(start_offset, total_size);
  VLOG(1) << "Appended " << name_for_log
          << " with " << total_size << " bytes at " << start_offset;
  return Status::OK();
}

Status Writer::WriteRawData(const Slice& data) {
  Status s = file_->Append(data);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append slice of size "
                << data.size() << " at offset " << off_
                << ": " << s.ToString();
  }
  off_ += data.size();
  return s;
}

Writer::~Writer() {
}


}
}
