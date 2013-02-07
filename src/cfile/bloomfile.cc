// Copyright (c) 2013, Cloudera, inc.

#include <tr1/memory>

#include "cfile/cfile.h"
#include "cfile/bloomfile.h"
#include "gutil/stringprintf.h"
#include "util/env.h"
#include "util/coding.h"
#include "util/hexdump.h"
#include "util/pb_util.h"

namespace kudu { namespace cfile {


////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////

BloomFileWriter::BloomFileWriter(const shared_ptr<WritableFile> &file,
                                 const BloomFilterSizing &sizing) :
  bloom_builder_(sizing)
{
  cfile::WriterOptions opts;
  opts.write_posidx = false;
  opts.write_validx = true;
  writer_.reset(new cfile::Writer(opts, STRING, PLAIN, file));
}

Status BloomFileWriter::Start() {
  return writer_->Start();
}

Status BloomFileWriter::Finish() {
  if (bloom_builder_.count() > 0) {
    RETURN_NOT_OK( FinishCurrentBloomBlock() );
  }
  return writer_->Finish();
}

Status BloomFileWriter::AppendKeys(
  const Slice *keys, size_t n_keys) {

  // If this is the call on a new bloom, copy the first key.
  if (bloom_builder_.count() == 0 && n_keys > 0) {
    first_key_.assign_copy(keys[0].data(), keys[0].size());
  }

  for (size_t i = 0; i < n_keys; i++) {

    bloom_builder_.AddKey(keys[i]);

    // Bloom has reached optimal occupancy: flush it to the file
    if (bloom_builder_.count() >= bloom_builder_.expected_count()) {
      RETURN_NOT_OK( FinishCurrentBloomBlock() );

      // Copy the next key as the first key of the next block.
      // Doing this here avoids having to do it in normal code path of the loop.
      if (i < n_keys - 1) {
        first_key_.assign_copy(keys[i + 1].data(), keys[i + 1].size());
      }
    }
  }

  return Status::OK();
}

Status BloomFileWriter::FinishCurrentBloomBlock() {
  VLOG(1) << "Appending a new bloom block, first_key=" << Slice(first_key_).ToDebugString();

  // Encode the header.
  BloomBlockHeaderPB hdr;
  hdr.set_num_hash_functions(bloom_builder_.n_hashes());
  faststring hdr_str;
  PutFixed32(&hdr_str, hdr.ByteSize());
  CHECK(pb_util::AppendToString(hdr, &hdr_str));

  // The data is the concatenation of the header and the bloom itself.
  vector<Slice> slices;
  slices.push_back(Slice(hdr_str));
  slices.push_back(bloom_builder_.slice());

  // Append to the file.
  Slice start_key(first_key_);
  RETURN_NOT_OK( writer_->AppendRawBlock(slices, 0, &start_key, "bloom block") );

  bloom_builder_.Clear();

  #ifndef NDEBUG
  first_key.assign_copy("POST_RESET");
  #endif

  return Status::OK();
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status BloomFileReader::Open(Env *env, const string &path,
                             BloomFileReader **reader) {
  RandomAccessFile *raf;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &raf));
  shared_ptr<RandomAccessFile> f(raf);

  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(path, &size));

  *reader = new BloomFileReader(f, size);
  return (*reader)->Init();
}

BloomFileReader::BloomFileReader(const shared_ptr<RandomAccessFile> &file,
                                 uint64_t file_size) :
  reader_(new CFileReader(ReaderOptions(), file, file_size))
{
}

Status BloomFileReader::Init() {
  RETURN_NOT_OK(reader_->Init());
  if (!reader_->has_validx()) {
    // TODO: include path!
    return Status::Corruption("bloom file missing value index");
  }

  BlockPointer validx_root = reader_->validx_root();
  index_iter_.reset(
    IndexTreeIterator::Create(reader_.get(), STRING, validx_root));

  return Status::OK();
}

Status BloomFileReader::ParseBlockHeader(const Slice &block,
                                         BloomBlockHeaderPB *hdr,
                                         Slice *bloom_data) const {
  Slice data(block);
  if (PREDICT_FALSE(data.size() < 4)) {
    return Status::Corruption("Invalid bloom block header: not enough bytes");
  }

  uint32_t header_len = DecodeFixed32(data.data());
  data.remove_prefix(sizeof(header_len));

  if (header_len > data.size()) {
    return Status::Corruption(
      StringPrintf("Header length %d doesn't fit in buffer of size %ld",
                   header_len, data.size()));
  }

  if (!hdr->ParseFromArray(data.data(), header_len)) {
    return Status::Corruption(
      string("Invalid bloom block header: ") +
      hdr->InitializationErrorString() +
      "\nHeader:" + HexDump(Slice(data.data(), header_len)));
  }

  data.remove_prefix(header_len);
  *bloom_data = data;
  return Status::OK();
}

Status BloomFileReader::CheckKeyPresent(const BloomKeyProbe &probe,
                                        bool *maybe_present) {

  Status s = index_iter_->SeekAtOrBefore(&probe.key());
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seek to before the first entry in the file.
    *maybe_present = false;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  // Successfully found the pointer to the bloom block. Read it.
  BlockPointer bblk_ptr = index_iter_->GetCurrentBlockPointer();
  BlockCacheHandle dblk_data;
  RETURN_NOT_OK(reader_->ReadBlock(bblk_ptr, &dblk_data));

  // Parse the header in the block.
  BloomBlockHeaderPB hdr;
  Slice bloom_data;
  RETURN_NOT_OK( ParseBlockHeader(dblk_data.data(), &hdr, &bloom_data) );

  // Actually check the bloom filter.
  BloomFilter bf(bloom_data, hdr.num_hash_functions());
  *maybe_present = bf.MayContainKey(probe);
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
