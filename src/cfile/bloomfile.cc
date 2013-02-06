// Copyright (c) 2013, Cloudera, inc.

#include <tr1/memory>

#include "cfile/cfile.h"
#include "cfile/bloomfile.h"
#include "gutil/stringprintf.h"
#include "util/env.h"
#include "util/coding.h"
#include "util/hexdump.h"

namespace kudu { namespace cfile {


////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////

BloomFileWriter::BloomFileWriter(const shared_ptr<WritableFile> &file)
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
  return writer_->Finish();
}

Status BloomFileWriter::AppendBloom(
  const BloomFilterBuilder &bloom, const Slice &start_key) {

  BloomBlockHeaderPB hdr;
  hdr.set_num_hash_functions(bloom.n_hashes());
  string hdr_str;
  PutFixed32(&hdr_str, hdr.ByteSize());
  CHECK(hdr.AppendToString(&hdr_str));

  vector<Slice> slices;
  slices.push_back(Slice(hdr_str));
  slices.push_back(bloom.slice());

  return writer_->AppendRawBlock(slices, 0, &start_key, "bloom block");
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

  BlockPointer validx_root = reader_->validx_root();
  scoped_ptr<cfile::IndexTreeIterator> iter(
    IndexTreeIterator::Create(reader_.get(), STRING, validx_root));

  Status s = iter->SeekAtOrBefore(&probe.key());
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seek to before the first entry in the file.
    *maybe_present = false;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  // Successfully found the pointer to the bloom block. Read it.
  BlockPointer bblk_ptr = iter->GetCurrentBlockPointer();
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
