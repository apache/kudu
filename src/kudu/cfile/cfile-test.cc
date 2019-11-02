// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/cache.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/int128.h"
#include "kudu/util/int128_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/nvm_cache.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(cfile_write_checksums);
DECLARE_bool(cfile_verify_checksums);
DECLARE_string(block_cache_type);
DECLARE_string(nvm_cache_path);
DECLARE_bool(nvm_cache_simulate_allocation_failure);

METRIC_DECLARE_counter(block_cache_hits_caching);

METRIC_DECLARE_entity(server);


using kudu::fs::BlockManager;
using kudu::fs::CountingReadableBlock;
using kudu::fs::CreateCorruptBlock;
using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cfile {

class TestCFile : public CFileTestBase {
 protected:
  template <class DataGeneratorType>
  void TestReadWriteFixedSizeTypes(EncodingType encoding) {
    BlockId block_id;
    DataGeneratorType generator;

    WriteTestFile(&generator, encoding, NO_COMPRESSION, 10000, SMALL_BLOCKSIZE, &block_id);

    unique_ptr<ReadableBlock> block;
    ASSERT_OK(fs_manager_->OpenBlock(block_id, &block));
    unique_ptr<CFileReader> reader;
    ASSERT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

    BlockPointer ptr;
    unique_ptr<CFileIterator> iter;
    ASSERT_OK(reader->NewIterator(&iter, CFileReader::CACHE_BLOCK, nullptr));

    ASSERT_OK(iter->SeekToOrdinal(5000));
    ASSERT_EQ(5000u, iter->GetCurrentOrdinal());

    // Seek to last key exactly, should succeed.
    ASSERT_OK(iter->SeekToOrdinal(9999));
    ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

    // Seek to after last key. Should result in not found.
    ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());

    // Seek to start of file
    ASSERT_OK(iter->SeekToOrdinal(0));
    ASSERT_EQ(0u, iter->GetCurrentOrdinal());

    // Fetch all data.
    ScopedColumnBlock<DataGeneratorType::kDataType> out(10000);
    size_t n = 10000;
    SelectionVector sel(10000);
    ColumnMaterializationContext out_ctx = CreateNonDecoderEvalContext(&out, &sel);
    ASSERT_OK(iter->CopyNextValues(&n, &out_ctx));
    ASSERT_EQ(10000, n);

    DataGeneratorType data_generator_pre;

    for (int i = 0; i < 10000; i++) {
      if (out[i] != data_generator_pre.BuildTestValue(0,i)) {
        FAIL() << "mismatch at index " << i
               << " expected: " << data_generator_pre.BuildTestValue(0,i)
               << " got: " << out[i];
      }
      out[i] = 0;
    }

    // Fetch all data using small batches of only a few rows.
    // This should catch edge conditions like a batch lining up exactly
    // with the end of a block.
    unsigned int seed = time(nullptr);
    LOG(INFO) << "Using random seed: " << seed;
    srand(seed);
    ASSERT_OK(iter->SeekToOrdinal(0));
    size_t fetched = 0;
    while (fetched < 10000) {
      ColumnBlock advancing_block(out.type_info(), nullptr,
                                  out.data() + (fetched * out.stride()),
                                  out.nrows() - fetched, out.arena());
      ColumnMaterializationContext adv_ctx = CreateNonDecoderEvalContext(&advancing_block, &sel);
      ASSERT_TRUE(iter->HasNext());
      size_t batch_size = random() % 5 + 1;
      size_t n = batch_size;
      ASSERT_OK(iter->CopyNextValues(&n, &adv_ctx));
      ASSERT_LE(n, batch_size);
      fetched += n;
    }
    ASSERT_FALSE(iter->HasNext());

    DataGeneratorType data_generator_post;

    // Re-verify
    for (int i = 0; i < 10000; i++) {
      if (out[i] != data_generator_post.BuildTestValue(0,i)) {
        FAIL() << "mismatch at index " << i
               << " expected: " << data_generator_post.BuildTestValue(0,i)
               << " got: " << out[i];
      }
      out[i] = 0;
    }

    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(10000, n);
  }

  template <class DataGeneratorType>
  void TimeSeekAndReadFileWithNulls(DataGeneratorType* generator,
                                    const BlockId& block_id, size_t num_entries) {
    unique_ptr<ReadableBlock> block;
    ASSERT_OK(fs_manager_->OpenBlock(block_id, &block));
    unique_ptr<CFileReader> reader;
    ASSERT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));
    ASSERT_EQ(DataGeneratorType::kDataType, reader->type_info()->type());

    unique_ptr<CFileIterator> iter;
    ASSERT_OK(reader->NewIterator(&iter, CFileReader::CACHE_BLOCK, nullptr));

    Arena arena(8192);
    ScopedColumnBlock<DataGeneratorType::kDataType> cb(10);

    SelectionVector sel(10);
    ColumnMaterializationContext ctx = CreateNonDecoderEvalContext(&cb, &sel);
    const int kNumLoops = AllowSlowTests() ? num_entries : 10;
    for (int loop = 0; loop < kNumLoops; loop++) {
      // Seek to a random point in the file,
      // or just try each entry as starting point if you're running SlowTests
      int target = AllowSlowTests() ? loop : (random() % (num_entries - 1));
      SCOPED_TRACE(target);
      ASSERT_OK(iter->SeekToOrdinal(target));
      ASSERT_TRUE(iter->HasNext());

      // Read and verify several ColumnBlocks from this point in the file.
      int read_offset = target;
      for (int block = 0; block < 3 && iter->HasNext(); block++) {
        size_t n = cb.nrows();
        ASSERT_OK_FAST(iter->CopyNextValues(&n, &ctx));
        ASSERT_EQ(n, std::min(num_entries - read_offset, cb.nrows()));

        // Verify that the block data is correct.
        generator->Build(read_offset, n);
        for (size_t j = 0; j < n; ++j) {
          bool expected_null = generator->TestValueShouldBeNull(read_offset + j);
          ASSERT_EQ(expected_null, cb.is_null(j));
          if (!expected_null) {
            ASSERT_EQ((*generator)[j], cb[j]);
          }
        }
        cb.arena()->Reset();
        read_offset += n;
      }
    }
  }

  template <class DataGeneratorType>
  void TestNullTypes(DataGeneratorType* generator, EncodingType encoding,
                     CompressionType compression) {
    BlockId block_id;
    WriteTestFile(generator, encoding, compression, 10000, SMALL_BLOCKSIZE, &block_id);

    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(n, 10000);

    generator->Reset();
    TimeSeekAndReadFileWithNulls(generator, block_id, n);
  }

  void TestReadWriteRawBlocks(CompressionType compression, int num_entries) {
    // Test Write
    unique_ptr<WritableBlock> sink;
    ASSERT_OK(fs_manager_->CreateNewBlock({}, &sink));
    BlockId id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;
    opts.write_validx = false;
    opts.storage_attributes.cfile_block_size = FLAGS_cfile_test_block_size;
    opts.storage_attributes.compression = compression;
    opts.storage_attributes.encoding = PLAIN_ENCODING;
    CFileWriter w(opts, GetTypeInfo(STRING), false, std::move(sink));
    ASSERT_OK(w.Start());
    for (uint32_t i = 0; i < num_entries; i++) {
      vector<Slice> slices;
      slices.emplace_back("Head");
      slices.emplace_back("Body");
      slices.emplace_back("Tail");
      slices.emplace_back(reinterpret_cast<uint8_t *>(&i), 4);
      ASSERT_OK(w.AppendRawBlock(slices, i, nullptr, Slice(), "raw-data"));
    }
    ASSERT_OK(w.Finish());

    // Test Read
    unique_ptr<ReadableBlock> source;
    ASSERT_OK(fs_manager_->OpenBlock(id, &source));
    unique_ptr<CFileReader> reader;
    ASSERT_OK(CFileReader::Open(std::move(source), ReaderOptions(), &reader));

    ASSERT_EQ(reader->footer().compression(), compression);
    if (FLAGS_cfile_write_checksums) {
      ASSERT_TRUE(reader->footer().incompatible_features() & IncompatibleFeatures::CHECKSUM);
    } else {
      ASSERT_FALSE(reader->footer().incompatible_features() & IncompatibleFeatures::CHECKSUM);
    }

    gscoped_ptr<IndexTreeIterator> iter;
    iter.reset(IndexTreeIterator::Create(nullptr, reader.get(), reader->posidx_root()));
    ASSERT_OK(iter->SeekToFirst());

    uint8_t data[16];
    Slice expected_data(data, 16);
    memcpy(data, "HeadBodyTail", 12);

    uint32_t count = 0;
    do {
      BlockHandle dblk_data;
      BlockPointer blk_ptr = iter->GetCurrentBlockPointer();
      ASSERT_OK(reader->ReadBlock(nullptr, blk_ptr, CFileReader::CACHE_BLOCK, &dblk_data));

      memcpy(data + 12, &count, 4);
      ASSERT_EQ(expected_data, dblk_data.data());

      count++;
    } while (iter->Next().ok());
    ASSERT_EQ(num_entries, count);
  }

  void TestReadWriteStrings(EncodingType encoding) {
    TestReadWriteStrings(encoding, [](size_t val) {
        return StringPrintf("hello %04zd", val);
      });
  }

  void TestReadWriteStrings(EncodingType encoding,
                            std::function<string(size_t)> formatter);

#ifdef NDEBUG
  void TestWrite100MFileStrings(EncodingType encoding) {
    BlockId block_id;
    LOG_TIMING(INFO, "writing 100M strings") {
      LOG(INFO) << "Starting writefile";
      StringDataGenerator<false> generator([](size_t idx) {
          char buf[kFastToBufferSize];
          FastHex64ToBuffer(idx, buf);
          return string(buf);
        });
      WriteTestFile(&generator, encoding, NO_COMPRESSION, 100000000, NO_FLAGS, &block_id);
      LOG(INFO) << "Done writing";
    }

    LOG_TIMING(INFO, "reading 100M strings") {
      LOG(INFO) << "Starting readfile";
      size_t n;
      TimeReadFile(fs_manager_.get(), block_id, &n);
      ASSERT_EQ(100000000, n);
      LOG(INFO) << "End readfile";
    }
  }
#endif

  void TestWriteDictEncodingLowCardinalityStrings(int64_t num_rows) {
    BlockId block_id;
    LOG_TIMING(INFO, Substitute("writing $0 strings with dupes", num_rows)) {
      LOG(INFO) << "Starting writefile";
      // The second parameter specify how many distinct strings are there
      DuplicateStringDataGenerator<false> generator("hello %zu", 256);
      WriteTestFile(&generator, DICT_ENCODING, NO_COMPRESSION, num_rows, NO_FLAGS, &block_id);
      LOG(INFO) << "Done writing";
    }

    LOG_TIMING(INFO, Substitute("reading $0 strings with dupes", num_rows)) {
      LOG(INFO) << "Starting readfile";
      size_t n;
      TimeReadFile(fs_manager_.get(), block_id, &n);
      ASSERT_EQ(num_rows, n);
      LOG(INFO) << "End readfile";
    }
  }

  Status CorruptAndReadBlock(const BlockId block_id, const uint64_t corrupt_offset,
                             uint8_t flip_bit) {
    BlockId new_id;
    RETURN_NOT_OK(CreateCorruptBlock(
        fs_manager_.get(), block_id, corrupt_offset, flip_bit, &new_id));

    // Open and read the corrupt block with the CFileReader
    unique_ptr<ReadableBlock> corrupt_source;
    RETURN_NOT_OK(fs_manager_->OpenBlock(new_id, &corrupt_source));
    unique_ptr<CFileReader> reader;
    ReaderOptions opts;
    const fs::IOContext io_context({ "corrupted-dummy-tablet" });
    opts.io_context = &io_context;
    RETURN_NOT_OK(CFileReader::Open(std::move(corrupt_source), std::move(opts), &reader));
    gscoped_ptr<IndexTreeIterator> iter;
    iter.reset(IndexTreeIterator::Create(&io_context, reader.get(), reader->posidx_root()));
    RETURN_NOT_OK(iter->SeekToFirst());

    do {
      BlockHandle dblk_data;
      BlockPointer blk_ptr = iter->GetCurrentBlockPointer();
      RETURN_NOT_OK(reader->ReadBlock(&io_context, blk_ptr,
          CFileReader::DONT_CACHE_BLOCK, &dblk_data));
    } while (iter->Next().ok());

    return Status::OK();
  }
};

// Subclass of TestCFile which is parameterized on the block cache type.
// Tests that use TEST_P(TestCFileBothCacheMemoryTypes, ...) will run twice --
// once for each cache memory type (DRAM, NVM).
class TestCFileBothCacheMemoryTypes :
    public TestCFile,
    public ::testing::WithParamInterface<Cache::MemoryType> {
 public:
  void SetUp() OVERRIDE {
    // The NVM cache can run using any directory as its path -- it doesn't have
    // a lot of practical use outside of an actual NVM device, but for testing
    // purposes, we'll point it at our test dir, unless otherwise specified.
    if (google::GetCommandLineFlagInfoOrDie("nvm_cache_path").is_default) {
      FLAGS_nvm_cache_path = GetTestPath("nvm-cache");
      ASSERT_OK(Env::Default()->CreateDir(FLAGS_nvm_cache_path));
    }
    switch (GetParam()) {
      case Cache::MemoryType::DRAM:
        FLAGS_block_cache_type = "DRAM";
        break;
      case Cache::MemoryType::NVM:
        FLAGS_block_cache_type = "NVM";
        break;
      default:
        LOG(FATAL) << "Unknown block cache type: '"
                   << static_cast<int16_t>(GetParam());
    }
    CFileTestBase::SetUp();
  }

  void TearDown() OVERRIDE {
    Singleton<BlockCache>::UnsafeReset();
  }
};

INSTANTIATE_TEST_CASE_P(CacheMemoryTypes, TestCFileBothCacheMemoryTypes,
                        ::testing::Values(Cache::MemoryType::DRAM,
                                          Cache::MemoryType::NVM));

template<DataType type>
void CopyOne(CFileIterator *it,
             typename TypeTraits<type>::cpp_type *ret,
             Arena *arena) {
  ColumnBlock cb(GetTypeInfo(type), nullptr, ret, 1, arena);
  SelectionVector sel(1);
  ColumnMaterializationContext ctx(0, nullptr, &cb, &sel);
  ctx.SetDecoderEvalNotSupported();
  size_t n = 1;
  ASSERT_OK(it->CopyNextValues(&n, &ctx));
  ASSERT_EQ(1, n);
}

#ifdef NDEBUG
// Only run the 100M entry tests in non-debug mode.
// They take way too long with debugging enabled.

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MFileInts) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100m ints") {
    LOG(INFO) << "Starting writefile";
    Int32DataGenerator<false> generator;
    WriteTestFile(&generator, BIT_SHUFFLE, NO_COMPRESSION, 100000000, NO_FLAGS, &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MFileNullableInts) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  BlockId block_id;
  LOG_TIMING(INFO, "writing 100m nullable ints") {
    LOG(INFO) << "Starting writefile";
    Int32DataGenerator<true> generator;
    WriteTestFile(&generator, PLAIN_ENCODING, NO_COMPRESSION, 100000000, NO_FLAGS, &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 100M nullable ints") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(100000000, n);
    LOG(INFO) << "End readfile";
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MFileStringsPrefixEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestWrite100MFileStrings(PREFIX_ENCODING);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MUniqueStringsDictEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestWrite100MFileStrings(DICT_ENCODING);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MLowCardinalityStringsDictEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestWriteDictEncodingLowCardinalityStrings(100 * 1e6);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestWrite100MFileStringsPlainEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestWrite100MFileStrings(PLAIN_ENCODING);
}

#endif

// Write and Read 1 million unique strings with dictionary encoding
TEST_P(TestCFileBothCacheMemoryTypes, TestWrite1MUniqueFileStringsDictEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  BlockId block_id;
  LOG_TIMING(INFO, "writing 1M unique strings") {
    LOG(INFO) << "Starting writefile";
    StringDataGenerator<false> generator("hello %zu");
    WriteTestFile(&generator, DICT_ENCODING, NO_COMPRESSION, 1000000, NO_FLAGS, &block_id);
    LOG(INFO) << "Done writing";
  }

  LOG_TIMING(INFO, "reading 1M strings") {
    LOG(INFO) << "Starting readfile";
    size_t n;
    TimeReadFile(fs_manager_.get(), block_id, &n);
    ASSERT_EQ(1000000, n);
    LOG(INFO) << "End readfile";
  }
}

// Write and Read 1 million strings, which contains duplicates with dictionary encoding
TEST_P(TestCFileBothCacheMemoryTypes, TestWrite1MLowCardinalityStringsDictEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestWriteDictEncodingLowCardinalityStrings(1000000);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteUInt32) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  for (auto enc : { PLAIN_ENCODING, RLE }) {
    TestReadWriteFixedSizeTypes<UInt32DataGenerator<false>>(enc);
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteInt32) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  for (auto enc : { PLAIN_ENCODING, RLE }) {
    TestReadWriteFixedSizeTypes<Int32DataGenerator<false>>(enc);
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteUInt64) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  for (auto enc : { PLAIN_ENCODING, RLE, BIT_SHUFFLE }) {
    TestReadWriteFixedSizeTypes<UInt64DataGenerator<false>>(enc);
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteInt64) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  for (auto enc : { PLAIN_ENCODING, RLE, BIT_SHUFFLE }) {
    TestReadWriteFixedSizeTypes<Int64DataGenerator<false>>(enc);
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteInt128) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteFixedSizeTypes<Int128DataGenerator<false>>(PLAIN_ENCODING);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestFixedSizeReadWritePlainEncodingFloat) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteFixedSizeTypes<FPDataGenerator<FLOAT, false>>(PLAIN_ENCODING);
}
TEST_P(TestCFileBothCacheMemoryTypes, TestFixedSizeReadWritePlainEncodingDouble) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteFixedSizeTypes<FPDataGenerator<DOUBLE, false>>(PLAIN_ENCODING);
}

// Test for BitShuffle builder for UINT8, INT8, UINT16, INT16, UINT32, INT32,
// UINT64, INT64, INT128, FLOAT, DOUBLE
template <typename T>
class BitShuffleTest : public TestCFile {
  public:
    void TestBitShuffle() {
      TestReadWriteFixedSizeTypes<T>(BIT_SHUFFLE);
    }
};
typedef ::testing::Types<UInt8DataGenerator<false>,
                         Int8DataGenerator<false>,
                         UInt16DataGenerator<false>,
                         Int16DataGenerator<false>,
                         UInt32DataGenerator<false>,
                         Int32DataGenerator<false>,
                         UInt64DataGenerator<false>,
                         Int64DataGenerator<false>,
                         Int128DataGenerator<false>,
                         FPDataGenerator<FLOAT, false>,
                         FPDataGenerator<DOUBLE, false> > MyTypes;
TYPED_TEST_CASE(BitShuffleTest, MyTypes);
TYPED_TEST(BitShuffleTest, TestFixedSizeReadWriteBitShuffle) {
  this->TestBitShuffle();
}

void EncodeStringKey(const Schema &schema, const Slice& key,
                     gscoped_ptr<EncodedKey> *encoded_key) {
  EncodedKeyBuilder kb(&schema);
  kb.AddColumnKey(&key);
  encoded_key->reset(kb.BuildEncodedKey());
}

void TestCFile::TestReadWriteStrings(EncodingType encoding,
                                     std::function<string(size_t)> formatter) {
  Schema schema({ ColumnSchema("key", STRING) }, 1);

  const int nrows = 10000;
  BlockId block_id;
  StringDataGenerator<false> generator(formatter);
  WriteTestFile(&generator, encoding, NO_COMPRESSION, nrows,
                SMALL_BLOCKSIZE | WRITE_VALIDX, &block_id);

  unique_ptr<ReadableBlock> block;
  ASSERT_OK(fs_manager_->OpenBlock(block_id, &block));
  unique_ptr<CFileReader> reader;
  ASSERT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  rowid_t reader_nrows;
  ASSERT_OK(reader->CountRows(&reader_nrows));
  ASSERT_EQ(nrows, reader_nrows);

  BlockPointer ptr;

  unique_ptr<CFileIterator> iter;
  ASSERT_OK(reader->NewIterator(&iter, CFileReader::CACHE_BLOCK, nullptr));

  Arena arena(1024);

  ASSERT_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());
  Slice s;

  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(formatter(5000), s.ToString());

  // Seek to last key exactly, should succeed
  ASSERT_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());


  ////////
  // Now try some seeks by the value instead of position
  /////////

  gscoped_ptr<EncodedKey> encoded_key;
  bool exact;

  // Seek in between each key.
  // (seek to "hello 0000.5" through "hello 9999.5")
  string buf;
  for (int i = 1; i < 10000; i++) {
    arena.Reset();
    buf = formatter(i - 1);
    buf.append(".5");
    s = Slice(buf);
    EncodeStringKey(schema, s, &encoded_key);
    ASSERT_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
    ASSERT_FALSE(exact);
    ASSERT_EQ(i, iter->GetCurrentOrdinal());
    CopyOne<STRING>(iter.get(), &s, &arena);
    ASSERT_EQ(formatter(i), s.ToString());
  }

  // Seek exactly to each key
  // (seek to "hello 0000" through "hello 9999")
  for (int i = 0; i < 9999; i++) {
    arena.Reset();
    buf = formatter(i);
    s = Slice(buf);
    EncodeStringKey(schema, s, &encoded_key);
    ASSERT_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
    ASSERT_TRUE(exact);
    ASSERT_EQ(i, iter->GetCurrentOrdinal());
    Slice read_back;
    CopyOne<STRING>(iter.get(), &read_back, &arena);
    ASSERT_EQ(read_back.ToString(), s.ToString());
  }

  // after last entry
  // (seek to "hello 9999.x")
  buf = formatter(9999) + ".x";
  s = Slice(buf);
  EncodeStringKey(schema, s, &encoded_key);
  EXPECT_TRUE(iter->SeekAtOrAfter(*encoded_key, &exact).IsNotFound());

  // before first entry
  // (seek to "hello 000", which falls before "hello 0000")
  buf = formatter(0);
  buf.resize(buf.size() - 1);
  s = Slice(buf);
  EncodeStringKey(schema, s, &encoded_key);
  ASSERT_OK(iter->SeekAtOrAfter(*encoded_key, &exact));
  EXPECT_FALSE(exact);
  EXPECT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  EXPECT_EQ(formatter(0), s.ToString());

  // Seek to start of file by ordinal
  ASSERT_OK(iter->SeekToFirst());
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
  CopyOne<STRING>(iter.get(), &s, &arena);
  ASSERT_EQ(formatter(0), s.ToString());

  // Reseek to start and fetch all data.
  // We fetch in 10 smaller chunks to avoid using too much RAM for the
  // case where the values are large.
  SelectionVector sel(10000);
  ASSERT_OK(iter->SeekToFirst());
  for (int i = 0; i < 10; i++) {
    ScopedColumnBlock<STRING> cb(10000);
    ColumnMaterializationContext cb_ctx = CreateNonDecoderEvalContext(&cb, &sel);
    size_t n = 1000;
    ASSERT_OK(iter->CopyNextValues(&n, &cb_ctx));
    ASSERT_EQ(1000, n);
  }
}


TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteStringsPrefixEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteStrings(PREFIX_ENCODING);
}

// Read/Write test for dictionary encoded blocks
TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteStringsDictEncoding) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteStrings(DICT_ENCODING);
}

// Regression test for properly handling cells that are larger
// than the index block and/or data block size.
//
// This test is disabled in TSAN because it's single-threaded anyway
// and runs extremely slowly with TSAN enabled.
#ifndef THREAD_SANITIZER
TEST_P(TestCFileBothCacheMemoryTypes, TestReadWriteLargeStrings) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  // Pad the values out to a length of ~65KB.
  // We use this method instead of just a longer sprintf format since
  // this is much more CPU-efficient (speeds up the test).
  auto formatter = [](size_t val) {
    string ret(66000, '0');
    StringAppendF(&ret, "%010zd", val);
    return ret;
  };
  TestReadWriteStrings(PLAIN_ENCODING, formatter);
  if (AllowSlowTests()) {
    TestReadWriteStrings(DICT_ENCODING, formatter);
    TestReadWriteStrings(PREFIX_ENCODING, formatter);
  }
}
#endif

// Test that metadata entries stored in the cfile are persisted.
TEST_P(TestCFileBothCacheMemoryTypes, TestMetadata) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  BlockId block_id;

  // Write the file.
  {
    unique_ptr<WritableBlock> sink;
    ASSERT_OK(fs_manager_->CreateNewBlock({}, &sink));
    block_id = sink->id();
    WriterOptions opts;
    CFileWriter w(opts, GetTypeInfo(INT32), false, std::move(sink));

    w.AddMetadataPair("key_in_header", "header value");
    ASSERT_OK(w.Start());

    uint32_t val = 1;
    ASSERT_OK(w.AppendEntries(&val, 1));

    w.AddMetadataPair("key_in_footer", "footer value");
    ASSERT_OK(w.Finish());
  }

  // Read the file and ensure metadata is present.
  {
    unique_ptr<ReadableBlock> source;
    ASSERT_OK(fs_manager_->OpenBlock(block_id, &source));
    unique_ptr<CFileReader> reader;
    ASSERT_OK(CFileReader::Open(std::move(source), ReaderOptions(), &reader));
    string val;
    ASSERT_TRUE(reader->GetMetadataEntry("key_in_header", &val));
    ASSERT_EQ(val, "header value");
    ASSERT_TRUE(reader->GetMetadataEntry("key_in_footer", &val));
    ASSERT_EQ(val, "footer value");
    ASSERT_FALSE(reader->GetMetadataEntry("not a key", &val));

    // Test that, even though we didn't specify an encoding or compression, the
    // resulting file has them explicitly set.
    ASSERT_EQ(BIT_SHUFFLE, reader->type_encoding_info()->encoding_type());
    ASSERT_EQ(NO_COMPRESSION, reader->footer().compression());
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestDefaultColumnIter) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  const int kNumItems = 64;
  uint8_t null_bitmap[BitmapSize(kNumItems)];
  uint32_t data[kNumItems];

  // Test Int Default Value
  uint32_t int_value = 15;
  DefaultColumnValueIterator iter(GetTypeInfo(UINT32), &int_value);
  ColumnBlock int_col(GetTypeInfo(UINT32), nullptr, data, kNumItems, nullptr);
  SelectionVector sel(kNumItems);
  ColumnMaterializationContext int_ctx = CreateNonDecoderEvalContext(&int_col, &sel);
  ASSERT_OK(iter.Scan(&int_ctx));
  for (size_t i = 0; i < int_col.nrows(); ++i) {
    ASSERT_EQ(int_value, *reinterpret_cast<const uint32_t *>(int_col.cell_ptr(i)));
  }

  // Test Int Nullable Default Value
  int_value = 321;
  DefaultColumnValueIterator nullable_iter(GetTypeInfo(UINT32), &int_value);
  ColumnBlock nullable_col(GetTypeInfo(UINT32), null_bitmap, data, kNumItems, nullptr);
  ColumnMaterializationContext nullable_ctx = CreateNonDecoderEvalContext(&nullable_col, &sel);
  ASSERT_OK(nullable_iter.Scan(&nullable_ctx));
  for (size_t i = 0; i < nullable_col.nrows(); ++i) {
    ASSERT_FALSE(nullable_col.is_null(i));
    ASSERT_EQ(int_value, *reinterpret_cast<const uint32_t *>(nullable_col.cell_ptr(i)));
  }

  // Test NULL Default Value
  DefaultColumnValueIterator null_iter(GetTypeInfo(UINT32),  nullptr);
  ColumnBlock null_col(GetTypeInfo(UINT32), null_bitmap, data, kNumItems, nullptr);
  ColumnMaterializationContext null_ctx = CreateNonDecoderEvalContext(&null_col, &sel);
  ASSERT_OK(null_iter.Scan(&null_ctx));
  for (size_t i = 0; i < null_col.nrows(); ++i) {
    ASSERT_TRUE(null_col.is_null(i));
  }

  // Test String Default Value
  Slice str_data[kNumItems];
  Slice str_value("Hello");
  Arena arena(32*1024);
  DefaultColumnValueIterator str_iter(GetTypeInfo(STRING), &str_value);
  ColumnBlock str_col(GetTypeInfo(STRING), nullptr, str_data, kNumItems, &arena);
  ColumnMaterializationContext str_ctx = CreateNonDecoderEvalContext(&str_col, &sel);
  ASSERT_OK(str_iter.Scan(&str_ctx));
  for (size_t i = 0; i < str_col.nrows(); ++i) {
    ASSERT_EQ(str_value, *reinterpret_cast<const Slice *>(str_col.cell_ptr(i)));
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestAppendRaw) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  TestReadWriteRawBlocks(NO_COMPRESSION, 1000);
  TestReadWriteRawBlocks(SNAPPY, 1000);
  TestReadWriteRawBlocks(LZ4, 1000);
  TestReadWriteRawBlocks(ZLIB, 1000);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestChecksumFlags) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  for (bool write_checksums : {false, true}) {
    for (bool verify_checksums : {false, true}) {
      FLAGS_cfile_write_checksums = write_checksums;
      FLAGS_cfile_verify_checksums = verify_checksums;
      TestReadWriteRawBlocks(NO_COMPRESSION, 1000);
      TestReadWriteRawBlocks(SNAPPY, 1000);
    }
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestDataCorruption) {
  RETURN_IF_NO_NVM_CACHE(GetParam());
  FLAGS_cfile_write_checksums = true;
  FLAGS_cfile_verify_checksums = true;

  // Write some data
  unique_ptr<WritableBlock> sink;
  ASSERT_OK(fs_manager_->CreateNewBlock({}, &sink));
  BlockId id = sink->id();
  WriterOptions opts;
  opts.write_posidx = true;
  opts.write_validx = false;
  opts.storage_attributes.cfile_block_size = FLAGS_cfile_test_block_size;
  opts.storage_attributes.encoding = PLAIN_ENCODING;
  CFileWriter w(opts, GetTypeInfo(STRING), false, std::move(sink));
  w.AddMetadataPair("header_key", "header_value");
  ASSERT_OK(w.Start());
  vector<Slice> slices;
  slices.emplace_back("HelloWorld");
  ASSERT_OK(w.AppendRawBlock(slices, 1, nullptr, Slice(), "raw-data"));
  ASSERT_OK(w.Finish());

  // Get the final size of the data
  unique_ptr<ReadableBlock> source;
  ASSERT_OK(fs_manager_->OpenBlock(id, &source));
  uint64_t file_size;
  ASSERT_OK(source->Size(&file_size));

  // Corrupt each bit and verify a corruption status is returned
  for (size_t i = 0; i < file_size; i++) {
    for (uint8_t flip = 0; flip < 8; flip++) {
      Status s = CorruptAndReadBlock(id, i, flip);
      ASSERT_TRUE(s.IsCorruption());
      ASSERT_STR_MATCHES(s.ToString(), "block [0-9]+");
    }
  }
}

TEST_P(TestCFileBothCacheMemoryTypes, TestNullInts) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  UInt32DataGenerator<true> generator;
  TestNullTypes(&generator, PLAIN_ENCODING, NO_COMPRESSION);
  TestNullTypes(&generator, PLAIN_ENCODING, LZ4);
  TestNullTypes(&generator, BIT_SHUFFLE, NO_COMPRESSION);
  TestNullTypes(&generator, BIT_SHUFFLE, LZ4);
  TestNullTypes(&generator, RLE, NO_COMPRESSION);
  TestNullTypes(&generator, RLE, LZ4);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestNullFloats) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  FPDataGenerator<FLOAT, true> generator;
  TestNullTypes(&generator, PLAIN_ENCODING, NO_COMPRESSION);
  TestNullTypes(&generator, BIT_SHUFFLE, NO_COMPRESSION);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestNullPrefixStrings) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  StringDataGenerator<true> generator("hello %zu");
  TestNullTypes(&generator, PLAIN_ENCODING, NO_COMPRESSION);
  TestNullTypes(&generator, PLAIN_ENCODING, LZ4);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestNullPlainStrings) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  StringDataGenerator<true> generator("hello %zu");
  TestNullTypes(&generator, PREFIX_ENCODING, NO_COMPRESSION);
  TestNullTypes(&generator, PREFIX_ENCODING, LZ4);
}

// Test for dictionary encoding
TEST_P(TestCFileBothCacheMemoryTypes, TestNullDictStrings) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  StringDataGenerator<true> generator("hello %zu");
  TestNullTypes(&generator, DICT_ENCODING, NO_COMPRESSION);
  TestNullTypes(&generator, DICT_ENCODING, LZ4);
}

TEST_P(TestCFileBothCacheMemoryTypes, TestReleaseBlock) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  unique_ptr<WritableBlock> sink;
  ASSERT_OK(fs_manager_->CreateNewBlock({}, &sink));
  ASSERT_EQ(WritableBlock::CLEAN, sink->state());
  WriterOptions opts;
  CFileWriter w(opts, GetTypeInfo(STRING), false, std::move(sink));
  ASSERT_OK(w.Start());
  BlockManager* bm = fs_manager_->block_manager();
  unique_ptr<fs::BlockCreationTransaction> transaction = bm->NewCreationTransaction();
  ASSERT_OK(w.FinishAndReleaseBlock(transaction.get()));
  ASSERT_OK(transaction->CommitCreatedBlocks());
}

TEST_P(TestCFileBothCacheMemoryTypes, TestLazyInit) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  // Create a small test file.
  BlockId block_id;
  {
    const int nrows = 1000;
    StringDataGenerator<false> generator("hello %04d");
    WriteTestFile(&generator, PREFIX_ENCODING, NO_COMPRESSION, nrows,
                  SMALL_BLOCKSIZE | WRITE_VALIDX, &block_id);
  }

  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test");
  int64_t initial_mem_usage = tracker->consumption();

  // Open it using a "counting" readable block.
  unique_ptr<ReadableBlock> block;
  ASSERT_OK(fs_manager_->OpenBlock(block_id, &block));
  size_t bytes_read = 0;
  unique_ptr<ReadableBlock> count_block(
      new CountingReadableBlock(std::move(block), &bytes_read));
  ASSERT_EQ(initial_mem_usage, tracker->consumption());

  // Lazily opening the cfile should not trigger any reads.
  ReaderOptions opts;
  opts.parent_mem_tracker = tracker;
  unique_ptr<CFileReader> reader;
  ASSERT_OK(CFileReader::OpenNoInit(std::move(count_block), opts, &reader));
  ASSERT_EQ(0, bytes_read);
  int64_t lazy_mem_usage = tracker->consumption();
  ASSERT_GT(lazy_mem_usage, initial_mem_usage);

  // But initializing it should (only the first time), and the reader's
  // memory usage should increase.
  ASSERT_OK(reader->Init(nullptr));
  ASSERT_GT(bytes_read, 0);
  size_t bytes_read_after_init = bytes_read;
  ASSERT_OK(reader->Init(nullptr));
  ASSERT_EQ(bytes_read_after_init, bytes_read);
  ASSERT_GT(tracker->consumption(), lazy_mem_usage);

  // And let's test non-lazy open for good measure; it should yield the
  // same number of bytes read.
  ASSERT_OK(fs_manager_->OpenBlock(block_id, &block));
  bytes_read = 0;
  count_block.reset(new CountingReadableBlock(std::move(block), &bytes_read));
  ASSERT_OK(CFileReader::Open(std::move(count_block), ReaderOptions(), &reader));
  ASSERT_EQ(bytes_read_after_init, bytes_read);
}

// Tests that the block cache keys used by CFileReaders are stable. That is,
// different reader instances operating on the same block should use the same
// block cache keys.
TEST_P(TestCFileBothCacheMemoryTypes, TestCacheKeysAreStable) {
  RETURN_IF_NO_NVM_CACHE(GetParam());

  // Set up block cache instrumentation.
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity(METRIC_ENTITY_server.Instantiate(&registry, "test_entity"));
  BlockCache* cache = BlockCache::GetSingleton();
  cache->StartInstrumentation(entity);

  // Create a small test file.
  BlockId block_id;
  {
    const int nrows = 1000;
    StringDataGenerator<false> generator("hello %04d");
    WriteTestFile(&generator, PREFIX_ENCODING, NO_COMPRESSION, nrows,
                  SMALL_BLOCKSIZE | WRITE_VALIDX, &block_id);
  }

  // Open and read from it twice, checking the block cache statistics.
  for (int i = 0; i < 2; i++) {
    unique_ptr<ReadableBlock> source;
    ASSERT_OK(fs_manager_->OpenBlock(block_id, &source));
    unique_ptr<CFileReader> reader;
    ASSERT_OK(CFileReader::Open(std::move(source), ReaderOptions(), &reader));

    gscoped_ptr<IndexTreeIterator> iter;
    iter.reset(IndexTreeIterator::Create(nullptr, reader.get(), reader->posidx_root()));
    ASSERT_OK(iter->SeekToFirst());

    BlockHandle bh;
    ASSERT_OK(reader->ReadBlock(nullptr, iter->GetCurrentBlockPointer(),
                                CFileReader::CACHE_BLOCK,
                                &bh));

    // The first time through, we miss in the seek and in the ReadBlock().
    // But the second time through, both are hits, because we've got the same
    // cache keys as before.
    ASSERT_EQ(i * 2, down_cast<Counter*>(
        entity->FindOrNull(METRIC_block_cache_hits_caching).get())->value());
  }
}

// Inject failures in nvm allocation and ensure that we can still read a file.
TEST_P(TestCFileBothCacheMemoryTypes, TestNvmAllocationFailure) {
  if (GetParam() != Cache::MemoryType::NVM) return;
  RETURN_IF_NO_NVM_CACHE(GetParam());

  FLAGS_nvm_cache_simulate_allocation_failure = true;
  TestReadWriteFixedSizeTypes<UInt32DataGenerator<false> >(PLAIN_ENCODING);
}

class TestCFileDifferentCodecs : public TestCFile,
                                 public testing::WithParamInterface<CompressionType> {
};

INSTANTIATE_TEST_CASE_P(Codecs, TestCFileDifferentCodecs,
                        ::testing::Values(NO_COMPRESSION, SNAPPY, LZ4, ZLIB));

// Read/write a file with uncompressible data (random int32s)
TEST_P(TestCFileDifferentCodecs, TestUncompressible) {
  auto codec = GetParam();
  const size_t nrows = 1000000;
  BlockId block_id;
  size_t rdrows;

  // Generate a plain-encoded file with random (uncompressible) data.
  // This exercises the code path which short-circuits compression
  // when the codec is not able to be effective on the input data.
  {
    RandomInt32DataGenerator int_gen;
    WriteTestFile(&int_gen, PLAIN_ENCODING, codec, nrows,
                  NO_FLAGS, &block_id);
    TimeReadFile(fs_manager_.get(), block_id, &rdrows);
    ASSERT_EQ(nrows, rdrows);
  }
}

} // namespace cfile
} // namespace kudu
