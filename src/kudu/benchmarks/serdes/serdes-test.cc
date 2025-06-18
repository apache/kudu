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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <flatbuffers/buffer.h>
#include <flatbuffers/flatbuffer_builder.h>
#include <flatbuffers/string.h>
#include <flatbuffers/vector.h>
#include <flatbuffers/verifier.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/benchmarks/serdes/arrays.fb.h"
#include "kudu/benchmarks/serdes/arrays.pb.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using arrays::test::ArrayPB;
using arrays::test::CreateContentDirect;
using arrays::test::CreateInt32Direct;
using arrays::test::CreateUInt64Direct;
using arrays::test::Content;
using arrays::test::Int16;
using arrays::test::Int32;
using arrays::test::Int64;
using arrays::test::GetContent;
using arrays::test::String;
using arrays::test::ScalarArray;
using arrays::test::UInt64;
using arrays::test::VerifyContentBuffer;
using flatbuffers::FlatBufferBuilder;
using flatbuffers::Verifier;
using std::iota;
using std::string;
using std::string_view;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// Various serialization/de-serialization test scenarios using Flatbuffers
// and Protobuf for one-dimensional arrays based on data structures defined
// in the 'arrays.fbs' and 'arrays.proto' IDL files. For Flatbuffers-specific
// C++ bindings and reference, see https://flatbuffers.dev/languages/cpp/
class SerDesTest : public KuduTest {
};

TEST_F(SerDesTest, FlatbuffersBasic) {
  constexpr const size_t kBufSize = 1024;
  const vector<int32_t> values_src{ 1, 2, 3, 4, 5 };
  const vector<uint8_t> validity_src{ 0b00010111 };

  // Add the data using FlatBufferBuilder.
  FlatBufferBuilder builder(kBufSize);
  {
    auto values = CreateInt32Direct(builder, &values_src);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::Int32,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract the data from the buffer. The instance of FlatBufferBuilder
  // needs to stay alive since it owns the buffer. Alternatively, it's possible
  // to use FlatBufferBuilder::Release() to get DetachedBuffer, passing the
  // ownership to the instance of DetachedBuffer.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);

    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::Int32, array_type);

    const auto* values = content->data_as<Int32>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(5, values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      ASSERT_EQ(i + 1, values->Get(i));
    }

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), validity_src.size()));

    // Verify raw data access.
    const int32_t* data_src = values_src.data();
    const int32_t* data = values->data();
    for (size_t i = 0; i < values_src.size(); ++i) {
      SCOPED_TRACE(Substitute("array index: $0", i));
      ASSERT_TRUE(*data_src++ == *data++);
    }
  }
}

TEST_F(SerDesTest, FlatbuffersPlainSrcBuffer) {
  constexpr const size_t kBufSize = 1024;
  const vector<int16_t> values_src{ 1, 2, 3, 4, 5, 6, 7 };
  const vector<uint8_t> validity_src{ 0b01010111 };

  FlatBufferBuilder builder(kBufSize);
  {
    // Create helper objects accessing the source data via raw pointer,
    // relying on the C-style array memory layout.
    auto values_vec = builder.CreateVector(values_src.data(), values_src.size());
    auto values = Int16::Traits::Create(builder, values_vec);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::Int16,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract data from the buffer and verify it matches the source.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::Int16, array_type);

    const auto* values = content->data_as<Int16>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(7, values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      ASSERT_EQ(i + 1, values->Get(i));
    }

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), validity_src.size()));

    // Verify raw data access.
    const int16_t* data_src = values_src.data();
    const int16_t* data = values->data();
    for (size_t i = 0; i < 7; ++i) {
      SCOPED_TRACE(Substitute("array index: $0", i));
      ASSERT_TRUE(*data_src++ == *data++);
    }
  }
}

TEST_F(SerDesTest, FlatbuffersStringSrcVector) {
  constexpr const size_t kBufSize = 1024;
  const vector<string> values_src{ "", "1", "02", "003", "0004", "00005" };
  const vector<uint8_t> validity_src{ 0b00111110 };

  FlatBufferBuilder builder(kBufSize);
  {
    // Create helper objects accessing the source data via raw pointer,
    // relying on the C-style array memory layout.
    auto values_vec = builder.CreateVectorOfStrings(values_src);
    auto values = String::Traits::Create(builder, values_vec);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::String,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract data from the buffer and verify it matches the source.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::String, array_type);

    const auto* values = content->data_as<String>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(values_src.size(), values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      ASSERT_EQ(values_src[i], values->Get(i)->string_view());

      // Do explicit memory comparison using raw data accessors.
      const auto ref_size = values_src[i].size();
      ASSERT_EQ(ref_size, values->Get(i)->size());
      ASSERT_EQ(0, memcmp(values->Get(i)->Data(), values_src[i].data(), ref_size));
    }

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), validity_src.size()));
  }
}

TEST_F(SerDesTest, FlatbuffersStringViewSrcVector) {
  constexpr const size_t kBufSize = 1024;
  const vector<string_view> values_src{ "1", "02", "003" };
  const vector<uint8_t> validity_src{ 0b00000111 };

  FlatBufferBuilder builder(kBufSize);
  {
    // Create helper objects accessing the source data via raw pointer,
    // relying on the C-style array memory layout.
    auto values_vec = builder.CreateVectorOfStrings(values_src);
    auto values = String::Traits::Create(builder, values_vec);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::String,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract data from the buffer and verify it matches the source.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::String, array_type);

    const auto* values = content->data_as<String>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(values_src.size(), values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      ASSERT_EQ(values_src[i], values->Get(i)->string_view());

      // Do explicit memory comparison using raw data accessors.
      const auto ref_size = values_src[i].size();
      ASSERT_EQ(ref_size, values->Get(i)->size());
      ASSERT_EQ(0, memcmp(values->Get(i)->Data(), values_src[i].data(), ref_size));
    }

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), validity_src.size()));
  }
}

TEST_F(SerDesTest, FlatbuffersSliceSrcVector) {
  constexpr const size_t kBufSize = 1024;
  const vector<Slice> values_src{ "-1", "0", "1", "02", "003", "100000000000" };
  const vector<uint8_t> validity_src{ 0b00101111 };

  FlatBufferBuilder builder(kBufSize);
  {
    // Create helper objects accessing the source data via raw pointer,
    // relying on the C-style array memory layout.
    auto values_vec = builder.CreateVectorOfStrings(values_src);
    auto values = String::Traits::Create(builder, values_vec);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::String,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract data from the buffer and verify it matches the source.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::String, array_type);

    const auto* values = content->data_as<String>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(values_src.size(), values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      // Do explicit memory comparison using raw data accessors.
      const auto ref_size = values_src[i].size();
      ASSERT_EQ(ref_size, values->Get(i)->size());
      ASSERT_EQ(0, memcmp(values->Get(i)->Data(), values_src[i].data(), ref_size));
    }

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), validity_src.size()));
  }
}

TEST_F(SerDesTest, FlatbuffersBuilderUninitializedVector) {
  constexpr const size_t kBufSize = 1024;
  const int64_t values_src[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
  const vector<uint8_t> validity_src{ 0b00000010, 0b11010111 };

  FlatBufferBuilder builder(kBufSize);
  {
    int64_t* buf = nullptr;
    auto v = builder.CreateUninitializedVector(11, &buf);
    ASSERT_NE(nullptr, buf);
    // Copy the data into the allocated, but uninitialized vector.
    memcpy(buf, values_src, 11 * sizeof(int64_t));
    auto values = Int64::Traits::Create(builder, v);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::Int64,
                                       values.Union(),
                                       &validity_src));
  }

  // Extract data from the buffer and verify it matches the source.
  const uint8_t* buf = builder.GetBufferPointer();
  {
    Verifier verifier(buf, kBufSize);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::Int64, array_type);

    const auto* values = content->data_as<Int64>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(11, values->size());

    const auto* validity = content->validity();
    ASSERT_EQ(validity_src.size(), validity->size());
    ASSERT_EQ(0, memcmp(validity_src.data(), validity->data(), 2));

    // Verify raw data access.
    const int64_t* data_src = values_src;
    const int64_t* data = values->data();
    ASSERT_EQ(0, memcmp(data, data_src, 11 * sizeof(int64_t)));
  }
}

TEST_F(SerDesTest, FlatbuffersReleaseRaw) {
  constexpr const size_t kBufSize = 1024;
  const vector<int32_t> values_src{ 1, 2, 3, 4, 5 };
  const vector<uint8_t> validity_src{ 0b00010111 };

  size_t buf_size = 0;
  size_t buf_offset = 0;
  unique_ptr<uint8_t[]> buf_data;

  {
    // Add the data using FlatBufferBuilder.
    FlatBufferBuilder builder(kBufSize);
    auto values = CreateInt32Direct(builder, &values_src);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::Int32,
                                       values.Union(),
                                       &validity_src));
    buf_data.reset(builder.ReleaseRaw(buf_size, buf_offset));
  }
  ASSERT_NE(nullptr, buf_data.get());
  ASSERT_GT(buf_size, 0);
  ASSERT_GT(buf_size, buf_offset);

  {
    // The serialized data starts at 'buf_offset'.
    Verifier verifier(buf_data.get() + buf_offset, buf_size - buf_offset);
    ASSERT_TRUE(VerifyContentBuffer(verifier));

    const Content* content = GetContent(buf_data.get() + buf_offset);
    ASSERT_NE(nullptr, content);
    const auto array_type = content->data_type();
    ASSERT_EQ(ScalarArray::Int32, array_type);

    const auto* values = content->data_as<Int32>()->values();
    ASSERT_NE(nullptr, values);
    ASSERT_EQ(5, values->size());
    for (size_t i = 0; i < values->size(); ++i) {
      ASSERT_EQ(i + 1, values->Get(i));
    }

    const auto* validity = content->validity();
    ASSERT_NE(nullptr, validity);
    ASSERT_EQ(1, validity->size());
    ASSERT_EQ(validity_src[0], validity->Get(0));

    // Verify raw data access for the data, element by element.
    const int32_t* data_src = values_src.data();
    const int32_t* data = values->data();
    for (size_t i = 0; i < values_src.size(); ++i) {
      SCOPED_TRACE(Substitute("array index: $0", i));
      ASSERT_TRUE(*data_src++ == *data++);
    }

    // Verify raw data access for the validity bits, 8 bits at a time.
    const uint8_t* validity_bits_src = validity_src.data();
    const uint8_t* validity_bits_dst = validity->data();
    for (size_t i = 0; i < validity_src.size(); ++i) {
      SCOPED_TRACE(Substitute("validity index: $0", i));
      ASSERT_TRUE(*validity_bits_src++ == *validity_bits_dst++);
    }
  }
}

TEST_F(SerDesTest, FlatbuffersBenchmark) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const size_t kIterNum = 100000;
  constexpr const size_t kElemNum = 1024;
  constexpr const size_t kBufSize = kElemNum * (8 + 1 + 1);
  static_assert(kElemNum % 8 == 0);

  vector<uint64_t> data_src(kElemNum);
  iota(data_src.begin(), data_src.end(), 0);

  vector<uint8_t> validity_src(kElemNum / 8);
  iota(validity_src.begin(), validity_src.end(), 0);

  Stopwatch timer_s;
  Stopwatch timer_d;

  Verifier::Options opt;
  opt.check_nested_flatbuffers = true;
  opt.max_depth = 2;
  opt.max_tables = 2;
  opt.max_size = 2 * kBufSize;

  size_t buf_min_size = std::numeric_limits<size_t>::max();
  size_t buf_max_size = std::numeric_limits<size_t>::min();
  size_t buf_sum_size = 0;

  std::mt19937 gen(SeedRandom());
  for (size_t iter = 0; iter < kIterNum; ++iter) {
    uint8_t* buf = nullptr;
    std::shuffle(data_src.begin(), data_src.end(), gen);
    std::shuffle(validity_src.begin(), validity_src.end(), gen);

    timer_s.resume();
    FlatBufferBuilder builder(kBufSize);
    auto values = CreateUInt64Direct(builder, &data_src);
    builder.Finish(CreateContentDirect(builder,
                                       ScalarArray::UInt64,
                                       values.Union(),
                                       &validity_src));

    buf = builder.GetBufferPointer();
    timer_s.stop();

    ASSERT_NE(nullptr, buf);

    timer_d.resume();
    Verifier verifier(buf, kBufSize, opt);
    const auto verification_result = VerifyContentBuffer(verifier);
    const Content* content = GetContent(buf);
    const auto* result_data = content->data_as<UInt64>()->values();
    timer_d.stop();

    ASSERT_TRUE(verification_result);
    ASSERT_EQ(data_src.size(), result_data->size());
    for (size_t i = 0; i < data_src.size(); ++i) {
      ASSERT_EQ(data_src[i], result_data->Get(i));
    }

    // Verify raw data access.
    const uint64_t* raw_data_src = data_src.data();
    const uint64_t* raw_data = result_data->data();
    ASSERT_EQ(0, memcmp(raw_data_src,
                        raw_data,
                        sizeof(uint64_t) * data_src.size()));
    // Collect stats on the serialized data size.
    const size_t buf_size = builder.GetSize();
    buf_sum_size += buf_size;
    if (buf_size > buf_max_size) {
      buf_max_size = buf_size;
    }
    if (buf_size < buf_min_size) {
      buf_min_size = buf_size;
    }
  }

  const auto& timer_s_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd", kElemNum, kIterNum);
  LOG(INFO) << Substitute("Flatbuffers serialize  : $0 $1",
                          timer_s_desc, timer_s.elapsed().ToString());
  const auto& timer_d_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd", kElemNum, kIterNum);
  LOG(INFO) << Substitute("Flatbuffers deserialize: $0 $1",
                          timer_d_desc, timer_d.elapsed().ToString());
  const auto& buffer_size_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd min=%5zd max=%5zd average=%5zd",
      kElemNum, kIterNum, buf_min_size, buf_max_size, buf_sum_size / kIterNum);
  LOG(INFO) << Substitute("Flatbuffers buffer size: $0",
                          buffer_size_desc);
}

TEST_F(SerDesTest, ProtobufBasic) {
  const vector<int32_t> values_src{ 1, 2, 3, 4, 5 };
  const vector<bool> validity_src{ true, true, true, false, true };

  string message_str;
  {
    ArrayPB message;
    message.mutable_validity()->Add(validity_src.begin(), validity_src.end());
    auto* data = message.mutable_val_int32();
    data->mutable_values()->Add(values_src.begin(), values_src.end());

    ASSERT_TRUE(message.SerializeToString(&message_str));
  }

  {
    ArrayPB message;
    ASSERT_TRUE(message.ParseFromString(message_str));

    const auto& validity = message.validity();
    ASSERT_EQ(validity_src.size(), validity.size());
    for (auto i = 0; i < validity.size(); ++i) {
      ASSERT_EQ(validity_src[i], validity[i]);
    }

    ASSERT_TRUE(message.has_val_int32());
    ASSERT_FALSE(message.has_val_int64());
    const auto& data = message.val_int32();
    ASSERT_EQ(5, data.values_size());
    ASSERT_EQ(values_src.size(), data.values_size());

    for (auto i = 0; i < data.values_size(); ++i) {
      ASSERT_EQ(values_src[i], data.values(i));
    }
  }
}

TEST_F(SerDesTest, ProtobufBenchmark) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const size_t kIterNum = 100000;
  constexpr const size_t kElemNum = 1024;
  constexpr const size_t kDataSeqStart =
      std::numeric_limits<uint64_t>::max() - 2 * kElemNum;

  static_assert(kElemNum % 8 == 0);

  vector<uint64_t> data_src(kElemNum);
  iota(data_src.begin(), data_src.end(), kDataSeqStart);

  vector<bool> validity_src(kElemNum);
  for (size_t i = 0; i < kElemNum; ++i) {
    validity_src[i] = (i % 8 != 0);
  }

  Stopwatch timer_s;
  Stopwatch timer_d;

  size_t buf_min_size = std::numeric_limits<size_t>::max();
  size_t buf_max_size = std::numeric_limits<size_t>::min();
  size_t buf_sum_size = 0;

  std::mt19937 gen(SeedRandom());
  for (auto iter = 0; iter < kIterNum; ++iter) {
    std::shuffle(data_src.begin(), data_src.end(), gen);
    std::shuffle(validity_src.begin(), validity_src.end(), gen);
    string message_str;
    {
      timer_s.resume();
      ArrayPB message;
      auto* validity = message.mutable_validity();
      validity->Add(validity_src.begin(), validity_src.end());
      auto* data = message.mutable_val_uint64();
      data->mutable_values()->Add(data_src.begin(), data_src.end());

      const auto status = message.SerializeToString(&message_str);
      timer_s.stop();
      ASSERT_TRUE(status);
    }

    {
      timer_d.resume();
      ArrayPB message;
      const auto status = message.ParseFromString(message_str);
      const auto is_uint64 = message.has_val_uint64();
      const auto& data = message.val_uint64();
      const auto& validity = message.validity();
      timer_d.stop();

      // Do verification outside of the performance timer section.
      ASSERT_TRUE(status);
      ASSERT_TRUE(is_uint64);
      ASSERT_EQ(data_src.size(), data.values_size());
      ASSERT_EQ(0, memcmp(data_src.data(),
                          data.values().data(),
                          data_src.size() * sizeof(uint64_t)));
      ASSERT_EQ(validity_src.size(), validity.size());
      for (auto i = 0; i < validity.size(); ++i) {
        ASSERT_EQ(validity_src[i], validity[i]);
      }
    }
    // Collect stats on the serialized data size.
    const size_t buf_size = message_str.size();
    buf_sum_size += buf_size;
    if (buf_size > buf_max_size) {
      buf_max_size = buf_size;
    }
    if (buf_size < buf_min_size) {
      buf_min_size = buf_size;
    }
  }

  const auto& timer_s_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd", kElemNum, kIterNum);
  LOG(INFO) << Substitute("Protobuf    serialize  : $0 $1",
                          timer_s_desc, timer_s.elapsed().ToString());
  const auto& timer_d_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd", kElemNum, kIterNum);
  LOG(INFO) << Substitute("Protobuf    deserialize: $0 $1",
                          timer_d_desc, timer_d.elapsed().ToString());
  const auto& buffer_size_desc = StringPrintf(
      "ElemNum=%5zd Iterations=%8zd min=%5zd max=%5zd average=%5zd",
      kElemNum, kIterNum, buf_min_size, buf_max_size, buf_sum_size / kIterNum);
  LOG(INFO) << Substitute("Flatbuffers buffer size: $0",
                          buffer_size_desc);
}

} // namespace kudu
