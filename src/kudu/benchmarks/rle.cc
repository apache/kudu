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
//
// Micro benchmark for writing/reading bit streams and Kudu specific
// run-length encoding (RLE) APIs. Currently only covers booleans and
// the most performance sensitive APIs. NB: Impala contains a RLE
// micro benchmark (rle-benchmark.cc).
//

#include <cstddef>
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/util/bit-stream-utils.h"
#include "kudu/util/bit-stream-utils.inline.h"
#include "kudu/util/bit-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/logging.h"
#include "kudu/util/rle-encoding.h"
#include "kudu/util/stopwatch.h"

DEFINE_int32(bitstream_num_bytes, 1 * 1024 * 1024,
             "Number of bytes worth of bits to write and read from the bitstream");

namespace kudu {

// Measure writing and reading single-bit streams
int BooleanBitStream(faststring* buffer) {
  // Write alternating strings of repeating 0's and 1's
  const auto num_bytes = buffer->capacity();
  BitWriter writer(buffer);
  for (auto i = 0; i < num_bytes; ++i) {
    auto val = i % 2;
    for (auto j = 0; j < 8; ++j) {
      writer.PutValue(val, 1);
    }
  }
  writer.Flush();
  const auto bytes_written = writer.bytes_written();

  BitReader reader(buffer->data(), bytes_written);
  for (auto i = 0; i < num_bytes; ++i) {
    bool val;
    for (auto j = 0; j < 8; ++j) {
      reader.GetValue(1, &val);
    }
  }

  return bytes_written;
}

// Measure bulk puts and decoding runs of RLE bools
int BooleanRLE(faststring* buffer) {
  constexpr int kNumIters = 3 * 1024;

  RleEncoder<bool> encoder(buffer, 1);
  for (auto i = 0; i < kNumIters; ++i) {
    encoder.Put(false, 100 * 1024);
    encoder.Put(true, 3);
    encoder.Put(false, 3);
    encoder.Put(true, 213 * 1024);
    encoder.Put(false, 300);
    encoder.Put(true, 8);
    encoder.Put(false, 4);
  }

  const auto bytes_written = encoder.len();

  RleDecoder<bool> decoder(buffer->data(), encoder.len(), 1);
  bool val = false;
  for (auto i = 0; i < kNumIters * 7; ++i) {
    ignore_result(decoder.GetNextRun(&val, MathLimits<size_t>::kMax));
  }

  return bytes_written;
}

int BitUtilCeil(int num_iter) {
  volatile int res = 0;
  for (int i = 0; i < num_iter; ++i) {
    res = BitUtil::Ceil(i, 8);
  }
  return res;
}

int BitUtilCeilLog2Div(int num_iter) {
  volatile int res;
  for (int i = 0; i < num_iter; ++i) {
    res = BitUtil::Ceil<3>(i);
  }
  return res;
}

} // namespace kudu

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::ParseCommandLineFlags(&argc, &argv, true);
  kudu::InitGoogleLoggingSafe(argv[0]);

  {
    // kudu::BooleanBitStream() assumes fastring::capacity() returns
    // the actual capacity of the buffer.
    CHECK_GT(FLAGS_bitstream_num_bytes, kudu::faststring::kInitialCapacity);

    int bytes_written = 0;
    kudu::faststring buffer(FLAGS_bitstream_num_bytes);
    LOG_TIMING(INFO, "BooleanBitStream") {
      bytes_written = kudu::BooleanBitStream(&buffer);
    }
    LOG(INFO) << "Wrote " << bytes_written << " bytes";
  }

  {
    int bytes_written = 0;
    kudu::faststring buffer(45 * 1024);
    LOG_TIMING(INFO, "BooleanRLE") {
      bytes_written = kudu::BooleanRLE(&buffer);
    }
    LOG(INFO) << "Wrote " << bytes_written << " bytes";
  }

  {
    int res = 0;
    LOG_TIMING(INFO, "BitUtil::Ceil(..., 8)") {
      res = kudu::BitUtilCeil(1000000000);
    }
    LOG(INFO) << "Result: " << res;
  }

  {
    int res = 0;
    LOG_TIMING(INFO, "BitUtil::Ceil<3>(...)") {
      res = kudu::BitUtilCeilLog2Div(1000000000);
    }
    LOG(INFO) << "Result: " << res;
  }

  return 0;
}
