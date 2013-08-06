// Copyright (c) 2013, Cloudera, inc.
//
// Micro benchmark for writing/reading bit streams and Kudu specific
// run-length encoding (RLE) APIs. Currently only covers booleans and
// the most performance sensitive APIs. NB: Impala contains a RLE
// micro benchmark (rle-benchmark.cc).
//

#include <glog/logging.h>

#include "util/bit-stream-utils.h"
#include "util/rle-encoding.h"
#include "util/stopwatch.h"

namespace kudu {

// Measure writing and reading single-bit streams
void BooleanBitStream() {
  const int num_iters = 1024 * 1024;

  faststring buffer(1024 * 1024);
  BitWriter writer(&buffer);

  // Write alternating strings of repeating 0's and 1's
  for (int i = 0; i < num_iters; ++i) {
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
    writer.PutValue(i % 2, 1);
  }
  writer.Flush();

  LOG(INFO) << "Wrote " << writer.bytes_written() << " bytes";

  BitReader reader(buffer.data(), writer.bytes_written());
  for (int i = 0; i < num_iters; ++i) {
    bool val;
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
    reader.GetValue(1, &val);
  }
}

// Measure bulk puts and decoding runs of RLE bools
void BooleanRLE() {
  const int num_iters = 3 * 1024;

  faststring buffer(45 * 1024);
  RleEncoder<bool> encoder(&buffer, 1);

  for (int i = 0; i < num_iters; i++) {
    encoder.Put(false, 100 * 1024);
    encoder.Put(true, 3);
    encoder.Put(false, 3);
    encoder.Put(true, 213 * 1024);
    encoder.Put(false, 300);
    encoder.Put(true, 8);
    encoder.Put(false, 4);
  }

  LOG(INFO) << "Wrote " << encoder.len() << " bytes";

  RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);
  bool val = false;
  size_t run_length;
  for (int i = 0; i < num_iters; i++) {
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
    decoder.GetNextRun(&val, &run_length);
  }
}

} // namespace kudu

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  LOG_TIMING(INFO, "BooleanBitStream") {
    kudu::BooleanBitStream();
  }

  LOG_TIMING(INFO, "BooleanRLE") {
    kudu::BooleanRLE();
  }

  return 0;
}
