// Copyright (c) 2013, Cloudera, inc

#ifndef KUDU_CFILE_TEST_BASE_H
#define KUDU_CFILE_TEST_BASE_H

#include <glog/logging.h>

#include "common/columnblock.h"
#include "gutil/stringprintf.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/test_macros.h"
#include "util/test_util.h"
#include "util/stopwatch.h"
#include "util/status.h"
#include "cfile-test-base.h"
#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"

DEFINE_int32(cfile_test_block_size, 1024,
             "Block size to use for testing cfiles. "
             "Default is low to stress code, but can be set higher for "
             "performance testing");

namespace kudu {
namespace cfile {

class CFileTestBase : public KuduTest {
protected:
  void WriteTestFileStrings(const string &path,
                            EncodingType encoding,
                            CompressionType compression,
                            int num_entries,
                            const char *format) {
    shared_ptr<WritableFile> sink;
    ASSERT_STATUS_OK( env_util::OpenFileForWrite(env_.get(), path, &sink) );
    WriterOptions opts;
    opts.write_posidx = true;
    opts.write_validx = true;
    // Use a smaller block size to exercise multi-level
    // indexing.
    opts.block_size = FLAGS_cfile_test_block_size;
    opts.compression = compression;
    Writer w(opts, STRING, encoding, sink);

    ASSERT_STATUS_OK(w.Start());

    // Append given number of values to the test tree
    char data[20];
    for (int i = 0; i < num_entries; i++) {
      int len = snprintf(data, sizeof(data), format, i);
      Slice slice(data, len);

      Status s = w.AppendEntries(&slice, 1, 0);
      // Dont use ASSERT because it accumulates all the logs
      // even for successes
      if (!s.ok()) {
        FAIL() << "Failed Append(" << i << ")";
      }
    }

    ASSERT_STATUS_OK(w.Finish());
  }

  void WriteTestFileInts(const string &path,
                         EncodingType encoding,
                         CompressionType compression,
                         int num_entries) {
    shared_ptr<WritableFile> sink;
    ASSERT_STATUS_OK( env_util::OpenFileForWrite(env_.get(), path, &sink) );
    WriterOptions opts;
    opts.write_posidx = true;
    // Use a smaller block size to exercise multi-level
    // indexing.
    opts.block_size = FLAGS_cfile_test_block_size;
    opts.compression = compression;
    Writer w(opts, UINT32, encoding, sink);

    ASSERT_STATUS_OK(w.Start());

    uint32_t block[8096];
    size_t stride = sizeof(uint32_t);

    // Append given number of values to the test tree
    int i = 0;
    while (i < num_entries) {
      int towrite = std::min(num_entries - i, 8096);
      for (int j = 0; j < towrite; j++) {
        block[j] = i++ * 10;
      }

      Status s = w.AppendEntries(block, towrite, stride);
      // Dont use ASSERT because it accumulates all the logs
      // even for successes
      if (!s.ok()) {
        FAIL() << "Failed Append(" << (i - towrite) << ")";
      }
    }

    ASSERT_STATUS_OK(w.Finish());
  }
};

// Fast unrolled summing of a vector.
// GCC's auto-vectorization doesn't work here, because there isn't
// enough guarantees on alignment and it can't seem to decude the
// constant stride.
template<class Indexable>
uint64_t FastSum(const Indexable &data, size_t n) {
  uint64_t sums[4] = {0, 0, 0, 0};
  int rem = n;
  int i = 0;
  while (rem >= 4) {
    sums[0] += data[i];
    sums[1] += data[i+1];
    sums[2] += data[i+2];
    sums[3] += data[i+3];
    i += 4;
    rem -= 4;
  }
  while (rem > 0) {
    sums[3] += data[i++];
    rem--;
  }
  return sums[0] + sums[1] + sums[2] + sums[3];
}

static void TimeReadFile(const string &path, size_t *count_ret) {
  Env *env = Env::Default();
  Status s;

  gscoped_ptr<CFileReader> reader;
  ASSERT_STATUS_OK(CFileReader::Open(env, path, ReaderOptions(), &reader));

  gscoped_ptr<CFileIterator> iter;
  ASSERT_STATUS_OK( reader->NewIterator(&iter) );
  iter->SeekToOrdinal(0);

  Arena arena(8192, 8*1024*1024);
  int count = 0;
  switch (reader->data_type()) {
    case UINT32:
    {
      ScopedColumnBlock<UINT32> cb(8192);

      uint64_t sum = 0;
      while (iter->HasNext()) {
        size_t n = cb.size();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        sum += FastSum(cb, n);
        count += n;
        cb.arena()->Reset();
      }
      LOG(INFO) << "Sum: " << sum;
      LOG(INFO) << "Count: " << count;
      break;
    }
    case STRING:
    {
      ScopedColumnBlock<STRING> cb(100);
      uint64_t sum_lens = 0;
      while (iter->HasNext()) {
        size_t n = cb.size();
        ASSERT_STATUS_OK_FAST(iter->CopyNextValues(&n, &cb));
        for (int i = 0; i < n; i++) {
          sum_lens += cb[i].size();
        }
        count += n;
        cb.arena()->Reset();
      }
      LOG(INFO) << "Sum of value lengths: " << sum_lens;
      LOG(INFO) << "Count: " << count;
      break;
    }
    default:
      FAIL() << "Unknown type: " << reader->data_type();
  }
  *count_ret = count;
}

} // namespace cfile
} // namespace kudu

#endif
