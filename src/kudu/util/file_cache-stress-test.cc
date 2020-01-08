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
#include <deque>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/file_cache-test-util.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(test_num_producer_threads, 1, "Number of producer threads");
DEFINE_int32(test_num_consumer_threads, 4, "Number of consumer threads");
DEFINE_int32(test_duration_secs, 2, "Number of seconds to run the test");

DECLARE_bool(cache_force_single_shard);

using std::deque;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

// FD limit to enforce during the test.
static const int kTestMaxOpenFiles = 100;

class FileCacheStressTest : public KuduTest {

// Like CHECK_OK(), but dumps the contents of the cache before failing.
//
// The output of ToDebugString() tends to be long enough that LOG() truncates
// it, so we must split it ourselves before logging.
#define TEST_CHECK_OK(to_call) do {                                       \
    const Status& _s = (to_call);                                         \
    if (!_s.ok()) {                                                       \
      LOG(INFO) << "Dumping cache contents";                              \
      vector<string> lines = strings::Split(cache_->ToDebugString(), "\n",\
                                            strings::SkipEmpty());        \
      for (const auto& l : lines) {                                       \
        LOG(INFO) << l;                                                   \
      }                                                                   \
    }                                                                     \
    CHECK(_s.ok()) << "Bad status: " << _s.ToString();                    \
  } while (0)

 public:
  typedef unordered_map<string, unordered_map<string, int>> MetricMap;

  FileCacheStressTest()
      : rand_(SeedRandom()),
        running_(1) {
    // Use a single shard. Otherwise, the cache can be a little bit "sloppy"
    // depending on the number of CPUs on the system.
    FLAGS_cache_force_single_shard = true;
    cache_.reset(new FileCache("test",
                               env_,
                               kTestMaxOpenFiles,
                               scoped_refptr<MetricEntity>()));
  }

  void SetUp() override {
    ASSERT_OK(cache_->Init());
  }

  void ProducerThread() {
    Random rand(rand_.Next32());
    ObjectIdGenerator oid_generator;
    MetricMap metrics;

    do {
      // Choose randomly between creating a file to be opened read-only and one
      // to be opened read-write.
      bool use_rwf = rand.OneIn(2);

      // Create a new file with some (0-32k) random data in it. The file name
      // will signal what file type to open as.
      string next_file_name = GetTestPath((use_rwf ? kRWFPrefix : kRAFPrefix) +
                                          oid_generator.Next());
      {
        unique_ptr<WritableFile> next_file;
        CHECK_OK(env_->NewWritableFile(next_file_name, &next_file));
        uint8_t buf[rand.Uniform((32 * 1024) - 1) + 1];
        CHECK_OK(next_file->Append(GenerateRandomChunk(buf, sizeof(buf), &rand)));
        CHECK_OK(next_file->Close());
      }
      {
        std::lock_guard<simple_spinlock> l(lock_);
        InsertOrDie(&available_files_, next_file_name, 0);
      }
      metrics[BaseName(next_file_name)]["create"] = 1;
    } while (!running_.WaitFor(MonoDelta::FromMilliseconds(1)));

    // Update the global metrics map.
    MergeNewMetrics(std::move(metrics));
  }

  void ConsumerThread() {
    // Each thread has its own PRNG to minimize contention on the main one.
    Random rand(rand_.Next32());

    // Active opened files in this thread.
    deque<shared_ptr<RWFile>> rwfs;
    deque<shared_ptr<RandomAccessFile>> rafs;

    // Metrics generated by this thread. They will be merged into the main
    // metrics map when the thread is done.
    MetricMap metrics;

    do {
      // Pick an action to perform. Distribution:
      // 20% open
      // 15% close
      // 35% read
      // 20% write
      // 10% delete
      int next_action = rand.Uniform(100);

      if (next_action < 20) {
        // Open an existing file.
        string to_open;
        if (!GetRandomFile(OPEN, &rand, &to_open)) {
          continue;
        }
        if (HasPrefixString(BaseName(to_open), kRWFPrefix)) {
          shared_ptr<RWFile> rwf;
          TEST_CHECK_OK(cache_->OpenExistingFile(to_open, &rwf));
          rwfs.emplace_back(std::move(rwf));
        } else {
          CHECK(HasPrefixString(BaseName(to_open), kRAFPrefix));

          shared_ptr<RandomAccessFile> raf;
          TEST_CHECK_OK(cache_->OpenExistingFile(to_open, &raf));
          rafs.emplace_back(std::move(raf));
        }
        FinishedOpen(to_open);
        metrics[BaseName(to_open)]["open"]++;
      } else if (next_action < 35) {
        // Close a file.
        if (rwfs.empty() && rafs.empty()) {
          continue;
        }
        shared_ptr<File> f;
        if (rafs.empty() || (!rwfs.empty() && rand.OneIn(2))) {
          f = rwfs.front();
          rwfs.pop_front();
        } else {
          f = rafs.front();
          rafs.pop_front();
        }
        metrics[BaseName(f->filename())]["close"]++;
      } else if (next_action < 70) {
        // Read a random chunk from a file.
        if (rwfs.empty() && rafs.empty()) {
          continue;
        }
        if (rafs.empty() || (!rwfs.empty() && rand.OneIn(2))) {
          TEST_CHECK_OK(ReadRandomChunk(rwfs, &metrics, &rand));
        } else {
          TEST_CHECK_OK(ReadRandomChunk(rafs, &metrics, &rand));
        }
      } else if (next_action < 90) {
        // Write a random chunk to a file.
        TEST_CHECK_OK(WriteRandomChunk(rwfs, &metrics, &rand));
      } else if (next_action < 100) {
        // Delete a file.
        string to_delete;
        if (!GetRandomFile(DELETE, &rand, &to_delete)) {
          continue;
        }
        TEST_CHECK_OK(cache_->DeleteFile(to_delete));
        metrics[BaseName(to_delete)]["delete"]++;
      }
    } while (!running_.WaitFor(MonoDelta::FromMilliseconds(1)));

    // Update the global metrics map.
    MergeNewMetrics(std::move(metrics));
  }

 protected:
  void NotifyThreads() { running_.CountDown(); }

  const MetricMap& metrics() const { return metrics_; }

 private:
  static constexpr const char* const kRWFPrefix = "rwf-";
  static constexpr const char* const kRAFPrefix = "raf-";

  enum GetMode {
    OPEN,
    DELETE
  };

  // Retrieve a random file name to be either opened or deleted. If deleting,
  // the file name is made inaccessible to future operations.
  bool GetRandomFile(GetMode mode, Random* rand, string* out) {
    std::lock_guard<simple_spinlock> l(lock_);
    if (available_files_.empty()) {
      return false;
    }

    // This is linear time, but it's simpler than managing multiple data
    // structures.
    auto it = available_files_.begin();
    std::advance(it, rand->Uniform(available_files_.size()));

    // It's unsafe to delete a file that is still being opened.
    if (mode == DELETE && it->second > 0) {
      return false;
    }

    *out = it->first;
    if (mode == OPEN) {
      it->second++;
    } else {
      available_files_.erase(it);
    }
    return true;
  }

  // Signal that a previously in-progress open has finished, allowing the file
  // in question to be deleted.
  void FinishedOpen(const string& opened) {
    std::lock_guard<simple_spinlock> l(lock_);
    int& openers = FindOrDie(available_files_, opened);
    openers--;
  }

  // Reads a random chunk of data from a random file in 'files'. On success,
  // writes to 'metrics'.
  template <class FileType>
  static Status ReadRandomChunk(const deque<shared_ptr<FileType>>& files,
                                MetricMap* metrics,
                                Random* rand) {
    if (files.empty()) {
      return Status::OK();
    }
    const shared_ptr<FileType>& file = files[rand->Uniform(files.size())];

    uint64_t file_size;
    RETURN_NOT_OK(file->Size(&file_size));
    uint64_t off = file_size > 0 ? rand->Uniform(file_size) : 0;
    size_t len = file_size > 0 ? rand->Uniform(file_size - off) : 0;
    unique_ptr<uint8_t[]> scratch(new uint8_t[len]);
    RETURN_NOT_OK(file->Read(off, Slice(scratch.get(), len)));

    (*metrics)[BaseName(file->filename())]["read"]++;
    return Status::OK();
  }

  // Writes a random chunk of data to a random file in 'files'. On success,
  // updates 'metrics'.
  static Status WriteRandomChunk(const deque<shared_ptr<RWFile>>& files,
                                 MetricMap* metrics,
                                 Random* rand) {
    if (files.empty()) {
      return Status::OK();
    }
    const shared_ptr<RWFile>& file = files[rand->Uniform(files.size())];

    uint64_t file_size;
    RETURN_NOT_OK(file->Size(&file_size));
    uint64_t off = file_size > 0 ? rand->Uniform(file_size) : 0;
    uint8_t buf[64];
    RETURN_NOT_OK(file->Write(off, GenerateRandomChunk(buf, sizeof(buf), rand)));
    (*metrics)[BaseName(file->filename())]["write"]++;
    return Status::OK();
  }

  static Slice GenerateRandomChunk(uint8_t* buffer, size_t max_length, Random* rand) {
    size_t len = rand->Uniform(max_length);
    len -= len % sizeof(uint32_t);
    for (int i = 0; i < (len / sizeof(uint32_t)); i += sizeof(uint32_t)) {
      reinterpret_cast<uint32_t*>(buffer)[i] = rand->Next32();
    }
    return Slice(buffer, len);
  }

  // Merge the metrics in 'new_metrics' into the global metric map.
  void MergeNewMetrics(MetricMap new_metrics) {
    std::lock_guard<simple_spinlock> l(lock_);
    for (const auto& file_action_pair : new_metrics) {
      for (const auto& action_count_pair : file_action_pair.second) {
        metrics_[file_action_pair.first][action_count_pair.first] += action_count_pair.second;
      }
    }
  }

  unique_ptr<FileCache> cache_;

  // Used to seed per-thread PRNGs.
  ThreadSafeRandom rand_;

  // Drops to zero when the test ends.
  CountDownLatch running_;

  // Protects 'available_files_' and 'metrics_'.
  simple_spinlock lock_;

  // Contains files produced by producer threads and ready for consumption by
  // consumer threads.
  //
  // Each entry is a file name and the number of in-progress openers. To delete
  // a file, there must be no openers.
  unordered_map<string, int> available_files_;

  // For each file name, tracks the count of consumer actions performed.
  //
  // Only updated at test end.
  MetricMap metrics_;
};

TEST_F(FileCacheStressTest, TestStress) {
  OverrideFlagForSlowTests("test_num_producer_threads", "2");
  OverrideFlagForSlowTests("test_num_consumer_threads", "8");
  OverrideFlagForSlowTests("test_duration_secs", "30");

  // Start the threads.
  PeriodicOpenFdChecker checker(
      env_,
      GetTestPath("*"),                 // only count within our test dir
      kTestMaxOpenFiles +               // cache capacity
      FLAGS_test_num_producer_threads + // files being written
      FLAGS_test_num_consumer_threads); // files being opened
  checker.Start();
  vector<thread> producers;
  producers.reserve(FLAGS_test_num_producer_threads);
  for (int i = 0; i < FLAGS_test_num_producer_threads; i++) {
    producers.emplace_back(&FileCacheStressTest::ProducerThread, this);
  }
  vector<thread> consumers;
  consumers.reserve(FLAGS_test_num_consumer_threads);
  for (int i = 0; i < FLAGS_test_num_consumer_threads; i++) {
    consumers.emplace_back(&FileCacheStressTest::ConsumerThread, this);
  }

  // Let the test run.
  SleepFor(MonoDelta::FromSeconds(FLAGS_test_duration_secs));

  // Stop the threads.
  NotifyThreads();
  checker.Stop();
  for (auto& p : producers) {
    p.join();
  }
  for (auto& c : consumers) {
    c.join();
  }

  // Log the metrics.
  unordered_map<string, int> action_counts;
  for (const auto& file_action_pair : metrics()) {
    for (const auto& action_count_pair : file_action_pair.second) {
      VLOG(2) << Substitute("$0: $1: $2",
                            file_action_pair.first,
                            action_count_pair.first,
                            action_count_pair.second);
      action_counts[action_count_pair.first] += action_count_pair.second;
    }
  }
  for (const auto& action_count_pair : action_counts) {
    LOG(INFO) << Substitute("$0: $1",
                            action_count_pair.first,
                            action_count_pair.second);
  }
}

} // namespace kudu
