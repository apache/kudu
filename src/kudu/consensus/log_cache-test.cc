// Copyright (c) 2014, Cloudera, inc.

#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

using std::tr1::shared_ptr;

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(log_cache_size_soft_limit_mb);
DECLARE_int32(log_cache_size_hard_limit_mb);
DECLARE_int32(global_log_cache_size_soft_limit_mb);
DECLARE_int32(global_log_cache_size_hard_limit_mb);

namespace kudu {
namespace consensus {

static const char* kPeerUuid = "leader";
static const char* kTestTablet = "test-tablet";

class LogCacheTest : public KuduTest {
 public:
  LogCacheTest()
    : schema_(GetSimpleTestSchema()),
      metric_context_(&metric_registry_, "LogCacheTest") {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
    CHECK_OK(log::Log::Open(log::LogOptions(),
                            fs_manager_.get(),
                            kTestTablet,
                            schema_,
                            NULL,
                            &log_));

    CloseAndReopenCache(MinimumOpId(), "TestMemTracker");
  }

  virtual void TearDown() OVERRIDE {
    cache_->Flush();
  }

  void CloseAndReopenCache(const OpId& preceding_id,
                           const string& mem_tracker_name) {
    if (cache_) {
      cache_->Flush();
    }
    cache_.reset(new LogCache(metric_context_,
                              log_.get(),
                              kPeerUuid,
                              kTestTablet,
                              mem_tracker_name));
    cache_->Init(preceding_id);
  }

 protected:
  static void FatalOnError(const Status& s) {
    CHECK_OK(s);
  }

  bool AppendReplicateMessagesToCache(
    int first,
    int count,
    int payload_size = 0) {

    for (int i = first; i < first + count; i++) {
      int term = i / 7;
      int index = i;
      vector<const ReplicateMsg*> msgs;
      msgs.push_back(CreateDummyReplicate(term, index, payload_size).release());
      if (!cache_->AppendOperations(msgs, Bind(&FatalOnError))) {
        STLDeleteElements(&msgs);
        return false;
      }
    }
    return true;
  }

  Status RetryReadWhileIncomplete(int64_t after_op_index,
                                  int max_size_bytes,
                                  std::vector<const ReplicateMsg*>* messages,
                                  OpId* preceding_op) {
    Status s;
    int sleep_for = 0;
    do {
      usleep(sleep_for);
      sleep_for += 1000;
      s = cache_->ReadOps(after_op_index, max_size_bytes, messages, preceding_op);
    } while (s.IsIncomplete());
    return s;
  }

  void WaitForCachedOpCount(int expected) {
    int count = -1;
    for (int i = 0; i < 1000; i++) {
      count = cache_->metrics_.log_cache_total_num_ops->value();
      if (count == expected) {
        break;
      }
      usleep(10000);
    }
    ASSERT_EQ(expected, count);
  }

  const Schema schema_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<log::Log> log_;
  gscoped_ptr<LogCache> cache_;
};


TEST_F(LogCacheTest, TestAppendAndGetMessages) {
  ASSERT_EQ(0, cache_->metrics_.log_cache_total_num_ops->value());
  ASSERT_EQ(0, cache_->metrics_.log_cache_size_bytes->value());
  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 100));
  ASSERT_EQ(100, cache_->metrics_.log_cache_total_num_ops->value());
  ASSERT_GE(cache_->metrics_.log_cache_size_bytes->value(), 500);

  vector<const ReplicateMsg*> messages;
  OpId preceding;
  ASSERT_OK(cache_->ReadOps(0, 8 * 1024 * 1024, &messages, &preceding));
  EXPECT_EQ(100, messages.size());
  EXPECT_EQ("0.0", OpIdToString(preceding));

  // Get starting in the middle of the cache.
  messages.clear();
  ASSERT_OK(cache_->ReadOps(70, 8 * 1024 * 1024, &messages, &preceding));
  EXPECT_EQ(30, messages.size());
  EXPECT_EQ("10.70", OpIdToString(preceding));
  EXPECT_EQ("10.71", OpIdToString(messages[0]->id()));

  // Get at the end of the cache
  messages.clear();
  ASSERT_OK(cache_->ReadOps(100, 8 * 1024 * 1024, &messages, &preceding));
  EXPECT_EQ(0, messages.size());
  EXPECT_EQ("14.100", OpIdToString(preceding));

  // Evict some and wait for the eviction to take effect.
  // (it may not be instant, since we didn't flush the log)
  cache_->SetPinnedOp(50);
  ASSERT_NO_FATAL_FAILURE(WaitForCachedOpCount(51));

  // Can still read data that was evicted, since it got written through.
  messages.clear();
  cache_->SetPinnedOp(20);
  RetryReadWhileIncomplete(20, 8 * 1024 * 1024, &messages, &preceding);
  EXPECT_EQ(80, messages.size());
  EXPECT_EQ("2.20", OpIdToString(preceding));
  EXPECT_EQ("3.21", OpIdToString(messages[0]->id()));
}


// Ensure that the cache always yields at least one message,
// even if that message is larger than the batch size. This ensures
// that we don't get "stuck" in the case that a large message enters
// the cache.
TEST_F(LogCacheTest, TestAlwaysYieldsAtLeastOneMessage) {
  // generate a 2MB dummy payload
  const int kPayloadSize = 2 * 1024 * 1024;

  // Append several large ops to the cache
  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 4, kPayloadSize));

  // We should get one of them, even though we only ask for 100 bytes
  vector<const ReplicateMsg*> messages;
  OpId preceding;
  ASSERT_OK(cache_->ReadOps(0, 100, &messages, &preceding));
  ASSERT_EQ(1, messages.size());
}

// Test that, if the hard limit has been reached, requests are refused.
TEST_F(LogCacheTest, TestCacheRefusesRequestWhenFilled) {
  FLAGS_log_cache_size_soft_limit_mb = 0;
  FLAGS_log_cache_size_hard_limit_mb = 1;

  CloseAndReopenCache(MinimumOpId(), "TestCacheRefusesRequestWhenFilled");

  // generate a 128Kb dummy payload
  const int kPayloadSize = 128 * 1024;

  // append 8 messages to the cache, these should be allowed
  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 7, kPayloadSize));

  // should fail because the cache is full
  ASSERT_FALSE(AppendReplicateMessagesToCache(8, 1, kPayloadSize));


  // Move the pin past the first two ops
  cache_->SetPinnedOp(2);
  log_->WaitUntilAllFlushed();

  // And try again -- should now succeed
  ASSERT_TRUE(AppendReplicateMessagesToCache(8, 1, kPayloadSize));
}

// Tests that the cache returns Status::NotFound() if queried for messages after an
// index that is higher than it's latest, returns an empty set of messages when queried for
// the the last index and returns all messages when queried for MinimumOpId().
TEST_F(LogCacheTest, TestCacheEdgeCases) {
  // Append 1 message to the cache
  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1));

  std::vector<const ReplicateMsg*> messages;
  OpId preceding;

  // Test when the searched index is MinimumOpId().index().
  ASSERT_OK(cache_->ReadOps(0, 100, &messages, &preceding));
  ASSERT_EQ(1, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 0), preceding);

  messages.clear();
  preceding.Clear();
  // Test when 'after_op_index' is the last index in the cache.
  ASSERT_OK(cache_->ReadOps(1, 100, &messages, &preceding));
  ASSERT_EQ(0, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 1), preceding);

  messages.clear();
  preceding.Clear();
  // Now test the case when 'after_op_index' is after the last index
  // in the cache.
  Status s = cache_->ReadOps(2, 100, &messages, &preceding);
  ASSERT_TRUE(s.IsNotFound()) << "unexpected status: " << s.ToString();
  ASSERT_EQ(0, messages.size());
  ASSERT_FALSE(preceding.IsInitialized());

  messages.clear();
  preceding.Clear();

  // Evict entries from the cache, and ensure that we can still read
  // entries at the beginning of the log.
  cache_->SetPinnedOp(50);
  cache_->SetPinnedOp(0); // re-pin at 0 to allow the read
  ASSERT_OK(RetryReadWhileIncomplete(0, 100, &messages, &preceding));
  ASSERT_EQ(1, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 0), preceding);
}


TEST_F(LogCacheTest, TestHardAndSoftLimit) {
  FLAGS_log_cache_size_soft_limit_mb = 1;
  FLAGS_log_cache_size_hard_limit_mb = 2;

  CloseAndReopenCache(MinimumOpId(), "TestHardAndSoftLimit");

  const int kPayloadSize = 768 * 1024;
  // Soft limit should not be violated.
  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));

  int size_with_one_msg = cache_->BytesUsed();
  ASSERT_LT(size_with_one_msg, 1 * 1024 * 1024);

  // Violating a soft limit, but not a hard limit should still allow
  // the operation to be added.
  ASSERT_TRUE(AppendReplicateMessagesToCache(2, 1, kPayloadSize));

  // Since the first operation is not yet done, we can't trim.
  int size_with_two_msgs = cache_->BytesUsed();
  ASSERT_GE(size_with_two_msgs, 2 * 768 * 1024);
  ASSERT_LT(size_with_two_msgs, 2 * 1024 * 1024);

  cache_->SetPinnedOp(2);

  log_->WaitUntilAllFlushed();

  // Verify that we have trimmed by appending a message that would
  // otherwise be rejected, since the cache max size limit is 2MB.
  ASSERT_TRUE(AppendReplicateMessagesToCache(3, 1, kPayloadSize));

  log_->WaitUntilAllFlushed();

  // The cache should be trimmed down to two messages.
  ASSERT_EQ(size_with_two_msgs, cache_->BytesUsed());

  // Ack indexes 2 and 3
  cache_->SetPinnedOp(4);
  ASSERT_TRUE(AppendReplicateMessagesToCache(4, 1, kPayloadSize));

  // Verify that the cache is trimmed down to just one message.
  ASSERT_EQ(size_with_one_msg, cache_->BytesUsed());

  cache_->SetPinnedOp(5);

  // Add a small message such that soft limit is not violated.
  const int kSmallPayloadSize = 128 * 1024;
  ASSERT_TRUE(AppendReplicateMessagesToCache(5, 1, kSmallPayloadSize));

  // Verify that the cache is not trimmed.
  ASSERT_GT(cache_->BytesUsed(), 0);
}

TEST_F(LogCacheTest, TestGlobalHardLimit) {
  FLAGS_log_cache_size_soft_limit_mb = 1;
  FLAGS_global_log_cache_size_soft_limit_mb = 4;

  FLAGS_log_cache_size_hard_limit_mb = 2;
  FLAGS_global_log_cache_size_hard_limit_mb = 5;

  const string kParentTrackerId = "TestGlobalHardLimit";
  shared_ptr<MemTracker> parent_tracker = MemTracker::CreateTracker(
    FLAGS_log_cache_size_soft_limit_mb * 1024 * 1024,
    kParentTrackerId,
    NULL);

  ASSERT_TRUE(parent_tracker.get() != NULL);

  // Exceed the global hard limit.
  parent_tracker->Consume(6 * 1024 * 1024);

  CloseAndReopenCache(MinimumOpId(), kParentTrackerId);

  const int kPayloadSize = 768 * 1024;

  // Should fail because the cache has exceeded hard limit.
  ASSERT_FALSE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));

  // Now release the memory.
  parent_tracker->Release(2 * 1024 * 1024);

  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));
}

TEST_F(LogCacheTest, TestEvictWhenGlobalSoftLimitExceeded) {
  FLAGS_log_cache_size_soft_limit_mb = 1;
  FLAGS_global_log_cache_size_soft_limit_mb = 4;

  FLAGS_log_cache_size_hard_limit_mb = 2;
  FLAGS_global_log_cache_size_hard_limit_mb = 5;

  const string kParentTrackerId = "TestGlobalSoftLimit";

  shared_ptr<MemTracker> parent_tracker = MemTracker::CreateTracker(
     FLAGS_log_cache_size_soft_limit_mb * 1024 * 1024,
     kParentTrackerId,
     NULL);

 ASSERT_TRUE(parent_tracker.get() != NULL);

 // Exceed the global soft limit.
 parent_tracker->Consume(4 * 1024 * 1024);
 parent_tracker->Consume(1024);

 CloseAndReopenCache(MinimumOpId(), kParentTrackerId);

 const int kPayloadSize = 768 * 1024;
 ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));

 int size_with_one_msg = cache_->BytesUsed();

 cache_->SetPinnedOp(2);

 log_->WaitUntilAllFlushed();

 // If this goes through, that means the queue has been trimmed, otherwise
 // the hard limit would be violated and false would be returned.
 ASSERT_TRUE(AppendReplicateMessagesToCache(2, 1, kPayloadSize));

 // Verify that there is only one message in the queue.
 ASSERT_EQ(size_with_one_msg, cache_->BytesUsed());
}

// Test that the log cache properly replaces messages when an index
// is reused. This is a regression test for a bug where the memtracker's
// consumption wasn't properly managed when messages were replaced.
TEST_F(LogCacheTest, TestReplaceMessages) {
  const int kPayloadSize = 128 * 1024;
  shared_ptr<MemTracker> tracker = cache_->tracker_;;
  ASSERT_EQ(0, tracker->consumption());

  ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));
  int size_with_one_msg = tracker->consumption();

  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(AppendReplicateMessagesToCache(1, 1, kPayloadSize));
  }

  EXPECT_EQ(size_with_one_msg, tracker->consumption());
  EXPECT_EQ(Substitute("Preceding Op: 0.0, Pinned index: 0, LogCacheStats(num_ops=1, bytes=$0)",
                       size_with_one_msg),
            cache_->ToString());
}

} // namespace consensus
} // namespace kudu
