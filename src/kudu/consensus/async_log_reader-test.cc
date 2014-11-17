// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/async_log_reader.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/opid_util.h"

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using consensus::MinimumOpId;
using consensus::OpId;

class AsyncLogReaderTest : public LogTestBase {
 public:

  // Appends a sequence of operations that spans multiple segments.
  // 'op_id' must be initialized and corresponds to the id of the
  // first appended operation. The index of 'op_id' is then incremented
  // by one for each appended operation so, when this method is done
  // 'op_id' will be id of the last appended operation.
  Status AppendMultiSegmentSequence(int num_total_segments,
                                    int num_ops_per_segment,
                                    OpId* op_id) {
    CHECK(op_id->IsInitialized());
    for (int i = 0; i < num_total_segments - 1; i++) {
      RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
      RETURN_NOT_OK(RollLog());
    }

    RETURN_NOT_OK(AppendNoOps(op_id, num_ops_per_segment));
    return Status::OK();
  }

  void ReadPerformedCallback(int64_t starting_at,
                             const Status& status,
                             const vector<ReplicateMsg*>& replicates) {
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      CHECK_EQ(expected_status_.CodeAsString(), status.CodeAsString())
        << "Expected status: " << expected_status_.ToString()
        << ". But got status: " << status.ToString();
      CHECK_EQ(expected_op_count_, replicates.size());
    }

    BOOST_FOREACH(ReplicateMsg* msg, replicates) {
      delete msg;
    }
  }

  Status expected_status_;
  int expected_op_count_;
  mutable simple_spinlock lock_;
};

// Tests that the reader can read an existing range.
TEST_F(AsyncLogReaderTest, TestReadExistingRange) {
  BuildLog();

  // Append 1.1 through 1.50 across several segments
  OpId first = consensus::MakeOpId(1, 1);
  AppendMultiSegmentSequence(5, 10, &first);

  AsyncLogReader reader(log_->GetLogReader());
  ASSERT_FALSE(reader.IsReading());


  // Enqueue a read that spans several segments, starting at and including 5
  // and ending and including 50. This should return a total of 46 ops.
  // Trigger the read under the lock, this will make sure we can
  // make a couple of assertions before the read is done. This is
  // also similar to how the async reader is used from the consensus
  // queue.
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    expected_status_ = Status::OK();
    expected_op_count_ = 46;
    ASSERT_OK(reader.EnqueueAsyncRead(5, 50,
                                      Bind(&AsyncLogReaderTest::ReadPerformedCallback,
                                           Unretained(this))));
    ASSERT_TRUE(reader.IsReading());
  }

  reader.Shutdown();
}

// Tests that the reader returns Status::NotFound() because the start operation
// could not be found.
TEST_F(AsyncLogReaderTest, TestReadWithNotFoundStart) {
  BuildLog();
  OpId first = consensus::MakeOpId(1, 5);
  AppendMultiSegmentSequence(5, 10, &first);

  AsyncLogReader reader(log_->GetLogReader());
  ASSERT_FALSE(reader.IsReading());


  // Enqueue a read that spans several segments, starting after (but not
  // including) 'starting_after' and ending (and including) in 'last'.
  // Trigger the read under the lock, this will make sure we can
  // make a couple of assertions before the read is done. This is
  // also similar to how the async reader is used from the consensus
  // queue.
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    expected_status_ = Status::NotFound("");
    expected_op_count_ = 0;
    ASSERT_OK(reader.EnqueueAsyncRead(1, 50,
                                      Bind(&AsyncLogReaderTest::ReadPerformedCallback,
                                           Unretained(this))));
  }

  reader.Shutdown();
}

}  // namespace log
}  // namespace kudu

