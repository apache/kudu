// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdio.h>

#include "tablet/rowset_tree.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

using std::string;
using kudu::metadata::RowSetMetadata;

namespace kudu { namespace tablet {

class TestRowSetTree : public KuduTest {
};

// Mock implementation of RowSet which just aborts on every call.
class MockRowSet : public RowSet {
 public:
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present, ProbeStats* stats) const {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual Status MutateRow(txid_t txid,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           ProbeStats* stats,
                           MutationResultPB *result) {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual RowwiseIterator *NewRowIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual CompactionInput *NewCompactionInput(const Schema& projection,
                                              const MvccSnapshot &snap) const {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual Status CountRows(rowid_t *count) const {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual string ToString() const {
    LOG(FATAL) << "Unimplemented";
    return "";
  }
  virtual Status DebugDump(vector<string> *lines = NULL) {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual Status Delete() {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual uint64_t EstimateOnDiskSize() const {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  virtual boost::mutex *compact_flush_lock() {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual shared_ptr<RowSetMetadata> metadata() {
    LOG(FATAL) << "Unimplemented";
    return shared_ptr<RowSetMetadata>(reinterpret_cast<RowSetMetadata *>(NULL));
  }
  virtual Status AlterSchema(const Schema& schema) {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual const Schema &schema() const {
    LOG(FATAL) << "Unimplemented";
  }

  virtual size_t DeltaMemStoreSize() const {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  virtual size_t CountDeltaStores() const {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  virtual Status FlushDeltas() {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  virtual Status MinorCompactDeltaStores() {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
};

// Mock which implements GetBounds() with constant provided bonuds.
class MockDiskRowSet : public MockRowSet {
 public:
  MockDiskRowSet(string first_key, string last_key)
    : first_key_(first_key),
      last_key_(last_key) {
  }

  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const {
    *min_encoded_key = Slice(first_key_);
    *max_encoded_key = Slice(last_key_);
    return Status::OK();
  }

 private:
  const string first_key_;
  const string last_key_;
};

// Mock which acts like a MemRowSet and has no known bounds.
class MockMemRowSet : public MockRowSet {
 public:
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const {
    return Status::NotSupported("");
  }

 private:
  const string first_key_;
  const string last_key_;
};


TEST_F(TestRowSetTree, TestTree) {
  RowSetVector vec;
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("0", "5")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("3", "5")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("5", "9")));
  vec.push_back(shared_ptr<RowSet>(new MockMemRowSet()));

  RowSetTree tree;
  ASSERT_STATUS_OK(tree.Reset(vec));

  // "2" overlaps 0-5 and the MemRowSet.
  vector<RowSet *> out;
  tree.FindRowSetsWithKeyInRange("2", &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);

  // "4" overlaps 0-5, 3-5, and the MemRowSet
  out.clear();
  tree.FindRowSetsWithKeyInRange("4", &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval (2,4) overlaps 0-5, 3-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("3", "4", &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval (0,2) overlaps 0-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("0", "2", &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);

  // interval (5,7) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("5", "7", &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);
}

TEST_F(TestRowSetTree, TestPerformance) {
  const int kNumRowSets = 200;
  const int kNumQueries = AllowSlowTests() ? 1000000 : 10000;
  SeedRandom();

  // Create a bunch of rowsets, each of which spans about 10% of the "row space".
  // The row space here is 4-digit 0-padded numbers.
  RowSetVector vec;
  for (int i = 0; i < kNumRowSets; i++) {
    int min = rand() % 9000;
    int max = min + 1000;

    vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(
                                       StringPrintf("%04d", min),
                                       StringPrintf("%04d", max))));
  }

  RowSetTree tree;
  ASSERT_STATUS_OK(tree.Reset(vec));

  LOG_TIMING(INFO, StringPrintf("Querying rowset %d times", kNumQueries)) {
    vector<RowSet *> out;
    char buf[32];
    for (int i = 0; i < kNumQueries; i++) {
      out.clear();
      int query = rand() % 10000;
      snprintf(buf, arraysize(buf), "%04d", query);
      tree.FindRowSetsWithKeyInRange(Slice(buf, 4), &out);
    }
  }
}

} // namespace tablet
} // namespace kudu
