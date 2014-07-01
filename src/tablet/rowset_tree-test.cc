// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <tr1/unordered_set>
#include <stdio.h>

#include "gutil/map-util.h"
#include "server/metadata.h"
#include "tablet/rowset_tree.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

using std::string;
using std::tr1::unordered_set;
using kudu::metadata::RowSetMetadata;

namespace kudu { namespace tablet {

class TestRowSetTree : public KuduTest {
};

// Mock implementation of RowSet which just aborts on every call.
class MockRowSet : public RowSet {
 public:
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                                 ProbeStats* stats) const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           const consensus::OpId& op_id_,
                           ProbeStats* stats,
                           OperationResultPB *result) OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual RowwiseIterator *NewRowIterator(const Schema* projection,
                                          const MvccSnapshot &snap) const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual CompactionInput *NewCompactionInput(const Schema* projection,
                                              const MvccSnapshot &snap) const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual Status CountRows(rowid_t *count) const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual string ToString() const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return "";
  }
  virtual Status DebugDump(vector<string> *lines = NULL) OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual Status Delete() {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual uint64_t EstimateOnDiskSize() const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }
  virtual boost::mutex *compact_flush_lock() OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return NULL;
  }
  virtual shared_ptr<RowSetMetadata> metadata() OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return shared_ptr<RowSetMetadata>(reinterpret_cast<RowSetMetadata *>(NULL));
  }
  virtual Status AlterSchema(const Schema& schema) OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }
  virtual const Schema &schema() const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
  }

  virtual size_t DeltaMemStoreSize() const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  virtual size_t CountDeltaStores() const OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

  virtual Status FlushDeltas() OVERRIDE {
    LOG(FATAL) << "Unimplemented";
    return Status::OK();
  }

  virtual Status MinorCompactDeltaStores() OVERRIDE {
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
                           Slice *max_encoded_key) const OVERRIDE {
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
                           Slice *max_encoded_key) const OVERRIDE {
    return Status::NotSupported("");
  }

 private:
  const string first_key_;
  const string last_key_;
};

namespace {

// Generates random rowsets with keys between 0 and 10000
static RowSetVector GenerateRandomRowSets(int num_sets) {
  RowSetVector vec;
  for (int i = 0; i < num_sets; i++) {
    int min = rand() % 9000;
    int max = min + 1000;

    vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", min),
                                                        StringPrintf("%04d", max))));
  }
  return vec;
}

} // anonymous namespace

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
  RowSetVector vec = GenerateRandomRowSets(kNumRowSets);

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

TEST_F(TestRowSetTree, TestEndpointsConsistency) {
  const int kNumRowSets = 1000;
  RowSetVector vec = GenerateRandomRowSets(kNumRowSets);
  // Add pathological one-key rows
  for (int i = 0; i < 10; ++i) {
    vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", 11000),
                                                        StringPrintf("%04d", 11000))));
  }
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", 12000),
                                                      StringPrintf("%04d", 12000))));
  // Make tree
  RowSetTree tree;
  tree.Reset(vec);
  // Keep track of "currently open" intervals defined by the endpoints
  unordered_set<RowSet*> open;
  // Keep track of all rowsets that have been visited
  unordered_set<RowSet*> visited;

  Slice prev;
  BOOST_FOREACH(const RowSetTree::RSEndpoint& rse, tree.key_endpoints()) {
    RowSet* rs = rse.rowset_;
    enum RowSetTree::EndpointType ept = rse.endpoint_;
    const Slice& slice = rse.slice_;

    ASSERT_TRUE(rs != NULL) << "RowSetTree has an endpoint with no rowset";
    ASSERT_TRUE(!slice.empty()) << "RowSetTree has an endpoint with no key";

    if (!prev.empty()) {
      ASSERT_LE(prev.compare(slice), 0);
    }

    Slice min, max;
    ASSERT_OK(rs->GetBounds(&min, &max));
    if (ept == RowSetTree::START) {
      ASSERT_EQ(min.data(), slice.data());
      ASSERT_EQ(min.size(), slice.size());
      ASSERT_TRUE(InsertIfNotPresent(&open, rs));
      ASSERT_TRUE(InsertIfNotPresent(&visited, rs));
    } else if (ept == RowSetTree::STOP) {
      ASSERT_EQ(max.data(), slice.data());
      ASSERT_EQ(max.size(), slice.size());
      ASSERT_TRUE(open.erase(rs) == 1);
    } else {
      FAIL() << "No such endpoint type exists";
    }
  }
}

} // namespace tablet
} // namespace kudu
