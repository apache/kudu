// Copyright (c) 2012, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "concurrent_btree.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {
namespace btree {

using boost::scoped_ptr;

// Ensure that the template magic to make the nodes sized
// as we expect is working.
TEST(TestCBTree, TestNodeSizes) {

  LeafNode<BTreeTraits> lnode(false);
  ASSERT_EQ(lnode.node_size(), sizeof(lnode));

  InternalNode<BTreeTraits> inode(Slice("split"), &lnode, &lnode);
  ASSERT_EQ(inode.node_size(), sizeof(inode));

}

TEST(TestCBTree, TestLeafNode) {
  LeafNode<BTreeTraits> lnode(false);

  // Must lock the node even in the single threaded test
  // to avoid firing the debug assertions.
  lnode.Lock();
  lnode.SetInserting();

  Slice k1("key1");
  Slice v1("val1");
  ASSERT_EQ(INSERT_SUCCESS,
            lnode.Insert(k1, v1));
  ASSERT_EQ(INSERT_DUPLICATE,
            lnode.Insert(k1, v1));

  // Insert another entry after first
  Slice k2("key2");
  Slice v2("val2");
  ASSERT_EQ(INSERT_SUCCESS, lnode.Insert(k2, v2));
  ASSERT_EQ(INSERT_DUPLICATE, lnode.Insert(k2, v2));

  // Another entry before first
  Slice k0("key0");
  Slice v0("val0");
  ASSERT_EQ(INSERT_SUCCESS, lnode.Insert(k0, v0));
  ASSERT_EQ(INSERT_DUPLICATE, lnode.Insert(k0, v0));

  // Another entry in the middle
  Slice k15("key1.5");
  Slice v15("val1.5");
  ASSERT_EQ(INSERT_SUCCESS, lnode.Insert(k15, v15));
  ASSERT_EQ(INSERT_DUPLICATE, lnode.Insert(k15, v15));
  ASSERT_EQ("[key0=val0], [key1=val1], [key1.5=val1.5], [key2=val2]",
            lnode.ToString());

  // Add entries until it is full
  int i;
  bool full = false;
  for (i = 0; i < 1000 && full; i++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "filler_key_%d", i);
    switch (lnode.Insert(Slice(buf), Slice("data"))) {
      case INSERT_SUCCESS:
        continue;
      case INSERT_DUPLICATE:
        FAIL() << "Unexpected INSERT_DUPLICATE for " << buf;
        break;
      case INSERT_FULL:
        full = true;
        break;
      default:
        FAIL() << "unexpected result";
    }
  }
  ASSERT_LT(i, 1000) << "should have filled up node before 1000 entries";

  lnode.Unlock();
}

// Setup the tree to fanout quicker, so we test internal node
// splitting, etc.
struct SmallFanoutTraits : public BTreeTraits{
  static const size_t internal_node_size = 256;
  static const size_t fanout = 4;

  static const size_t leaf_node_size = 256;

  // TODO: this should probably be dynamic, since we'd
  // know the size of the value for fixed size tables
  static const size_t leaf_max_entries = 4;
  
};

void MakeKey(char *kbuf, size_t len, int i) {
  snprintf(kbuf, len, "key_%d%d", i % 10, i / 10);
}

template<class T>
void VerifyEntry(CBTree<T> *tree, int i) {
  char kbuf[64];
  char vbuf[64];
  char vbuf_out[64];

  MakeKey(kbuf, sizeof(kbuf), i);
  snprintf(vbuf, sizeof(vbuf), "val_%d", i);

  size_t len = sizeof(vbuf_out);
  ASSERT_EQ(CBTree<T>::GET_SUCCESS,
            tree->GetCopy(Slice(kbuf), vbuf_out, &len))
    << "Failed to verify entry " << kbuf;
  ASSERT_EQ(string(vbuf, len), string(vbuf_out, len));
}


template<class T>
void InsertRange(CBTree<T> *tree,
                 int start_idx,
                 int end_idx) {
  char kbuf[64];
  char vbuf[64];
  for (int i = start_idx; i < end_idx; i++) {
    MakeKey(kbuf, sizeof(kbuf), i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    if (!tree->Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Failed insert at iteration " << i;
    }

    /*
    int to_verify = start_idx + (rand() % (i - start_idx + 1));
    CHECK_LE(to_verify, i);
    VerifyEntry(tree, to_verify);
    */
  }
}

template<class T>
void VerifyRange(CBTree<T> *tree,
                 int start_idx,
                 int end_idx) {
  char kbuf[64];
  char vbuf[64];
  char vbuf_out[64];
  for (int i = start_idx; i < end_idx; i++) {
    MakeKey(kbuf, sizeof(kbuf), i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);

    size_t len = sizeof(vbuf_out);
    ASSERT_EQ(CBTree<T>::GET_SUCCESS,
              tree->GetCopy(Slice(kbuf), vbuf_out, &len))
      << "Failed verification for key  " << kbuf;
    ASSERT_EQ(string(vbuf, len), string(vbuf_out, len));
  }
}


// Function which inserts a range of keys formatted key_<N>
// into the given tree, then verifies that they are all
// inserted properly
template<class T>
void InsertAndVerify(boost::barrier *go_barrier,
                     boost::barrier *done_barrier,
                     scoped_ptr<CBTree<T> > *tree,
                     int start_idx,
                     int end_idx) {
  while (true) {
    go_barrier->wait();

    if (tree->get() == NULL) return;

    InsertRange(tree->get(), start_idx, end_idx);
    VerifyRange(tree->get(), start_idx, end_idx);

    done_barrier->wait();
  }
}


TEST(TestCBTree, TestInsertAndVerify) {
  CBTree<SmallFanoutTraits> t;
  char kbuf[64];
  char vbuf[64];
  char vbuf_out[64];

  int n_keys = 10000;

  for (int i = 0; i < n_keys; i++) {
    snprintf(kbuf, sizeof(kbuf), "key_%d", i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    if (!t.Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Failed insert at iteration " << i;
    }
  }


  for (int i = 0; i < n_keys; i++) {
    snprintf(kbuf, sizeof(kbuf), "key_%d", i);

    // Try to insert with a different value, to ensure that on failure
    // it doesn't accidentally replace the old value anyway.
    snprintf(vbuf, sizeof(vbuf), "xxx_%d", i);
    if (t.Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Allowed duplicate insert at iteration " << i;
    }

    // Do a Get() and check that the real value is still accessible.
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    size_t len = sizeof(vbuf_out);
    ASSERT_EQ(CBTree<SmallFanoutTraits>::GET_SUCCESS,
              t.GetCopy(Slice(kbuf), vbuf_out, &len));
    ASSERT_EQ(string(vbuf, len), string(vbuf_out, len));
  }
}

template<class TREE, class COLLECTION>
static void InsertRandomKeys(TREE *t, int n_keys,
                             COLLECTION *inserted) {
  char kbuf[64];
  char vbuf[64];
  int i = 0;
  while (inserted->size() < n_keys) {
    int key = rand();
    memcpy(kbuf, &key, sizeof(key));
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    t->Insert(Slice(kbuf, sizeof(key)), Slice(vbuf));
    inserted->insert(key);
    i++;
  }
}

// Similar to above, but inserts in random order
TEST(TestCBTree, TestInsertAndVerifyRandom) {
  CBTree<SmallFanoutTraits> t;
  char kbuf[64];
  char vbuf_out[64];

  int n_keys = 100000;
  std::unordered_set<int> inserted;
  inserted.reserve(n_keys);

  InsertRandomKeys(&t, n_keys, &inserted);


  BOOST_FOREACH(int key, inserted) {
    memcpy(kbuf, &key, sizeof(key));

    // Do a Get() and check that the real value is still accessible.
    size_t len = sizeof(vbuf_out);
    ASSERT_EQ(CBTree<SmallFanoutTraits>::GET_SUCCESS,
              t.GetCopy(Slice(kbuf, sizeof(key)), vbuf_out, &len));
  }
}

void LockCycleThread(AtomicVersion *v, int count_split, int count_insert) {
  int i = 0;
  while (count_split > 0 || count_insert > 0) {
    i++;
    VersionField::Lock(v);
    if (i % 2 && count_split > 0) {
      VersionField::SetSplitting(v);
      count_split--;
    } else {
      VersionField::SetInserting(v);
      count_insert--;
    }
    VersionField::Unlock(v);
  }
}

TEST(TestCBTree, TestVersionLockSimple) {
  AtomicVersion v = 0;
  VersionField::Lock(&v);
  ASSERT_EQ(1L << 63, v);
  VersionField::Unlock(&v);
  ASSERT_EQ(0, v);

  VersionField::Lock(&v);
  VersionField::SetSplitting(&v);
  VersionField::Unlock(&v);

  ASSERT_EQ(0, VersionField::GetVInsert(v));
  ASSERT_EQ(1, VersionField::GetVSplit(v));

  VersionField::Lock(&v);
  VersionField::SetInserting(&v);
  VersionField::Unlock(&v);
  ASSERT_EQ(1, VersionField::GetVInsert(v));
  ASSERT_EQ(1, VersionField::GetVSplit(v));

}

TEST(TestCBTree, TestVersionLockConcurrent) {
  boost::ptr_vector<boost::thread> threads;
  int num_threads = 4;
  int split_per_thread = 2348;
  int insert_per_thread = 8327;

  AtomicVersion v = 0;

  for (int i = 0; i < num_threads; i++) {
    threads.push_back(new boost::thread(
                        LockCycleThread, &v, split_per_thread, insert_per_thread));
  }

  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }


  ASSERT_EQ(split_per_thread * num_threads,
            VersionField::GetVSplit(v));
  ASSERT_EQ(insert_per_thread * num_threads,
            VersionField::GetVInsert(v));
}

TEST(TestCBTree, TestConcurrentInsert) {
  scoped_ptr<CBTree<SmallFanoutTraits> > tree;
  
    int num_threads = 16;
    int ins_per_thread = 30;
  
    boost::ptr_vector<boost::thread> threads;
    boost::barrier go_barrier(num_threads + 1);
    boost::barrier done_barrier(num_threads + 1);


    for (int i = 0; i < num_threads; i++) {
      threads.push_back(new boost::thread(
                          InsertAndVerify<SmallFanoutTraits>,
                          &go_barrier,
                          &done_barrier,
                          &tree,
                          ins_per_thread * i,
                          ins_per_thread * (i + 1)));
    }


  // Rather than running one long trial, better to run
  // a bunch of short trials, so that the threads contend a lot
  // more on a smaller tree. As the tree gets larger, contention
  // on areas of the key space diminishes.

  for (int trial = 0; trial < 600; trial++) {
    tree.reset(new CBTree<SmallFanoutTraits>());
    go_barrier.wait();

    done_barrier.wait();

    if (::testing::Test::HasFatalFailure()) {
      tree->DebugPrint();
      return;
    }
  }

  tree.reset(NULL);
  go_barrier.wait();

  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }
}

TEST(TestCBTree, TestIterator) {
  CBTree<SmallFanoutTraits> t;

  int n_keys = 100000;
  std::unordered_set<int> inserted;
  inserted.reserve(n_keys);
  InsertRandomKeys(&t, n_keys, &inserted);

  // now iterate through, making sure we saw all
  // the keys that were inserted
  LOG_TIMING(INFO, "Iterating") {
    scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
      t.NewIterator());
    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice(""), &exact));
    int count = 0;
    while (iter->IsValid()) {
      Slice k, v;
      iter->GetCurrentEntry(&k, &v);

      int k_int;
      CHECK_EQ(sizeof(k_int), k.size());
      memcpy(&k_int, k.data(), k.size());

      bool removed = inserted.erase(k_int);
      if (!removed) {
        FAIL() << "Iterator saw entry " << k_int << " but not inserted";
      }
      count++;
      iter->Next();
    }

    ASSERT_EQ(n_keys, count);
    ASSERT_EQ(0, inserted.size()) << "Some entries were not seen by iterator";
  }
}

TEST(TestCBTree, TestIteratorSeekOnEmptyTree) {
  CBTree<SmallFanoutTraits> t;

  scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
    t.NewIterator());
  bool exact = true;
  ASSERT_FALSE(iter->SeekAtOrAfter(Slice(""), &exact));
  ASSERT_FALSE(exact);
  ASSERT_FALSE(iter->IsValid());
}

// Test seeking to exactly the first and last key, as well
// as the boundary conditions (before first and after last)
TEST(TestCBTree, TestIteratorSeekConditions) {
  CBTree<SmallFanoutTraits> t;

  ASSERT_TRUE(t.Insert(Slice("key1"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key2"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key3"), Slice("val")));

  // Seek to before first key should successfully reach first key
  {
    scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key0"), &exact));
    ASSERT_FALSE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key1", k.ToString());
  }

  // Seek to exactly first key should successfully reach first key
  // and set exact = true
  {
    scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key1"), &exact));
    ASSERT_TRUE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key1", k.ToString());
  }

  // Seek to exactly last key should successfully reach last key
  // and set exact = true
  {
    scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key3"), &exact));
    ASSERT_TRUE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key3", k.ToString());
    ASSERT_FALSE(iter->Next());
  }

  // Seek to after last key should fail.
  {
    scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter(
      t.NewIterator());

    bool exact;
    ASSERT_FALSE(iter->SeekAtOrAfter(Slice("key4"), &exact));
    ASSERT_FALSE(exact);
    ASSERT_FALSE(iter->IsValid());
  }
}

template<class T>
static void ScanThread(boost::barrier *go_barrier,
                       boost::barrier *done_barrier,
                       scoped_ptr<CBTree<T> > *tree) {
  while (true) {
    go_barrier->wait();
    if (tree->get() == NULL) return;

    int prev_count = 0;
    int count = 0;
    do {
      prev_count = count;
      count = 0;

      faststring prev_key;

      scoped_ptr<CBTreeIterator<SmallFanoutTraits> > iter((*tree)->NewIterator());
      bool exact;
      iter->SeekAtOrAfter(Slice(""), &exact);
      while (iter->IsValid()) {
        count++;
        Slice k, v;
        iter->GetCurrentEntry(&k, &v);

        if (k.compare(Slice(prev_key)) <= 0) {
          FAIL() << "prev key " << Slice(prev_key).ToString() <<
            " wasn't less than cur key " << k.ToString();
        }
        prev_key.assign_copy(k.data(), k.size());

        iter->Next();
      }
      ASSERT_GE(count, prev_count);
    } while (count != prev_count || count == 0);

    done_barrier->wait();
  }
}


TEST(TestCBTree, TestConcurrentIterateAndInsert) {
  scoped_ptr<CBTree<SmallFanoutTraits> > tree;

  int num_ins_threads = 4;
  int num_scan_threads = 4;
  int num_threads = num_ins_threads + num_scan_threads;
  int ins_per_thread = 30000;
  int trials = 2;

  boost::ptr_vector<boost::thread> threads;
  boost::barrier go_barrier(num_threads + 1);
  boost::barrier done_barrier(num_threads + 1);

  for (int i = 0; i < num_ins_threads; i++) {
    threads.push_back(new boost::thread(
                        InsertAndVerify<SmallFanoutTraits>,
                        &go_barrier,
                        &done_barrier,
                        &tree,
                        ins_per_thread * i,
                        ins_per_thread * (i + 1)));
  }
  for (int i = 0; i < num_scan_threads; i++) {
    threads.push_back(new boost::thread(
                        ScanThread<SmallFanoutTraits>,
                        &go_barrier,
                        &done_barrier,
                        &tree));
  }


  // Rather than running one long trial, better to run
  // a bunch of short trials, so that the threads contend a lot
  // more on a smaller tree. As the tree gets larger, contention
  // on areas of the key space diminishes.
  for (int trial = 0; trial < trials; trial++) {
    tree.reset(new CBTree<SmallFanoutTraits>());
    go_barrier.wait();

    done_barrier.wait();

    if (::testing::Test::HasFatalFailure()) {
      tree->DebugPrint();
      return;
    }
  }

  tree.reset(NULL);
  go_barrier.wait();

  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }
}

TEST(TestCBTree, TestScanPerformance) {
  CBTree<BTreeTraits> tree;
  int n_keys = 10000000;
  LOG_TIMING(INFO, StringPrintf("Insert %d keys", n_keys)) {
    InsertRange(&tree, 0, n_keys);
  }

  int scan_trials = 10;
  LOG_TIMING(INFO, StringPrintf("Scan %d keys %d times", n_keys, scan_trials)) {
    for (int i = 0; i < 10; i++)  {
      scoped_ptr<CBTreeIterator<BTreeTraits> > iter(
        tree.NewIterator());
      bool exact;
      iter->SeekAtOrAfter(Slice(""), &exact);
      int count = 0;
      while (iter->IsValid()) {
        count++;
        iter->Next();
      }
      ASSERT_EQ(count, n_keys);
    }
  }
}

}
}
}
