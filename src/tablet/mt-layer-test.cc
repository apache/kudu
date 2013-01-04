// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>

#include "tablet/layer-test-base.h"

DEFINE_int32(num_threads, 16, "Number of threads to test");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


class TestMultiThreadedLayer : public TestLayer {
public:
  void LayerUpdateThread(Layer *l) {
    unordered_set<uint32_t> updated;
    UpdateExistingRows(l, 0.5f, &updated);
  }

  void FlushThread(Layer *l) {
    for (int i = 0; i < 10; i++) {
      l->FlushDeltas();
    }
  }

  void StartUpdaterThreads(boost::ptr_vector<boost::thread> *threads,
                           Layer *l,
                           int n_threads) {
    for (int i = 0; i < n_threads; i++) {
      threads->push_back(new boost::thread(
                           &TestMultiThreadedLayer::LayerUpdateThread, this,
                           l));
    }
  }

  void StartFlushThread(boost::ptr_vector<boost::thread> *threads,
                        Layer *l) {
    threads->push_back(new boost::thread(
                         &TestMultiThreadedLayer::FlushThread, this, l));
  }

  void JoinThreads(boost::ptr_vector<boost::thread> *threads) {
    BOOST_FOREACH(boost::thread &thr, *threads) {
      thr.join();
    }
  }
};


TEST_F(TestMultiThreadedLayer, TestMTUpdate) {
  WriteTestLayer();

  // Re-open the layer
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // Spawn a bunch of threads, each of which will do updates.
  boost::ptr_vector<boost::thread> threads;
  StartUpdaterThreads(&threads, &l, FLAGS_num_threads);

  JoinThreads(&threads);
}

TEST_F(TestMultiThreadedLayer, TestMTUpdateAndFlush) {
  WriteTestLayer();

  // Re-open the layer
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // Spawn a bunch of threads, each of which will do updates.
  boost::ptr_vector<boost::thread> threads;
  StartUpdaterThreads(&threads, &l, FLAGS_num_threads);
  StartFlushThread(&threads, &l);

  JoinThreads(&threads);

  // TODO: test that updates were successful -- collect the updated
  // row lists from all the threads, and verify them.
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
