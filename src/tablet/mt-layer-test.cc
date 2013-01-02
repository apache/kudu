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

};


TEST_F(TestMultiThreadedLayer, TestMTUpdate) {
  WriteTestLayer();

  // Re-open the layer
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // Spawn a bunch of threads, each of which will do updates.
  boost::ptr_vector<boost::thread> threads;
  for (int i = 0; i < FLAGS_num_threads; i++) {
    threads.push_back(new boost::thread(
                        &TestMultiThreadedLayer::LayerUpdateThread, this,
                        &l));
  }

  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
