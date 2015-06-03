// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <glog/logging.h>
#include <boost/thread/thread.hpp>
#include <stdlib.h>

#include "kudu/benchmarks/tpch/line_item_tsv_importer.h"
#include "kudu/benchmarks/tpch/rpc_line_item_dao.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

DEFINE_string(tpch_path_to_data, "/data/3/dbgen/truncated_lineitem.tbl",
              "The full path to the '|' separated file containing the lineitem table.");
DEFINE_int32(tpch_demo_window, 3000000, "Size of the trailing windows, in terms of order numbers");
DEFINE_int32(tpch_demo_starting_point, 6000000, "Order number from which we start inserting");
DEFINE_int32(tpch_demo_updater_threads, 1 , "Number of threads that update, can be 0");
DEFINE_int32(tpch_demo_inserter_threads, 0 , "Number of threads that insert, min 0, max 1");
DEFINE_string(master_address, "localhost",
              "Address of master for the cluster to operate on");
DEFINE_int32(tpch_max_batch_size, 1000,
             "Maximum number of inserts/updates to batch at once");

const char * const kTabletId = "tpch1";

// This program is used to drive both inserts and read+mutates on the tpch
// data set.
// First, use the tpch1 insert test configured to talk to your cluster in order
// to load the initial dataset, the default start point and window are based
// on a 6GB lineitem file.
// Then, use a bigger file that's truncated up to the 6,000,000th order in
// order to insert even more data. The default path shows where that file is on
// the kudu machine a1228.
// Only 1 insert thread can be used, but many updaters can be specified.

namespace kudu {

class Demo {
 public:
  Demo(int window, int starting_point) : counter_(starting_point), window_(window) {}

  // Generate the next order, using a moving trailing window
  // The moving comes from the insert thread, no insert thread means no movement
  // The window size is configurable with tpch_demo_window
  // The order is taken at random within the window
  int GetNextOrder() {
    return (rand() % window_) + (counter_ - window_);
  }

  // Atomically replaces the current order number, thus moving the window
  void SetLastInsertedOrder(int order_number) {
    base::subtle::NoBarrier_Store(&counter_, order_number);
  }

 private:
  base::subtle::Atomic64 counter_;
  const int window_;
  DISALLOW_COPY_AND_ASSIGN(Demo);
};


static void UpdateRow(int64_t order, int line, int quantity, KuduPartialRow* row) {
  CHECK_OK(row->SetInt64(tpch::kOrderKeyColIdx, order));
  CHECK_OK(row->SetInt32(tpch::kLineNumberColIdx, line));
  CHECK_OK(row->SetInt32(tpch::kQuantityColIdx, quantity));
}

// This thread continuously updates the l_quantity column from orders
// as determined by Demo::GetNextOrder. It first needs to read the order to get
// the quantity, picking the highest line number, does l_quantity+1, then
// writes it back
static void UpdateThread(Demo *demo) {
  client::KuduSchema query_schema = tpch::CreateMS3DemoQuerySchema();
  gscoped_ptr<kudu::RpcLineItemDAO> dao(new kudu::RpcLineItemDAO(FLAGS_master_address,
                                        kTabletId, FLAGS_tpch_max_batch_size));
  dao->Init();

  while (true) {
    // 1. Get the next order to update
    int current_order = demo->GetNextOrder();
    VLOG(1) << "current order: " << current_order;

    // 2. Fetch the order including the column we want to update
    vector<client::KuduColumnRangePredicate> preds;
    client::KuduColumnRangePredicate pred(query_schema.Column(0), &current_order, &current_order);
    preds.push_back(pred);
    gscoped_ptr<RpcLineItemDAO::Scanner> scanner;
    dao->OpenScanner(query_schema, preds, &scanner);
    vector<client::KuduRowResult> rows;
    vector<client::KuduRowResult> batch;
    while (scanner->HasMore()) {
      scanner->GetNext(&batch);
      rows.insert(rows.end(), batch.begin(), batch.end());
    }
    if (rows.empty()) continue;
    client::KuduRowResult& last_row(rows.back());

    // 3. The last row has the highest line, we update it
    int64_t l_orderkey;
    int32_t l_linenumber;
    int32_t l_quantity;
    CHECK_OK(last_row.GetInt64(0, &l_orderkey));
    CHECK_OK(last_row.GetInt32(1, &l_linenumber));
    CHECK_OK(last_row.GetInt32(2, &l_quantity));
    uint32_t new_l_quantity = l_quantity + 1;

    // 4. Do the update
    VLOG(1) << "updating " << l_orderkey << " " << l_linenumber << " "
            << l_quantity << " " << new_l_quantity;
    dao->MutateLine(boost::bind(UpdateRow, l_orderkey,
                                l_linenumber, new_l_quantity, _1));
  }
}

// Import line, and write its order number to the argument
static void ImportLine(Demo* demo, LineItemTsvImporter* import, KuduPartialRow* row) {
  int order = import->GetNextLine(row);
  // Move the window forward
  demo->SetLastInsertedOrder(order);
}

// This function inserts all the orders it reads until it runs out, and keeps
// moving the window forward
static void InsertThread(Demo *demo, const string &path) {
  gscoped_ptr<kudu::RpcLineItemDAO> dao(new kudu::RpcLineItemDAO(FLAGS_master_address,
                                        kTabletId, FLAGS_tpch_max_batch_size));
  dao->Init();
  LineItemTsvImporter importer(path);

  while (importer.HasNextLine()) {
    dao->WriteLine(boost::bind(ImportLine, demo, &importer, _1));
  }
  dao->FinishWriting();
}

static int DemoMain(int argc, char** argv) {
  Demo demo(FLAGS_tpch_demo_window, FLAGS_tpch_demo_starting_point);
  int num_updaters = FLAGS_tpch_demo_updater_threads;
  int num_inserter = FLAGS_tpch_demo_inserter_threads;
  if (num_inserter > 1) {
    LOG(FATAL) << "Can only insert with 1 thread";
    return 1;
  }
  for (int i = 0; i < num_inserter; i++) {
    boost::thread inserter(InsertThread, &demo, FLAGS_tpch_path_to_data);
  }

  for (int i = 0; i < num_updaters; i++) {
    boost::thread flushdm_thread(UpdateThread, &demo);
  }

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
  return 0;
}
} //namespace kudu

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  kudu::InitGoogleLoggingSafe(argv[0]);

  return kudu::DemoMain(argc, argv);
}
