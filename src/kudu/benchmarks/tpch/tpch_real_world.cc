// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Benchmarking tool to run tpch1 concurrently with inserts.
//
// Requirements:
//  - TPC-H's dbgen tool, compiled.
//  - Optionally, a running cluster. By default it starts its own external cluster.
//
// This tool has three main configurations:
//  - tpch_test_runtime_sec: the longest this test can run for, in seconds, excluding startup time.
//    By default, it runs until there's no more rows to insert.
//  - tpch_scaling_factor: the dbgen scaling factor to generate the data. The test will end if
//    dbgen is done generating data, even if there's still time left. The default is 1.
//  - tpch_path_to_dbgen: where to find dbgen, by default it assumes it can be found in the
//    current directory.
//
// This tool has three threads:
//  - One that runs "dbgen -T L" with the configured scale factor.
//  - One that reads from the "lineitem.tbl" named pipe and inserts the rows.
//  - One that runs tpch1 continuously and outputs the timings. This thread won't start until at
//    least some rows have been written, because dbgen takes some seconds to startup. It also
//    stops at soon as it gets the signal that we ran out of time or that there are no more rows to
//    insert, so the last timing shouldn't be used.
//
// TODO Make the inserts multi-threaded. See Kudu-629 for the technique.
#include <boost/bind.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "kudu/benchmarks/tpch/line_item_tsv_importer.h"
#include "kudu/benchmarks/tpch/rpc_line_item_dao.h"
#include "kudu/benchmarks/tpch/tpch-schemas.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_bool(tpch_use_mini_cluster, true,
            "Create a mini cluster for the work to be performed against");
DEFINE_bool(tpch_run_queries, true,
            "Query dbgen data as it is inserted");
DEFINE_int32(tpch_max_batch_size, 1000,
             "Maximum number of inserts to batch at once");
DEFINE_int32(tpch_test_client_timeout_msec, 10000,
             "Timeout that will be used for all operations and RPCs");
DEFINE_int32(tpch_test_runtime_sec, 0,
             "How long this test should run for excluding startup time (note that it will also "
             "stop if dbgen finished generating all its data)");
DEFINE_string(tpch_master_addresses, "localhost",
              "Addresses of masters for the cluster to operate on if not using a mini cluster");
DEFINE_string(tpch_mini_cluster_base_dir, "/tmp/tpch",
              "If using a mini cluster, directory for master/ts data");
DEFINE_string(tpch_path_to_dbgen_dir, ".",
              "Path to the directory where the dbgen executable can be found");
DEFINE_string(tpch_path_to_ts_flags_file, "",
              "Path to the file that contains extra flags for the tablet servers if using "
              "a mini cluster. Doesn't use one by default.");
DEFINE_string(tpch_scaling_factor, "1",
             "Scaling factor to use with dbgen, the default is 1");
DEFINE_string(tpch_table_name, "tpch_real_world",
              "Table name to use during the test");

namespace kudu {

using client::KuduColumnRangePredicate;
using client::KuduColumnSchema;
using client::KuduRowResult;
using client::KuduSchema;
using strings::Substitute;

class TpchRealWorld {
 public:
  TpchRealWorld() : rows_inserted_(0), stop_threads_(false) {}

  Status Init();

  gscoped_ptr<RpcLineItemDAO> GetInittedDAO();

  void LoadLineItemsThread();

  void MonitorDbgenThread();

  void RunQueriesThread();

  Status WaitForRowCount(int64_t row_count);

  Status Run();

 private:
  Status CreateFifoFile();
  Status StartDbgen();

  gscoped_ptr<ExternalMiniCluster> cluster_;
  AtomicInt<int64_t> rows_inserted_;
  string master_addresses_;
  AtomicBool stop_threads_;
  string path_to_lineitem_;
  gscoped_ptr<Subprocess> dbgen_proc;
};

Status TpchRealWorld::Init() {
  Env* env = Env::Default();
  if (FLAGS_tpch_use_mini_cluster) {
    if (env->FileExists(FLAGS_tpch_mini_cluster_base_dir)) {
      RETURN_NOT_OK(env->DeleteRecursively(FLAGS_tpch_mini_cluster_base_dir));
    }
    RETURN_NOT_OK(env->CreateDir(FLAGS_tpch_mini_cluster_base_dir));

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    opts.data_root = FLAGS_tpch_mini_cluster_base_dir;
    if (!FLAGS_tpch_path_to_ts_flags_file.empty()) {
      opts.extra_tserver_flags.push_back("--flagfile=" + FLAGS_tpch_path_to_ts_flags_file);
    }

    cluster_.reset(new ExternalMiniCluster(opts));
    RETURN_NOT_OK(cluster_->Start());
    master_addresses_ = cluster_->leader_master()->bound_rpc_hostport().ToString();
  } else {
    master_addresses_ = FLAGS_tpch_master_addresses;
  }

  RETURN_NOT_OK(CreateFifoFile());
  RETURN_NOT_OK(StartDbgen());

  return Status::OK();
}

Status TpchRealWorld::CreateFifoFile() {
  path_to_lineitem_ = "./lineitem.tbl";
  struct stat sbuf;
  if (stat(path_to_lineitem_.c_str(), &sbuf) != 0) {
    if (errno == ENOENT) {
      if (mkfifo(path_to_lineitem_.c_str(), 0644) != 0) {
        string msg = Substitute("Could not create the named pipe for the dbgen output at $0",
                                path_to_lineitem_);
        return Status::InvalidArgument(msg);
      }
    } else {
      return Status::IOError(path_to_lineitem_, ErrnoToString(errno), errno);
    }
  } else {
    if (!S_ISFIFO(sbuf.st_mode)) {
      string msg = Substitute("Please remove the current lineitem file at $0",
                              path_to_lineitem_);
      return Status::InvalidArgument(msg);
    }
  }
  // We get here if the file was already a fifo or if we created it.
  return Status::OK();
}

Status TpchRealWorld::StartDbgen() {
  // This environment variable is necessary if dbgen isn't in the current dir.
  setenv("DSS_CONFIG", FLAGS_tpch_path_to_dbgen_dir.c_str(), 1);
  string path_to_dbgen = Substitute("$0/dbgen", FLAGS_tpch_path_to_dbgen_dir);
  vector<string> argv;
  argv.push_back(path_to_dbgen);
  argv.push_back("-T");
  argv.push_back("L");
  argv.push_back("-s");
  argv.push_back(FLAGS_tpch_scaling_factor);
  dbgen_proc.reset(new Subprocess(path_to_dbgen, argv));

  LOG(INFO) << "Running " << path_to_dbgen << "\n" << JoinStrings(argv, "\n");
  RETURN_NOT_OK(dbgen_proc->Start());
  return Status::OK();
}

gscoped_ptr<RpcLineItemDAO> TpchRealWorld::GetInittedDAO() {
  gscoped_ptr<RpcLineItemDAO> dao(new RpcLineItemDAO(master_addresses_,
                                                     FLAGS_tpch_table_name,
                                                     FLAGS_tpch_max_batch_size,
                                                     FLAGS_tpch_test_client_timeout_msec));
  dao->Init();
  return dao.Pass();
}

void TpchRealWorld::LoadLineItemsThread() {
  LOG(INFO) << "Connecting to cluster at " << master_addresses_;
  gscoped_ptr<RpcLineItemDAO> dao = GetInittedDAO();
  LineItemTsvImporter importer(path_to_lineitem_);

  while (importer.HasNextLine() && !stop_threads_.Load()) {
    dao->WriteLine(boost::bind(&LineItemTsvImporter::GetNextLine,
                               &importer, _1));
    int64_t current_count = rows_inserted_.Increment();
    if (current_count % 250000 == 0) {
      LOG(INFO) << "Inserted " << current_count << " rows";
    }
  }
  dao->FinishWriting();
}

void TpchRealWorld::MonitorDbgenThread() {

  while (!stop_threads_.Load()) {
    int ret;
    Status s = dbgen_proc->WaitNoBlock(&ret);
    if (s.ok()) {
      CHECK(ret == 0) << "dbgen exited with a non-zero return code: " << ret;
      LOG(INFO) << "dbgen finished inserting data";
      return;
    } else {
      SleepFor(MonoDelta::FromMilliseconds(100));
    }
  }
  dbgen_proc->Kill(9);
  int ret;
  dbgen_proc->Wait(&ret);
}

void TpchRealWorld::RunQueriesThread() {
  gscoped_ptr<RpcLineItemDAO> dao = GetInittedDAO();
  while (!stop_threads_.Load()) {
    LOG_TIMING(INFO, StringPrintf("querying %ld rows", rows_inserted_.Load())) {
      dao->OpenTpch1Scanner();
      vector<KuduRowResult> rows;
      // We check stop_threads_ even while scanning since it can takes tens of seconds to query.
      // This means that the last timing cannot be used for reporting.
      while (dao->HasMore() && !stop_threads_.Load()) {
        dao->GetNext(&rows);
      }
    }
  }
}

Status TpchRealWorld::WaitForRowCount(int64_t row_count) {
  while (rows_inserted_.Load() < row_count) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  return Status::OK();
}

Status TpchRealWorld::Run() {
  std::vector<scoped_refptr<Thread> > threads;
  scoped_refptr<kudu::Thread> dbgen_thread;
  RETURN_NOT_OK(kudu::Thread::Create("test", "lineitem-generator",
                                     &TpchRealWorld::MonitorDbgenThread, this,
                                     &dbgen_thread));

  scoped_refptr<kudu::Thread> load_items_thread;
  RETURN_NOT_OK(kudu::Thread::Create("test", "lineitem-loader",
                                     &TpchRealWorld::LoadLineItemsThread, this,
                                     &load_items_thread));
  threads.push_back(load_items_thread);

  // It takes some time for dbgen to start outputting rows so there's no need to query yet.
  LOG(INFO) << "Waiting for dbgen to start...";
  RETURN_NOT_OK(WaitForRowCount(10000));

  if (FLAGS_tpch_run_queries) {
    scoped_refptr<kudu::Thread> tpch1_thread;
    RETURN_NOT_OK(kudu::Thread::Create("test", "tpch1-runner",
                                       &TpchRealWorld::RunQueriesThread, this,
                                       &tpch1_thread));
    threads.push_back(tpch1_thread);
  }

  // We'll wait until dbgen finishes or after tpch_test_runtime_sec, whichever comes first.
  int runtime_ms = FLAGS_tpch_test_runtime_sec * 1000;
  ThreadJoiner tj(dbgen_thread.get());
  if (runtime_ms > 0) {
    tj.give_up_after_ms(runtime_ms).warn_after_ms(runtime_ms);
  }
  Status s = tj.Join();

  if (!s.ok()) {
    // Likely we gave up at this point, so we'll need to join with this thread.
    threads.push_back(dbgen_thread);
  }

  stop_threads_.Store(true);

  BOOST_FOREACH(scoped_refptr<kudu::Thread> thr, threads) {
    RETURN_NOT_OK(ThreadJoiner(thr.get()).Join());
  }
  return Status::OK();
}

} // namespace kudu

int main(int argc, char* argv[]) {
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  kudu::InitGoogleLoggingSafe(argv[0]);

  kudu::TpchRealWorld benchmarker;
  kudu::Status s = benchmarker.Init();
  if (!s.ok()) {
    std::cerr << "Couldn't initialize the benchmarking tool, reason: "<< s.ToString() << std::endl;
    return 1;
  }
  s = benchmarker.Run();
  if (!s.ok()) {
    std::cerr << "Couldn't run the benchmarking tool, reason: "<< s.ToString() << std::endl;
    return 1;
  }
  return 0;
}
