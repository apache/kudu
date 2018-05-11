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

#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/rebalancer.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"

using std::cout;
using std::endl;
using std::multimap;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

#define PUSH_PREPEND_NOT_OK(s, statuses, msg) do { \
  ::kudu::Status _s = (s); \
  if (PREDICT_FALSE(!_s.ok())) { \
    (statuses).push_back(string((msg)) + ": " + _s.message().ToString()); \
  } \
} while (0);

DEFINE_string(tables, "",
              "Tables to check (comma-separated list of names). "
              "If not specified, checks all tables.");

DEFINE_string(tablets, "",
              "Tablets to check (comma-separated list of IDs) "
              "If not specified, checks all tablets.");

DEFINE_uint32(max_moves_per_server, 5,
              "Maximum number of replica moves to perform concurrently on one "
              "tablet server: 'move from' and 'move to' are counted "
              "as separate move operations.");

DEFINE_uint32(max_staleness_interval_sec, 300,
              "Maximum duration of the 'staleness' interval, when the "
              "rebalancer cannot make any progress in scheduling new moves and "
              "no prior scheduled moves are left, even if re-synchronizing "
              "against the cluster's state again and again. Such a staleness "
              "usually happens in case of a persistent problem with the "
              "cluster or when some unexpected concurrent activity is "
              "present (such as automatic recovery of failed replicas, etc.)");

DEFINE_int64(max_run_time_sec, 0,
             "Maximum time to run the rebalancing, in seconds. Specifying 0 "
             "means not imposing any limit on the rebalancing run time.");

DEFINE_bool(move_single_replicas, false,
            "Whether to move single replica tablets (i.e. replicas of tablets "
            "of replication factor 1).");

DEFINE_bool(output_replica_distribution_details, false,
            "Whether to output details on per-table and per-server "
            "replica distribution");

namespace kudu {
namespace tools {

namespace {

Status RunKsck(const RunnerContext& context) {
  vector<string> master_addresses = Split(
      FindOrDie(context.required_args, kMasterAddressesArg), ",");
  shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK_PREPEND(RemoteKsckCluster::Build(master_addresses, &cluster),
                        "unable to build KsckCluster");
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  ksck->set_table_filters(Split(FLAGS_tables, ",", strings::SkipEmpty()));
  ksck->set_tablet_id_filters(Split(FLAGS_tablets, ",", strings::SkipEmpty()));

  return ksck->RunAndPrintResults();
}

// Rebalance the cluster. The process is run step-by-step, where at each step
// a new batch of move operations is output by the algorithm. As many as
// possible replica movements from one batch are performed concurrently, while
// running not more than the specified number of concurrent replica movements
// at each tablet server. In other words, at every moment a single tablet server
// can be the source and the destination of no more than the specified number of
// move operations.
Status RunRebalance(const RunnerContext& context) {
  Rebalancer rebalancer(Rebalancer::Config(
      std::move(Split(FindOrDie(context.required_args, kMasterAddressesArg), ",")),
      std::move(Split(FLAGS_tables, ",", strings::SkipEmpty())),
      FLAGS_max_moves_per_server,
      FLAGS_max_staleness_interval_sec,
      FLAGS_max_run_time_sec,
      FLAGS_move_single_replicas,
      FLAGS_output_replica_distribution_details));

  // Print info on pre-rebalance distribution of replicas.
  RETURN_NOT_OK(rebalancer.PrintStats(cout));

  Rebalancer::RunStatus result_status;
  size_t moves_count;
  RETURN_NOT_OK(rebalancer.Run(&result_status, &moves_count));

  const string msg_template = "rebalancing is complete: $0 (moved $1 replicas)";
  string msg_result_status;
  switch (result_status) {
    case Rebalancer::RunStatus::CLUSTER_IS_BALANCED:
      msg_result_status = "cluster is balanced";
      break;
    case Rebalancer::RunStatus::TIMED_OUT:
      msg_result_status = "time is up";
      break;
    default:
      msg_result_status = "unexpected rebalancer status";
      DCHECK(false) << msg_result_status;
      break;
  }
  cout << endl << Substitute(msg_template, msg_result_status, moves_count) << endl;

  if (moves_count != 0) {
    // Print info on post-rebalance distribution of replicas, if any moves
    // were performed at all.
    RETURN_NOT_OK(rebalancer.PrintStats(cout));
  }

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildClusterMode() {
  ModeBuilder builder("cluster");
  builder.Description("Operate on a Kudu cluster");

  {
    constexpr auto desc = "Check the health of a Kudu cluster";
    constexpr auto extra_desc = "By default, ksck checks that master and "
        "tablet server processes are running, and that table metadata is "
        "consistent. Use the 'checksum' flag to check that tablet data is "
        "consistent (also see the 'tables' and 'tablets' flags). Use the "
        "'checksum_snapshot' along with 'checksum' if the table or tablets "
        "are actively receiving inserts or updates. Use the 'verbose' flag to "
        "output detailed information on cluster status even if no "
        "inconsistency is found in metadata.";

    unique_ptr<Action> ksck = ActionBuilder("ksck", &RunKsck)
        .Description(desc)
        .ExtraDescription(extra_desc)
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("checksum_cache_blocks")
        .AddOptionalParameter("checksum_scan")
        .AddOptionalParameter("checksum_scan_concurrency")
        .AddOptionalParameter("checksum_snapshot")
        .AddOptionalParameter("checksum_timeout_sec")
        .AddOptionalParameter("color")
        .AddOptionalParameter("consensus")
        .AddOptionalParameter("ksck_format")
        .AddOptionalParameter("tables")
        .AddOptionalParameter("tablets")
        .Build();
    builder.AddAction(std::move(ksck));
  }

  {
    constexpr auto desc = "Move tablet replicas between tablet servers to "
        "balance replica counts for each table and for the cluster as a whole.";
    constexpr auto extra_desc = "The rebalancing tool moves tablet replicas "
        "between tablet servers, in the same manner as the "
        "'kudu tablet change_config move_replica' command, attempting to "
        "balance the count of replicas per table on each tablet server, "
        "and after that attempting to balance the total number of replicas "
        "per tablet server.";
    unique_ptr<Action> rebalance = ActionBuilder("rebalance", &RunRebalance)
        .Description(desc)
        .ExtraDescription(extra_desc)
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("max_moves_per_server")
        .AddOptionalParameter("max_run_time_sec")
        .AddOptionalParameter("max_staleness_interval_sec")
        .AddOptionalParameter("move_single_replicas")
        .AddOptionalParameter("output_replica_distribution_details")
        .AddOptionalParameter("tables")
        .Build();
    builder.AddAction(std::move(rebalance));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu

