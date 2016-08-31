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

#include "kudu/tools/tool_action.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <gflags/gflags.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/status.h"

#define PUSH_PREPEND_NOT_OK(s, statuses, msg) do { \
  ::kudu::Status _s = (s); \
  if (PREDICT_FALSE(!_s.ok())) { \
    statuses.push_back(string(msg) + ": " + _s.ToString()); \
  } \
} while (0);


DEFINE_bool(checksum_scan, false,
            "Perform a checksum scan on data in the cluster.");

DEFINE_string(tables, "",
              "Tables to check (comma-separated list of names). "
              "If not specified, checks all tables.");

DEFINE_string(tablets, "",
              "Tablets to check (comma-separated list of IDs) "
              "If not specified, checks all tablets.");

namespace kudu {
namespace tools {

using std::cerr;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace {

const char* const kMasterAddressesArg = "master_addresses";

Status RunKsck(const RunnerContext& context) {
  string master_addresses_str = FindOrDie(context.required_args,
                                          kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  shared_ptr<KsckMaster> master;
  RETURN_NOT_OK_PREPEND(RemoteKsckMaster::Build(master_addresses, &master),
                        "unable to build KsckMaster");

  shared_ptr<KsckCluster> cluster(new KsckCluster(master));
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  ksck->set_table_filters(strings::Split(
      FLAGS_tables, ",", strings::SkipEmpty()));
  ksck->set_tablet_id_filters(strings::Split(
      FLAGS_tablets, ",", strings::SkipEmpty()));

  RETURN_NOT_OK_PREPEND(ksck->CheckMasterRunning(),
                        "master liveness check error");
  RETURN_NOT_OK_PREPEND(ksck->FetchTableAndTabletInfo(),
                        "error fetching the cluster metadata from the Master server");

  vector<string> error_messages;
  PUSH_PREPEND_NOT_OK(ksck->FetchInfoFromTabletServers(), error_messages,
                      "error fetching info from tablet servers");

  // TODO: Add support for tables / tablets filter in the consistency check.
  PUSH_PREPEND_NOT_OK(ksck->CheckTablesConsistency(), error_messages,
                      "table consistency check error");

  if (FLAGS_checksum_scan) {
    PUSH_PREPEND_NOT_OK(ksck->ChecksumData(ChecksumOptions()),
                        error_messages, "checksum scan error");
  }

  // All good.
  if (error_messages.empty()) {
    cout << "OK" << endl;
    return Status::OK();
  }

  // Something went wrong.
  cerr << "==================" << endl;
  cerr << "Errors:" << endl;
  cerr << "==================" << endl;
  for (const auto& s : error_messages) {
    cerr << s << endl;
  }
  cerr << endl;
  cerr << "FAILED" << endl;
  return Status::RuntimeError("ksck discovered errors");
}

} // anonymous namespace

unique_ptr<Mode> BuildClusterMode() {
  string desc = "Check the health of a Kudu cluster";
  string extra_desc = "By default, ksck checks that master and tablet server "
      "processes are running, and that table metadata is consistent. Use the "
      "'checksum' flag to check that tablet data is consistent (also see the "
      "'tables' and 'tablets' flags). Use the 'checksum_snapshot' along with "
      "'checksum' if the table or tablets are actively receiving inserts or "
      "updates.";
  unique_ptr<Action> ksck =
      ActionBuilder("ksck", &RunKsck)
      .Description(desc)
      .ExtraDescription(extra_desc)
      .AddRequiredParameter({
        kMasterAddressesArg,
        "Comma-separated list of Kudu Master addressess where each address is "
        "of form 'hostname:port'" })
      .AddOptionalParameter("checksum_scan")
      .AddOptionalParameter("checksum_scan_concurrency")
      .AddOptionalParameter("checksum_snapshot")
      .AddOptionalParameter("color")
      .AddOptionalParameter("tables")
      .AddOptionalParameter("tablets")
      .Build();

  return ModeBuilder("cluster")
      .Description("Operate on a Kudu cluster")
      .AddAction(std::move(ksck))
      .Build();
}

} // namespace tools
} // namespace kudu

