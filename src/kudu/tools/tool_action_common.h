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
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/net/net_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoDelta;

namespace client {
class KuduClient;
} // namespace client

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
class RpcController;
} // namespace rpc

namespace log {
class ReadableLogSegment;
} // namespace log

namespace server {
class ServerStatusPB;
} // namespace server

namespace tserver {
class TabletServerServiceProxy;
} // namespace tserver

namespace tools {

// Constants for parameters and descriptions.
extern const char* const kMasterAddressesArg;
extern const char* const kMasterAddressesArgDesc;
extern const char* const kDestMasterAddressesArg;
extern const char* const kDestMasterAddressesArgDesc;
extern const char* const kTableNameArg;
extern const char* const kTabletIdArg;
extern const char* const kTabletIdArgDesc;
extern const char* const kTabletIdsCsvArg;
extern const char* const kTabletIdsCsvArgDesc;
extern const char* const kRowsetIdsCsvArg;
extern const char* const kRowsetIdsCsvArgDesc;

extern const char* const kMasterAddressArg;
extern const char* const kMasterAddressDesc;

extern const char* const kTServerAddressArg;
extern const char* const kTServerAddressDesc;

extern const char* const kCurrentLeaderUUIDArg;

// Builder for actions involving RPC communications, either with a whole Kudu
// cluster or a particular Kudu RPC server.
class RpcActionBuilder : public ActionBuilder {
 public:
  RpcActionBuilder(std::string name, ActionRunner runner);
  std::unique_ptr<Action> Build() override;
};

// Builder for actions involving RPC communications with a Kudu cluster.
class ClusterActionBuilder : public RpcActionBuilder {
 public:
  ClusterActionBuilder(std::string name, ActionRunner runner);
};

// Builder for actions involving RPC communications with a Kudu master.
class MasterActionBuilder : public RpcActionBuilder {
 public:
  MasterActionBuilder(std::string name, ActionRunner runner);
};

// Builder for actions involving RPC communications with a Kudu tablet server.
class TServerActionBuilder : public RpcActionBuilder {
 public:
  TServerActionBuilder(std::string name, ActionRunner runner);
};

// Utility methods used by multiple actions across different modes.

// Build an RPC messenger with the specified name. The messenger has the
// properties as defined by the command line flags/options.
Status BuildMessenger(std::string name,
                      std::shared_ptr<rpc::Messenger>* messenger);

// Builds a proxy to a Kudu server running at 'address', returning it in
// 'proxy'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
template<class ProxyClass>
Status BuildProxy(const std::string& address,
                  uint16_t default_port,
                  std::unique_ptr<ProxyClass>* proxy);

// Get the current status of the Kudu server running at 'address', storing it
// in 'status'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status GetServerStatus(const std::string& address, uint16_t default_port,
                       server::ServerStatusPB* status);


Status GetReplicas(tserver::TabletServerServiceProxy* proxy,
                   std::vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB>* replicas);

// Prints the contents of a WAL segment to stdout.
//
// The following gflags affect the output:
// - print_entries: in what style entries should be printed.
// - print_meta: whether or not headers/footers are printed.
// - truncate_data: how many bytes to print for each data field.
Status PrintSegment(const scoped_refptr<log::ReadableLogSegment>& segment);

// Print the current status of the Kudu server running at 'address'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status PrintServerStatus(const std::string& address, uint16_t default_port);

// Print the current timestamp of the Kudu server running at 'address'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status PrintServerTimestamp(const std::string& address, uint16_t default_port);

// Prints the values of the gflags set for the Kudu server running at 'address'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status PrintServerFlags(const std::string& address, uint16_t default_port);

// Changes the value of the gflag given by 'flag' to the value in 'value' on
// the Kudu server running at 'address'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status SetServerFlag(const std::string& address, uint16_t default_port,
                     const std::string& flag, const std::string& value);

// Dump the memtrackers of the server at 'address'.
//
// If 'address' does not contain a port, 'default_port' will be used instead.
Status DumpMemTrackers(const std::string& address, uint16_t default_port);

// Return true if 'str' matches any of the patterns in 'patterns', or if
// 'patterns' is empty.
bool MatchesAnyPattern(const std::vector<std::string>& patterns, const std::string& str);

// Create a Kudu client connected to the cluster with the set of the specified
// master addresses. The 'can_see_all_replicas' controls whether the client sees
// other than VOTER (e.g., NON_VOTER) tablet replicas.
Status CreateKuduClient(const std::vector<std::string>& master_addresses,
                        client::sp::shared_ptr<client::KuduClient>* client,
                        bool can_see_all_replicas = false);

// Creates a Kudu client connected to the cluster whose master addresses are specified by
// 'master_addresses_arg'
Status CreateKuduClient(const RunnerContext& context,
                        const char* master_addresses_arg,
                        client::sp::shared_ptr<client::KuduClient>* client);

// Creates a Kudu client connected to the cluster whose master addresses are specified by
// the kMasterAddressesArg argument in 'context'.
Status CreateKuduClient(const RunnerContext& context,
                        client::sp::shared_ptr<client::KuduClient>* client);

// Parses 'master_addresses_arg' from 'context' into 'master_addresses_str', a
// comma-separated string of host/port pairs.
//
// If 'master_addresses_arg' starts with a '@' it is interpreted as a cluster name and
// resolved against a config file in ${KUDU_CONFIG}/kudurc with content like:
//
// clusters_info:
//   cluster1:
//     master_addresses: ip1:port1,ip2:port2,ip3:port3
//   cluster2:
//     master_addresses: ip4:port4
Status ParseMasterAddressesStr(
    const RunnerContext& context,
    const char* master_addresses_arg,
    std::string* master_addresses_str);

// Like above, but parse Kudu master addresses into a string according to the
// kMasterAddressesArg argument in 'context'.
Status ParseMasterAddressesStr(
    const RunnerContext& context,
    std::string* master_addresses_str);

// Like above, but parse Kudu master addresses into a string vector according to the
// 'master_addresses_arg' argument in 'context'.
Status ParseMasterAddresses(
    const RunnerContext& context,
    const char* master_addresses_arg,
    std::vector<std::string>* master_addresses);

// Like above, but parse Kudu master addresses into a string vector according to the
// kMasterAddressesArg argument in 'context'.
Status ParseMasterAddresses(
    const RunnerContext& context,
    std::vector<std::string>* master_addresses);

// Get full path to the top-level 'kudu' tool binary in the output 'path' parameter.
// Returns appropriate error if the binary is not found.
Status GetKuduToolAbsolutePathSafe(std::string* path);

// Parses a comma separated list of "host:port" pairs into an unordered set of
// HostPort objects. If no port is specified for an entry in the comma separated list,
// the default master port is used for that entry's pair.
Status MasterAddressesToSet(
    const std::string& master_addresses_arg,
    kudu::UnorderedHostPortSet* res);

// Make sure the list of master addresses specified in 'master_addresses'
// corresponds to the actual list of masters addresses in the cluster,
// as reported in ConnectToMasterResponsePB::master_addrs.
Status VerifyMasterAddressList(const std::vector<std::string>& master_addresses);

// Parses the instance file set by the 'instance_file' flag, and sets the
// server key on the default Env.
Status SetServerKey();

// A table of data to present to the user.
//
// Supports formatting based on the --format flag.
// All data is buffered in memory before being output.
//
// Example usage:
//    DataTable table({"person", "favorite color"});
//    vector<string> cols(2);
//    AddTableRow({"joe", "red"}, &cols);
//    AddTableRow({"bob", "green"}, &cols);
//    AddTableRow({"alice", "yellow"}, &cols);
//    PrintTable(headers, cols, cout);
class DataTable {
 public:
  // Construct a table with the given column names.
  explicit DataTable(std::vector<std::string> col_names);

  // Add a row of data to the table.
  //
  // REQUIRES: 'row.size()' matches the number of column names specified
  // in the constructor.
  void AddRow(std::vector<std::string> row);

  // Add a column of data to the right side of the table.
  //
  // REQUIRES: if any rows have been added already, the length of this column
  // must match the length of all existing columns.
  void AddColumn(std::string name, std::vector<std::string> column);

  // Print the table to 'out'.
  Status PrintTo(std::ostream& out) const WARN_UNUSED_RESULT;
 private:
  std::vector<std::string> column_names_;
  std::vector<std::vector<std::string>> columns_;
};

// Wrapper around a Kudu client which allows calling proxy methods on the leader
// master.
class LeaderMasterProxy {
 public:
  LeaderMasterProxy() = default;
  explicit LeaderMasterProxy(client::sp::shared_ptr<client::KuduClient> client);

  // Initializes the leader master proxy with the given master addresses,
  // RPC timeout, and connection negotiation timeout.
  Status Init(const std::vector<std::string>& master_addrs,
              const MonoDelta& timeout,
              const MonoDelta& connection_negotiation_timeout);

  // Initialize the leader master proxy given the provided tool context.
  //
  // Uses the required 'master_addresses' option for the master addresses,
  // the optional 'timeout_ms' flag to control admin and operation timeouts, and
  // the optional 'connection_negotiation_timeout_ms' flag to control
  // the client-side RPC connection negotiation timeout.
  Status Init(const RunnerContext& context);

  // Calls a master RPC service method on the current leader master.
  template<typename Req, typename Resp>
  Status SyncRpc(const Req& req,
                 Resp* resp,
                 std::string func_name,
                 const std::function<void(master::MasterServiceProxy*,
                                          const Req&, Resp*,
                                          rpc::RpcController*,
                                          const rpc::ResponseCallback&)>& func,
                 std::vector<uint32_t> required_feature_flags = {})
      WARN_UNUSED_RESULT;

 private:
  client::sp::shared_ptr<client::KuduClient> client_;
};

} // namespace tools
} // namespace kudu
