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

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <boost/function.hpp>

#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoTime;

namespace client {
class KuduClient;
} // namespace client


namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class RpcController;
} // namespace rpc

namespace log {
class ReadableLogSegment;
} // namespace log

namespace server {
class ServerStatusPB;
} // namespace server

namespace tools {

struct RunnerContext;

// Constants for parameters and descriptions.
extern const char* const kMasterAddressesArg;
extern const char* const kMasterAddressesArgDesc;
extern const char* const kTabletIdArg;
extern const char* const kTabletIdArgDesc;

// Utility methods used by multiple actions across different modes.

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

// Changes the value of the gflag given by 'flag' to the value in 'value' on
// the Kudu server running at 'address'.
//
// If 'address' does not contain a port, 'default_port' is used instead.
Status SetServerFlag(const std::string& address, uint16_t default_port,
                     const std::string& flag, const std::string& value);

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
  // Initialize the leader master proxy given the provided tool context.
  //
  // Uses the required 'master_addresses' option for the master addresses, and
  // the optional 'timeout_ms' flag to control admin and operation timeouts.
  Status Init(const RunnerContext& context);

  // Calls a master RPC service method on the current leader master.
  template<typename Req, typename Resp>
  Status SyncRpc(const Req& req,
                 Resp* resp,
                 const char* func_name,
                 const boost::function<Status(master::MasterServiceProxy*,
                                              const Req&, Resp*,
                                              rpc::RpcController*)>& func);

 private:
  client::sp::shared_ptr<client::KuduClient> client_;
};

} // namespace tools
} // namespace kudu
