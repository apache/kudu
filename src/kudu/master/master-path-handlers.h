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
#ifndef KUDU_MASTER_MASTER_PATH_HANDLERS_H
#define KUDU_MASTER_MASTER_PATH_HANDLERS_H

#include <string>
#include <utility>

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"
#include "kudu/util/status.h"

namespace kudu {
class EasyJson;

namespace master {

class Master;
class TSDescriptor;

// Web page support for the master.
class MasterPathHandlers {
 public:
  explicit MasterPathHandlers(Master* master)
    : master_(master) {
  }

  ~MasterPathHandlers();

  Status Register(Webserver* server);

 private:
  void HandleTabletServers(const Webserver::WebRequest& req,
                           Webserver::WebResponse* resp);
  void HandleCatalogManager(const Webserver::WebRequest& req,
                            Webserver::WebResponse* resp);
  void HandleTablePage(const Webserver::WebRequest& req,
                       Webserver::WebResponse* resp);
  void HandleMasters(const Webserver::WebRequest& req,
                     Webserver::WebResponse* resp);
  void HandleDumpEntities(const Webserver::WebRequest& req,
                          Webserver::PrerenderedWebResponse* resp);

  // Returns a pair (text, target) given a tserver's TSDescriptor and a tablet id.
  // - text is the http host and port for the tserver, if available, or the tserver's uuid.
  // - target is a url to the tablet page for the tablet on the tserver's webui,
  //   or an empty string if no http address is available for the tserver.
  std::pair<std::string, std::string> TSDescToLinkPair(const TSDescriptor& desc,
                                                       const std::string& tablet_id) const;

  // Return a CSV of master addresses suitable for display.
  std::string MasterAddrsToCsv() const;

  // If a leader master is known and has an http address, place it in leader_http_addr.
  Status GetLeaderMasterHttpAddr(std::string* leader_http_addr) const;

  // Adds the necessary properties to 'output' to set up a redirect to the leader master, or
  // provide an error message if no redirect is possible.
  // The redirect will link to <master web UI url>/path&redirects=(redirects + 1).
  void SetupLeaderMasterRedirect(const std::string& path, int redirects, EasyJson* output) const;

  Master* master_;
  DISALLOW_COPY_AND_ASSIGN(MasterPathHandlers);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_MASTER_PATH_HANDLERS_H */
