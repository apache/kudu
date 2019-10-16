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
#ifndef KUDU_TSERVER_TSERVER_PATH_HANDLERS_H
#define KUDU_TSERVER_TSERVER_PATH_HANDLERS_H

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tserver {

class TabletServer;

class TabletServerPathHandlers {
 public:
  explicit TabletServerPathHandlers(TabletServer* tserver)
    : tserver_(tserver) {
  }

  ~TabletServerPathHandlers();

  Status Register(Webserver* server);

 private:
  void HandleScansPage(const Webserver::WebRequest& req,
                       Webserver::WebResponse* resp);
  void HandleTabletsPage(const Webserver::WebRequest& req,
                         Webserver::WebResponse* resp);
  void HandleTabletPage(const Webserver::WebRequest& req,
                        Webserver::WebResponse* resp);
  void HandleTransactionsPage(const Webserver::WebRequest& req,
                              Webserver::PrerenderedWebResponse* resp);
  void HandleTabletSVGPage(const Webserver::WebRequest& req,
                           Webserver::WebResponse* resp);
  void HandleLogAnchorsPage(const Webserver::WebRequest& req,
                            Webserver::WebResponse* resp);
  void HandleConsensusStatusPage(const Webserver::WebRequest& req,
                                 Webserver::WebResponse* resp);
  void HandleDashboardsPage(const Webserver::WebRequest& req,
                            Webserver::WebResponse* resp);
  void HandleMaintenanceManagerPage(const Webserver::WebRequest& req,
                                    Webserver::WebResponse* resp);

  TabletServer* tserver_;

  DISALLOW_COPY_AND_ASSIGN(TabletServerPathHandlers);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TSERVER_PATH_HANDLERS_H */
