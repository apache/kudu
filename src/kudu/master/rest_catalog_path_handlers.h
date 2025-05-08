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

#include <iosfwd>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {

namespace master {

class Master;

class RestCatalogPathHandlers final {
 public:
  explicit RestCatalogPathHandlers(Master* master) : master_(DCHECK_NOTNULL(master)) {}

  ~RestCatalogPathHandlers() = default;

  void Register(Webserver* server);

 private:
  void HandleApiTableEndpoint(const Webserver::WebRequest& req,
                              Webserver::PrerenderedWebResponse* resp);
  void HandleApiTablesEndpoint(const Webserver::WebRequest& req,
                               Webserver::PrerenderedWebResponse* resp);
  void HandleLeaderEndpoint(const Webserver::WebRequest& req,
                            Webserver::PrerenderedWebResponse* resp);
  void HandleApiDocsEndpoint(const Webserver::WebRequest& req,
                             Webserver::WebResponse* resp);
  void HandleApiSpecEndpoint(const Webserver::WebRequest& req,
                             Webserver::PrerenderedWebResponse* resp);

  // Handles REST API endpoints based on the request method and path.
  void HandleGetTables(std::ostringstream* output,
                       const Webserver::WebRequest& req,
                       HttpStatusCode* status_code);
  void HandlePostTables(std::ostringstream* output,
                        const Webserver::WebRequest& req,
                        HttpStatusCode* status_code);
  void HandleGetTable(std::ostringstream* output,
                      const Webserver::WebRequest& req,
                      HttpStatusCode* status_code);
  void HandlePutTable(std::ostringstream* output,
                      const Webserver::WebRequest& req,
                      HttpStatusCode* status_code);
  void HandleDeleteTable(std::ostringstream* output,
                         const Webserver::WebRequest& req,
                         HttpStatusCode* status_code);

  // Print a JSON object representing a table to 'output'.
  void PrintTableObject(std::ostringstream* output,
                        const std::string& table_id,
                        HttpStatusCode* status_code);

  Master* master_;
  DISALLOW_COPY_AND_ASSIGN(RestCatalogPathHandlers);
};

}  // namespace master
}  // namespace kudu
