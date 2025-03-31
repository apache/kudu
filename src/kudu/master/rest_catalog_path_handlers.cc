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

#include "kudu/master/rest_catalog_path_handlers.h"

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/util/json_util.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/web_callback_registry.h"

// We only use macros here to maintain cohesion with the existing RETURN_NOT_OK-style pattern.
// They provide a consistent way to return JSON-formatted error responses.
#define RETURN_JSON_ERROR(jw, error_msg, status_code, error_code) \
  RETURN_JSON_ERROR_VAL(jw, error_msg, status_code, error_code, void())

#define RETURN_JSON_ERROR_VAL(jw, error_msg, status_code, error_code, retval) \
  {                                                                           \
    (jw).StartObject();                                                       \
    (jw).String("error");                                                     \
    (jw).String(error_msg);                                                   \
    (jw).EndObject();                                                         \
    (status_code) = (error_code);                                             \
    return retval;                                                            \
  }

#define RETURN_JSON_ERROR_FROM_STATUS(jw, status, status_code) \
  RETURN_JSON_ERROR_VAL(jw, (status).ToString(), status_code, GetHttpCodeFromStatus(status), void())

DEFINE_int32(rest_catalog_default_request_timeout_ms, 30 * 1000, "Default request timeout in ms");
TAG_FLAG(rest_catalog_default_request_timeout_ms, advanced);
TAG_FLAG(rest_catalog_default_request_timeout_ms, runtime);

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;
using std::optional;
using std::ostringstream;
using std::string;
using strings::Substitute;

namespace kudu {
namespace master {

static bool CheckIsInitializedAndIsLeader(JsonWriter& jw,  // NOLINT JsonWriter cannot be const
                                          const CatalogManager::ScopedLeaderSharedLock& l,
                                          HttpStatusCode& status_code  // NOLINT
) {
  if (!l.catalog_status().ok()) {
    RETURN_JSON_ERROR_VAL(
        jw, l.catalog_status().ToString(), status_code, HttpStatusCode::ServiceUnavailable, false);
  }
  if (!l.leader_status().ok()) {
    RETURN_JSON_ERROR_VAL(
        jw, "Master is not the leader", status_code, HttpStatusCode::InternalServerError, false);
  }
  return true;
}

static HttpStatusCode GetHttpCodeFromStatus(const Status& status) {
  DCHECK(!status.ok());
  if (status.IsInvalidArgument() || status.IsAlreadyPresent()) {
    return HttpStatusCode::BadRequest;
  }
  if (status.IsNotFound()) {
    return HttpStatusCode::NotFound;
  }
  if (status.IsIllegalState() || status.IsServiceUnavailable()) {
    return HttpStatusCode::ServiceUnavailable;
  }
  return HttpStatusCode::InternalServerError;
}

void RestCatalogPathHandlers::HandleApiTableEndpoint(const Webserver::WebRequest& req,
                                                     Webserver::PrerenderedWebResponse* resp) {
  ostringstream* output = &resp->output;
  JsonWriter jw(output, JsonWriter::COMPACT);
  string table_id;
  auto table_id_it = req.path_params.find("table_id");
  if (table_id_it == req.path_params.end()) {
    RETURN_JSON_ERROR(jw, "Table ID not provided", resp->status_code, HttpStatusCode::BadRequest);
  }
  table_id = table_id_it->second;

  if (table_id.length() != 32) {
    RETURN_JSON_ERROR(jw,
                      "Invalid table ID: must be exactly 32 characters long.",
                      resp->status_code,
                      HttpStatusCode::BadRequest);
  }
  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!CheckIsInitializedAndIsLeader(jw, l, resp->status_code)) {
    return;
  }
  scoped_refptr<TableInfo> table;
  Status status = master_->catalog_manager()->GetTableInfo(table_id, &table);

  if (!status.ok()) {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, resp->status_code);
  }

  if (!table) {
    RETURN_JSON_ERROR(jw, "Table not found", resp->status_code, HttpStatusCode::NotFound);
  }

  if (req.request_method == "GET") {
    HandleGetTable(output, req, &resp->status_code);
  } else if (req.request_method == "PUT") {
    HandlePutTable(output, req, &resp->status_code);
  } else if (req.request_method == "DELETE") {
    HandleDeleteTable(output, req, &resp->status_code);
  } else {
    RETURN_JSON_ERROR(
        jw, "Method not allowed", resp->status_code, HttpStatusCode::MethodNotAllowed);
  }
}

void RestCatalogPathHandlers::HandleApiTablesEndpoint(const Webserver::WebRequest& req,
                                                      Webserver::PrerenderedWebResponse* resp) {
  ostringstream* output = &resp->output;
  JsonWriter jw(output, JsonWriter::COMPACT);
  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!CheckIsInitializedAndIsLeader(jw, l, resp->status_code)) {
    return;
  }

  if (req.request_method == "GET") {
    HandleGetTables(output, req, &resp->status_code);
  } else if (req.request_method == "POST") {
    HandlePostTables(output, req, &resp->status_code);
  } else {
    RETURN_JSON_ERROR(
        jw, "Method not allowed", resp->status_code, HttpStatusCode::MethodNotAllowed);
  }
}

void RestCatalogPathHandlers::HandleGetTables(ostringstream* output,
                                              const Webserver::WebRequest& req,
                                              HttpStatusCode* status_code) {
  ListTablesRequestPB request;
  ListTablesResponsePB response;
  optional<string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->ListTables(&request, &response, user);
  JsonWriter jw(output, JsonWriter::COMPACT);

  if (!status.ok()) {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
  }
  jw.StartObject();
  jw.String("tables");
  jw.StartArray();

  for (const auto& table : response.tables()) {
    jw.StartObject();
    jw.String("table_id");
    jw.String(table.id());
    jw.String("table_name");
    jw.String(table.name());
    jw.EndObject();
  }
  jw.EndArray();
  jw.EndObject();
  *status_code = HttpStatusCode::Ok;
}

void RestCatalogPathHandlers::HandlePostTables(ostringstream* output,
                                               const Webserver::WebRequest& req,
                                               HttpStatusCode* status_code) {
  CreateTableRequestPB request;
  CreateTableResponsePB response;
  JsonWriter jw(output, JsonWriter::COMPACT);

  const string& json_str = req.post_data;
  JsonParseOptions opts;
  opts.case_insensitive_enum_parsing = true;
  const auto s = JsonStringToMessage(json_str, &request, opts);

  if (!s.ok()) {
    RETURN_JSON_ERROR(jw,
                      Substitute("JSON table object is not correct: $0", json_str),
                      *status_code,
                      HttpStatusCode::BadRequest);
  }
  optional<string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->CreateTableWithUser(&request, &response, user);

  if (!status.ok()) {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
  }

  IsCreateTableDoneRequestPB check_req;
  IsCreateTableDoneResponsePB check_resp;
  check_req.mutable_table()->set_table_id(response.table_id());
  MonoTime deadline =
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_rest_catalog_default_request_timeout_ms);

  while (MonoTime::Now() < deadline) {
    status = master_->catalog_manager()->IsCreateTableDone(&check_req, &check_resp, user);

    if (!status.ok()) {
      RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
    }

    if (check_resp.has_error()) {
      RETURN_JSON_ERROR(jw,
                        check_resp.error().ShortDebugString(),
                        *status_code,
                        HttpStatusCode::InternalServerError);
    }

    if (check_resp.done()) {
      PrintTableObject(output, response.table_id(), status_code);
      *status_code = HttpStatusCode::Created;
      return;
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
  RETURN_JSON_ERROR(jw,
                    "Create table timed out while waiting for operation completion",
                    *status_code,
                    HttpStatusCode::InternalServerError);
}

void RestCatalogPathHandlers::HandleGetTable(ostringstream* output,
                                             const Webserver::WebRequest& req,
                                             HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  PrintTableObject(output, table_id, status_code);
  *status_code = HttpStatusCode::Ok;
}

void RestCatalogPathHandlers::HandlePutTable(ostringstream* output,
                                             const Webserver::WebRequest& req,
                                             HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  AlterTableRequestPB request;
  AlterTableResponsePB response;
  request.mutable_table()->set_table_id(table_id);
  JsonWriter jw(output, JsonWriter::COMPACT);

  const string& json_str = req.post_data;
  JsonParseOptions opts;
  opts.case_insensitive_enum_parsing = true;
  const auto s = JsonStringToMessage(json_str, &request, opts);

  if (!s.ok()) {
    RETURN_JSON_ERROR(jw,
                      Substitute("JSON table object is not correct: $0", json_str),
                      *status_code,
                      HttpStatusCode::BadRequest);
  }

  optional<string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->AlterTableWithUser(request, &response, user);

  if (!status.ok()) {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
  }

  IsAlterTableDoneRequestPB check_req;
  IsAlterTableDoneResponsePB check_resp;
  check_req.mutable_table()->set_table_id(table_id);
  MonoTime deadline =
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_rest_catalog_default_request_timeout_ms);

  while (MonoTime::Now() < deadline) {
    status = master_->catalog_manager()->IsAlterTableDone(&check_req, &check_resp, user);

    if (!status.ok()) {
      RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
    }

    if (check_resp.has_error()) {
      RETURN_JSON_ERROR(jw,
                        check_resp.error().ShortDebugString(),
                        *status_code,
                        HttpStatusCode::InternalServerError);
    }

    if (check_resp.done()) {
      PrintTableObject(output, table_id, status_code);
      *status_code = HttpStatusCode::Ok;
      return;
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
  RETURN_JSON_ERROR(jw,
                    "Alter table timed out while waiting for operation completion",
                    *status_code,
                    HttpStatusCode::InternalServerError);
}

void RestCatalogPathHandlers::HandleDeleteTable(ostringstream* output,
                                                const Webserver::WebRequest& req,
                                                HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  DeleteTableRequestPB request;
  DeleteTableResponsePB response;
  request.mutable_table()->set_table_id(table_id);
  JsonWriter jw(output, JsonWriter::COMPACT);
  optional<string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->DeleteTableWithUser(request, &response, user);

  if (status.ok()) {
    *status_code = HttpStatusCode::NoContent;
  } else {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
  }
}

void RestCatalogPathHandlers::PrintTableObject(ostringstream* output,
                                               const string& table_id,
                                               HttpStatusCode* status_code) {
  scoped_refptr<TableInfo> table;
  Status status = master_->catalog_manager()->GetTableInfo(table_id, &table);
  JsonWriter jw(output, JsonWriter::COMPACT);
  if (!status.ok()) {
    RETURN_JSON_ERROR_FROM_STATUS(jw, status, *status_code);
  }

  jw.StartObject();
  {
    TableMetadataLock l(table.get(), LockMode::READ);
    jw.String("name");
    jw.String(l.data().name());
    jw.String("id");
    jw.String(table_id);
    jw.String("schema");
    jw.Protobuf(l.data().pb.schema());
    jw.String("partition_schema");
    jw.Protobuf(l.data().pb.partition_schema());
    jw.String("owner");
    jw.String(l.data().owner());
    jw.String("comment");
    jw.String(l.data().comment());
    jw.String("extra_config");
    jw.Protobuf(l.data().pb.extra_config());
  }
  jw.EndObject();
}

void RestCatalogPathHandlers::Register(Webserver* server) {
  server->RegisterPrerenderedPathHandler(
      "/api/v1/tables/<table_id>",
      "",
      [this](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        this->HandleApiTableEndpoint(req, resp);
      },
      StyleMode::JSON,
      false);
  server->RegisterPrerenderedPathHandler(
      "/api/v1/tables",
      "",
      [this](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        this->HandleApiTablesEndpoint(req, resp);
      },
      StyleMode::JSON,
      false);
}

}  // namespace master
}  // namespace kudu

#undef RETURN_JSON_ERROR
#undef RETURN_JSON_ERROR_VAL
