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
#ifndef KUDU_MASTER_MASTER_SERVICE_H
#define KUDU_MASTER_MASTER_SERVICE_H

#include <cstdint>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/master/master.service.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

namespace rpc {
class RpcContext;
}

namespace master {

class AlterTableRequestPB;
class AlterTableResponsePB;
class ConnectToMasterRequestPB;
class ConnectToMasterResponsePB;
class CreateTableRequestPB;
class CreateTableResponsePB;
class DeleteTableRequestPB;
class DeleteTableResponsePB;
class GetMasterRegistrationRequestPB;
class GetMasterRegistrationResponsePB;
class GetTableLocationsRequestPB;
class GetTableLocationsResponsePB;
class GetTableSchemaRequestPB;
class GetTableSchemaResponsePB;
class GetTabletLocationsRequestPB;
class GetTabletLocationsResponsePB;
class IsAlterTableDoneRequestPB;
class IsAlterTableDoneResponsePB;
class IsCreateTableDoneRequestPB;
class IsCreateTableDoneResponsePB;
class ListMastersRequestPB;
class ListMastersResponsePB;
class ListTablesRequestPB;
class ListTablesResponsePB;
class ListTabletServersRequestPB;
class ListTabletServersResponsePB;
class Master;
class PingRequestPB;
class PingResponsePB;
class ReplaceTabletRequestPB;
class ReplaceTabletResponsePB;
class TSHeartbeatRequestPB;
class TSHeartbeatResponsePB;

// Implementation of the master service. See master.proto for docs
// on each RPC.
class MasterServiceImpl : public MasterServiceIf {
 public:
  explicit MasterServiceImpl(Master* server);

  // Authorize an RPC call which must be from a client.
  bool AuthorizeClient(const google::protobuf::Message* req,
                       google::protobuf::Message* resp,
                       rpc::RpcContext *context) override;

  // Authorize an RPC call which must be from within the Kudu service.
  bool AuthorizeService(const google::protobuf::Message* req,
                        google::protobuf::Message* resp,
                        rpc::RpcContext *context) override;

  bool AuthorizeClientOrService(const google::protobuf::Message* req,
                                google::protobuf::Message* resp,
                                rpc::RpcContext *context) override;

  bool AuthorizeSuperUser(const google::protobuf::Message* req,
                          google::protobuf::Message* resp,
                          rpc::RpcContext* context) override;

  virtual void Ping(const PingRequestPB* req,
                    PingResponsePB* resp,
                    rpc::RpcContext* rpc) OVERRIDE;

  virtual void TSHeartbeat(const TSHeartbeatRequestPB* req,
                           TSHeartbeatResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;

  virtual void GetTabletLocations(const GetTabletLocationsRequestPB* req,
                                  GetTabletLocationsResponsePB* resp,
                                  rpc::RpcContext* rpc) OVERRIDE;

  virtual void CreateTable(const CreateTableRequestPB* req,
                           CreateTableResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;
  virtual void IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                 IsCreateTableDoneResponsePB* resp,
                                 rpc::RpcContext* rpc) OVERRIDE;
  virtual void DeleteTable(const DeleteTableRequestPB* req,
                           DeleteTableResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;
  virtual void AlterTable(const AlterTableRequestPB* req,
                           AlterTableResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;
  virtual void IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                IsAlterTableDoneResponsePB* resp,
                                rpc::RpcContext* rpc) OVERRIDE;
  virtual void ListTables(const ListTablesRequestPB* req,
                          ListTablesResponsePB* resp,
                          rpc::RpcContext* rpc) OVERRIDE;
  virtual void GetTableLocations(const GetTableLocationsRequestPB* req,
                                 GetTableLocationsResponsePB* resp,
                                 rpc::RpcContext* rpc) OVERRIDE;
  virtual void GetTableSchema(const GetTableSchemaRequestPB* req,
                              GetTableSchemaResponsePB* resp,
                              rpc::RpcContext* rpc) OVERRIDE;
  virtual void ListTabletServers(const ListTabletServersRequestPB* req,
                                 ListTabletServersResponsePB* resp,
                                 rpc::RpcContext* rpc) OVERRIDE;

  virtual void ListMasters(const ListMastersRequestPB* req,
                           ListMastersResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;

  virtual void GetMasterRegistration(const GetMasterRegistrationRequestPB* req,
                                     GetMasterRegistrationResponsePB* resp,
                                     rpc::RpcContext* rpc) OVERRIDE;

  virtual void ConnectToMaster(const ConnectToMasterRequestPB* req,
                               ConnectToMasterResponsePB* resp,
                               rpc::RpcContext* rpc) OVERRIDE;

  virtual void ReplaceTablet(const ReplaceTabletRequestPB* req,
                             ReplaceTabletResponsePB* resp,
                             rpc::RpcContext* rpc) OVERRIDE;

  bool SupportsFeature(uint32_t feature) const override;

 private:
  Master* server_;

  DISALLOW_COPY_AND_ASSIGN(MasterServiceImpl);
};

} // namespace master
} // namespace kudu

#endif
