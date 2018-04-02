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
#ifndef KUDU_SERVER_GENERIC_SERVICE_H
#define KUDU_SERVER_GENERIC_SERVICE_H

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/server/server_base.service.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

namespace rpc {
class RpcContext;
}

namespace server {

class CheckLeaksRequestPB;
class CheckLeaksResponsePB;
class FlushCoverageRequestPB;
class FlushCoverageResponsePB;
class GetFlagsRequestPB;
class GetFlagsResponsePB;
class GetStatusRequestPB;
class GetStatusResponsePB;
class ServerBase;
class ServerClockRequestPB;
class ServerClockResponsePB;
class SetFlagRequestPB;
class SetFlagResponsePB;
class SetServerWallClockForTestsRequestPB;
class SetServerWallClockForTestsResponsePB;

class GenericServiceImpl : public GenericServiceIf {
 public:
  explicit GenericServiceImpl(ServerBase* server);
  virtual ~GenericServiceImpl();

  bool AuthorizeSuperUser(const google::protobuf::Message* req,
                          google::protobuf::Message* resp,
                          rpc::RpcContext* rpc) override;

  bool AuthorizeClient(const google::protobuf::Message* req,
                       google::protobuf::Message* resp,
                       rpc::RpcContext* rpc) override;

  virtual void GetFlags(const GetFlagsRequestPB* req,
                        GetFlagsResponsePB* resp,
                        rpc::RpcContext* rpc) OVERRIDE;

  virtual void SetFlag(const SetFlagRequestPB* req,
                       SetFlagResponsePB* resp,
                       rpc::RpcContext* rpc) OVERRIDE;

  virtual void FlushCoverage(const FlushCoverageRequestPB* req,
                             FlushCoverageResponsePB* resp,
                             rpc::RpcContext* rpc) OVERRIDE;

  virtual void CheckLeaks(const CheckLeaksRequestPB* req,
                          CheckLeaksResponsePB* resp,
                          rpc::RpcContext* rpc) OVERRIDE;

  virtual void ServerClock(const ServerClockRequestPB* req,
                           ServerClockResponsePB* resp,
                           rpc::RpcContext* rpc) OVERRIDE;

  virtual void SetServerWallClockForTests(const SetServerWallClockForTestsRequestPB *req,
                                          SetServerWallClockForTestsResponsePB *resp,
                                          rpc::RpcContext *context) OVERRIDE;

  virtual void GetStatus(const GetStatusRequestPB* req,
                         GetStatusResponsePB* resp,
                         rpc::RpcContext* rpc) OVERRIDE;
 private:
  ServerBase* server_;

  DISALLOW_COPY_AND_ASSIGN(GenericServiceImpl);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_GENERIC_SERVICE_H */
