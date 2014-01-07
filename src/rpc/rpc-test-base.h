// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_RPC_TEST_BASE_H
#define KUDU_RPC_RPC_TEST_BASE_H

#include <algorithm>
#include <list>
#include <string>

#include "rpc/acceptor_pool.h"
#include "rpc/messenger.h"
#include "rpc/proxy.h"
#include "rpc/reactor.h"
#include "rpc/rtest.pb.h"
#include "rpc/rtest.proxy.h"
#include "rpc/rtest.service.h"
#include "rpc/rpc_context.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "util/net/sockaddr.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

namespace kudu { namespace rpc {

using kudu::rpc_test::AddRequestPB;
using kudu::rpc_test::AddRequestPartialPB;
using kudu::rpc_test::AddResponsePB;
using kudu::rpc_test::EchoRequestPB;
using kudu::rpc_test::EchoResponsePB;
using kudu::rpc_test::CalculatorError;
using kudu::rpc_test::CalculatorServiceIf;
using kudu::rpc_test::CalculatorServiceProxy;
using kudu::rpc_test::SleepRequestPB;
using kudu::rpc_test::SleepResponsePB;
using kudu::rpc_test::WhoAmIRequestPB;
using kudu::rpc_test::WhoAmIResponsePB;
using kudu::rpc_test_diff_package::ReqDiffPackagePB;
using kudu::rpc_test_diff_package::RespDiffPackagePB;

using std::tr1::shared_ptr;

// Implementation of CalculatorService which just implements the generic
// RPC handler (no generated code).
class GenericCalculatorService : public ServiceIf {
 public:
  static const char *kFullServiceName;
  static const char *kAddMethodName;
  static const char *kSleepMethodName;

  GenericCalculatorService() {
  }

  // To match the argument list of the generated CalculatorService.
  explicit GenericCalculatorService(const MetricContext& ctx) {
    // TODO: use the metrics context if needed later...
  }

  virtual void Handle(InboundCall *incoming) {
    if (incoming->method_name() == kAddMethodName) {
      DoAdd(incoming);
    } else if (incoming->method_name() == kSleepMethodName) {
      DoSleep(incoming);
    } else {
      incoming->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                               Status::InvalidArgument("bad method"));
    }
  }

  std::string service_name() const { return kFullServiceName; }
  static std::string static_service_name() { return kFullServiceName; }

 private:
  void DoAdd(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    AddRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
      // TODO: respond error
    }

    AddResponsePB resp;
    resp.set_result(req.x() + req.y());
    incoming->RespondSuccess(resp);
  }

  void DoSleep(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    SleepRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      incoming->RespondFailure(ErrorStatusPB::ERROR_INVALID_REQUEST,
        Status::InvalidArgument("Couldn't parse pb",
                                req.InitializationErrorString()));
      return;
    }

    LOG(INFO) << "got call: " << req.ShortDebugString();
    usleep(req.sleep_micros());
    SleepResponsePB resp;
    incoming->RespondSuccess(resp);
  }
};

class CalculatorService : public CalculatorServiceIf {
 public:
  explicit CalculatorService(const MetricContext& ctx)
    : CalculatorServiceIf(ctx) {
  }

  virtual void Add(const AddRequestPB *req,
                   AddResponsePB *resp,
                   RpcContext *context) {
    resp->set_result(req->x() + req->y());
    context->RespondSuccess();
  }

  virtual void Sleep(const SleepRequestPB *req,
                     SleepResponsePB *resp,
                     RpcContext *context) {
    if (req->return_app_error()) {
      CalculatorError my_error;
      my_error.set_extra_error_data("some application-specific error data");
      context->RespondApplicationError(CalculatorError::app_error_ext.number(),
                                       "Got some error", my_error);
      return;
    }

    if (req->deferred()) {
      // Spawn a new thread which does the sleep and responds later.
      boost::thread new_thr(&CalculatorService::DoSleep, this, req, context);
      return;
    }
    DoSleep(req, context);
  }

  virtual void Echo(const EchoRequestPB *req,
                    EchoResponsePB *resp,
                    RpcContext *context) {
    resp->set_data(req->data());
    context->RespondSuccess();
  }

  virtual void WhoAmI(const WhoAmIRequestPB* req,
                      WhoAmIResponsePB* resp,
                      RpcContext* context) {
    const UserCredentials& creds = context->user_credentials();
    if (creds.has_effective_user()) {
      resp->mutable_credentials()->set_effective_user(creds.effective_user());
    }
    resp->mutable_credentials()->set_real_user(creds.real_user());
    resp->set_address(context->remote_address().ToString());
    context->RespondSuccess();
  }

  virtual void TestArgumentsInDiffPackage(const ReqDiffPackagePB *req,
                                          RespDiffPackagePB *resp,
                                          ::kudu::rpc::RpcContext *context) {
    context->RespondSuccess();
  }

 private:
  void DoSleep(const SleepRequestPB *req,
               RpcContext *context) {
    usleep(req->sleep_micros());
    context->RespondSuccess();
  }

};

const char *GenericCalculatorService::kFullServiceName = "kudu.rpc.GenericCalculatorService";
const char *GenericCalculatorService::kAddMethodName = "Add";
const char *GenericCalculatorService::kSleepMethodName = "Sleep";

class RpcTestBase : public KuduTest {
 public:
  RpcTestBase()
    : n_worker_threads_(3),
      n_server_reactor_threads_(3),
      keepalive_time_ms_(1000),
      metric_ctx_(&metric_registry_, "test.rpc_test") {
  }

  virtual void SetUp() {
    KuduTest::SetUp();
    //alarm(60);
  }

  virtual void TearDown() {
    if (server_messenger_) {
      server_messenger_->Shutdown();
    }
    alarm(0);
    KuduTest::TearDown();
  }

 protected:
  shared_ptr<Messenger> CreateMessenger(const string &name,
                                        int n_reactors = 1) {
    MessengerBuilder bld(name);
    bld.set_num_reactors(n_reactors);
    bld.set_connection_keepalive_time(
      MonoDelta::FromMilliseconds(keepalive_time_ms_));
    bld.set_coarse_timer_granularity(MonoDelta::FromMilliseconds(
                                       std::min(keepalive_time_ms_, 100)));
    bld.set_metric_context(metric_ctx_);
    shared_ptr<Messenger> messenger;
    CHECK_OK(bld.Build(&messenger));
    return messenger;
  }

  Status DoTestSyncCall(const Proxy &p, const char *method) {
    AddRequestPB req;
    req.set_x(rand());
    req.set_y(rand());
    AddResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    RETURN_NOT_OK(p.SyncRequest(method, req, &resp, &controller));

    LOG(INFO) << "Result: " << resp.ShortDebugString();
    CHECK_EQ(req.x() + req.y(), resp.result());
    return Status::OK();
  }

  void DoTestExpectTimeout(const Proxy &p, const MonoDelta &timeout) {
    SleepRequestPB req;
    SleepResponsePB resp;
    req.set_sleep_micros(500000); // 0.5sec

    RpcController c;
    c.set_timeout(timeout);
    Stopwatch sw;
    sw.start();
    Status s = p.SyncRequest(GenericCalculatorService::kSleepMethodName, req, &resp, &c);
    ASSERT_FALSE(s.ok());
    sw.stop();
    ASSERT_LT(sw.elapsed().wall_seconds(), 0.100); // this arbitrary max can make tests flaky...
    LOG(INFO) << "status: " << s.ToString() << ", seconds elapsed: " << sw.elapsed().wall_seconds();
  }

  void StartTestServer(Sockaddr *server_addr) {
    DoStartTestServer<GenericCalculatorService>(server_addr);
  }

  void StartTestServerWithGeneratedCode(Sockaddr *server_addr) {
    DoStartTestServer<CalculatorService>(server_addr);
  }

  // Start a simple socket listening on a local port, returning the address.
  // This isn't an RPC server -- just a plain socket which can be helpful for testing.
  Status StartFakeServer(Socket *listen_sock, Sockaddr *listen_addr) {
    Sockaddr bind_addr;
    bind_addr.set_port(0);
    RETURN_NOT_OK(listen_sock->Init(0));
    RETURN_NOT_OK(listen_sock->BindAndListen(bind_addr, 1));
    RETURN_NOT_OK(listen_sock->GetSocketAddress(listen_addr));
    LOG(INFO) << "Bound to: " << listen_addr->ToString();
    return Status::OK();
  }

 private:

  template<class ServiceClass>
  void DoStartTestServer(Sockaddr *server_addr) {
    server_messenger_ = CreateMessenger("Server", n_server_reactor_threads_);
    shared_ptr<AcceptorPool> pool;
    ASSERT_STATUS_OK(server_messenger_->AddAcceptorPool(Sockaddr(), 2, &pool));
    *server_addr = pool->bind_address();

    gscoped_ptr<ServiceIf> impl(new ServiceClass(metric_ctx_));
    worker_pool_.reset(new ServicePool(server_messenger_, impl.Pass()));
    ASSERT_STATUS_OK(worker_pool_->Init(n_worker_threads_));
  }

 protected:
  shared_ptr<Messenger> server_messenger_;
  gscoped_ptr<ServicePool> worker_pool_;
  int n_worker_threads_;
  int n_server_reactor_threads_;
  int keepalive_time_ms_;

  MetricRegistry metric_registry_;
  MetricContext metric_ctx_;
};


} // namespace rpc
} // namespace kudu
#endif
