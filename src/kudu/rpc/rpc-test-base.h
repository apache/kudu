// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_RPC_TEST_BASE_H
#define KUDU_RPC_RPC_TEST_BASE_H

#include <boost/thread/thread.hpp>
#include <algorithm>
#include <list>
#include <string>

#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/rtest.service.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

namespace kudu { namespace rpc {

using kudu::rpc_test::AddRequestPB;
using kudu::rpc_test::AddRequestPartialPB;
using kudu::rpc_test::AddResponsePB;
using kudu::rpc_test::EchoRequestPB;
using kudu::rpc_test::EchoResponsePB;
using kudu::rpc_test::CalculatorError;
using kudu::rpc_test::CalculatorServiceIf;
using kudu::rpc_test::CalculatorServiceProxy;
using kudu::rpc_test::PanicRequestPB;
using kudu::rpc_test::PanicResponsePB;
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

  virtual void Handle(InboundCall *incoming) OVERRIDE {
    if (incoming->method_name() == kAddMethodName) {
      DoAdd(incoming);
    } else if (incoming->method_name() == kSleepMethodName) {
      DoSleep(incoming);
    } else {
      incoming->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                               Status::InvalidArgument("bad method"));
    }
  }

  std::string service_name() const OVERRIDE { return kFullServiceName; }
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
                   RpcContext *context) OVERRIDE {
    resp->set_result(req->x() + req->y());
    context->RespondSuccess();
  }

  virtual void Sleep(const SleepRequestPB *req,
                     SleepResponsePB *resp,
                     RpcContext *context) OVERRIDE {
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
                    RpcContext *context) OVERRIDE {
    resp->set_data(req->data());
    context->RespondSuccess();
  }

  virtual void WhoAmI(const WhoAmIRequestPB* req,
                      WhoAmIResponsePB* resp,
                      RpcContext* context) OVERRIDE {
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
                                          ::kudu::rpc::RpcContext *context) OVERRIDE {
    context->RespondSuccess();
  }

  virtual void Panic(const PanicRequestPB* req,
                     PanicResponsePB* resp,
                     RpcContext* context) OVERRIDE {
    TRACE("Got panic request");
    PANIC_RPC(context, "Test method panicking!");
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

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    //alarm(60);
  }

  virtual void TearDown() OVERRIDE {
    if (service_pool_) {
      server_messenger_->UnregisterService(service_name_);
      service_pool_->Shutdown();
    }
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

    int expected_millis = timeout.ToMilliseconds();
    int elapsed_millis = sw.elapsed().wall_millis();

    // We shouldn't timeout significantly faster than our configured timeout.
    EXPECT_GE(elapsed_millis, expected_millis - 10);
    // And we also shouldn't take the full 0.5sec that we asked for
    EXPECT_LT(elapsed_millis, 500);
    EXPECT_TRUE(s.IsTimedOut());
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

    gscoped_ptr<ServiceIf> service(new ServiceClass(metric_ctx_));
    service_name_ = service->service_name();
    const MetricContext& metric_ctx = *server_messenger_->metric_context();
    service_pool_ = new ServicePool(service.Pass(), metric_ctx, 50);
    server_messenger_->RegisterService(service_name_, service_pool_);
    ASSERT_STATUS_OK(service_pool_->Init(n_worker_threads_));
  }

 protected:
  string service_name_;
  shared_ptr<Messenger> server_messenger_;
  scoped_refptr<ServicePool> service_pool_;
  int n_worker_threads_;
  int n_server_reactor_threads_;
  int keepalive_time_ms_;

  MetricRegistry metric_registry_;
  MetricContext metric_ctx_;
};


} // namespace rpc
} // namespace kudu
#endif
