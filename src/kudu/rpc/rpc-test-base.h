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
#ifndef KUDU_RPC_RPC_TEST_BASE_H
#define KUDU_RPC_RPC_TEST_BASE_H

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <string>

#include "kudu/gutil/walltime.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/rtest.service.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/security-test-util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

DECLARE_string(rpc_ssl_server_certificate);
DECLARE_string(rpc_ssl_private_key);
DECLARE_string(rpc_ssl_certificate_authority);

namespace kudu { namespace rpc {

using kudu::rpc_test::AddRequestPB;
using kudu::rpc_test::AddRequestPartialPB;
using kudu::rpc_test::AddResponsePB;
using kudu::rpc_test::CalculatorError;
using kudu::rpc_test::CalculatorServiceIf;
using kudu::rpc_test::CalculatorServiceProxy;
using kudu::rpc_test::EchoRequestPB;
using kudu::rpc_test::EchoResponsePB;
using kudu::rpc_test::ExactlyOnceRequestPB;
using kudu::rpc_test::ExactlyOnceResponsePB;
using kudu::rpc_test::FeatureFlags;
using kudu::rpc_test::PanicRequestPB;
using kudu::rpc_test::PanicResponsePB;
using kudu::rpc_test::SendTwoStringsRequestPB;
using kudu::rpc_test::SendTwoStringsResponsePB;
using kudu::rpc_test::SleepRequestPB;
using kudu::rpc_test::SleepResponsePB;
using kudu::rpc_test::TestInvalidResponseRequestPB;
using kudu::rpc_test::TestInvalidResponseResponsePB;
using kudu::rpc_test::WhoAmIRequestPB;
using kudu::rpc_test::WhoAmIResponsePB;
using kudu::rpc_test_diff_package::ReqDiffPackagePB;
using kudu::rpc_test_diff_package::RespDiffPackagePB;

// Implementation of CalculatorService which just implements the generic
// RPC handler (no generated code).
class GenericCalculatorService : public ServiceIf {
 public:
  static const char *kFullServiceName;
  static const char *kAddMethodName;
  static const char *kSleepMethodName;
  static const char *kSendTwoStringsMethodName;
  static const char *kAddExactlyOnce;

  static const char* kFirstString;
  static const char* kSecondString;

  GenericCalculatorService() {
  }

  // To match the argument list of the generated CalculatorService.
  explicit GenericCalculatorService(const scoped_refptr<MetricEntity>& entity,
                                    const scoped_refptr<ResultTracker>& result_tracker) {
    // this test doesn't generate metrics, so we ignore the argument.
  }

  void Handle(InboundCall *incoming) override {
    if (incoming->remote_method().method_name() == kAddMethodName) {
      DoAdd(incoming);
    } else if (incoming->remote_method().method_name() == kSleepMethodName) {
      DoSleep(incoming);
    } else if (incoming->remote_method().method_name() == kSendTwoStringsMethodName) {
      DoSendTwoStrings(incoming);
    } else {
      incoming->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                               Status::InvalidArgument("bad method"));
    }
  }

  std::string service_name() const override { return kFullServiceName; }
  static std::string static_service_name() { return kFullServiceName; }

 private:
  void DoAdd(InboundCall *incoming) {
    Slice param(incoming->serialized_request());
    AddRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
    }

    AddResponsePB resp;
    resp.set_result(req.x() + req.y());
    incoming->RespondSuccess(resp);
  }

  void DoSendTwoStrings(InboundCall* incoming) {
    Slice param(incoming->serialized_request());
    SendTwoStringsRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.ToDebugString();
    }

    gscoped_ptr<faststring> first(new faststring);
    gscoped_ptr<faststring> second(new faststring);

    Random r(req.random_seed());
    first->resize(req.size1());
    RandomString(first->data(), req.size1(), &r);

    second->resize(req.size2());
    RandomString(second->data(), req.size2(), &r);

    SendTwoStringsResponsePB resp;
    int idx1, idx2;
    CHECK_OK(incoming->AddRpcSidecar(
        make_gscoped_ptr(new RpcSidecar(std::move(first))), &idx1));
    CHECK_OK(incoming->AddRpcSidecar(
        make_gscoped_ptr(new RpcSidecar(std::move(second))), &idx2));
    resp.set_sidecar1(idx1);
    resp.set_sidecar2(idx2);

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

    LOG(INFO) << "got call: " << SecureShortDebugString(req);
    SleepFor(MonoDelta::FromMicroseconds(req.sleep_micros()));
    SleepResponsePB resp;
    incoming->RespondSuccess(resp);
  }
};

class CalculatorService : public CalculatorServiceIf {
 public:
  explicit CalculatorService(const scoped_refptr<MetricEntity>& entity,
                             const scoped_refptr<ResultTracker> result_tracker)
    : CalculatorServiceIf(entity, result_tracker),
      exactly_once_test_val_(0) {
  }

  void Add(const AddRequestPB *req, AddResponsePB *resp, RpcContext *context) override {
    resp->set_result(req->x() + req->y());
    context->RespondSuccess();
  }

  void Sleep(const SleepRequestPB *req, SleepResponsePB *resp, RpcContext *context) override {
    if (req->return_app_error()) {
      CalculatorError my_error;
      my_error.set_extra_error_data("some application-specific error data");
      context->RespondApplicationError(CalculatorError::app_error_ext.number(),
                                       "Got some error", my_error);
      return;
    }

    // Respond w/ error if the RPC specifies that the client deadline is set,
    // but it isn't.
    if (req->client_timeout_defined()) {
      MonoTime deadline = context->GetClientDeadline();
      if (deadline == MonoTime::Max()) {
        CalculatorError my_error;
        my_error.set_extra_error_data("Timeout not set");
        context->RespondApplicationError(CalculatorError::app_error_ext.number(),
                                        "Missing required timeout", my_error);
        return;
      }
    }

    if (req->deferred()) {
      // Spawn a new thread which does the sleep and responds later.
      scoped_refptr<Thread> thread;
      CHECK_OK(Thread::Create("rpc-test", "deferred",
                              &CalculatorService::DoSleep, this, req, context,
                              &thread));
      return;
    }
    DoSleep(req, context);
  }

  void Echo(const EchoRequestPB *req, EchoResponsePB *resp, RpcContext *context) override {
    resp->set_data(req->data());
    context->RespondSuccess();
  }

  void WhoAmI(const WhoAmIRequestPB* /*req*/,
              WhoAmIResponsePB* resp,
              RpcContext* context) override {
    const UserCredentials& creds = context->user_credentials();
    resp->mutable_credentials()->set_real_user(creds.real_user());
    resp->set_address(context->remote_address().ToString());
    context->RespondSuccess();
  }

  void TestArgumentsInDiffPackage(const ReqDiffPackagePB *req,
                                  RespDiffPackagePB *resp,
                                  ::kudu::rpc::RpcContext *context) override {
    context->RespondSuccess();
  }

  void Panic(const PanicRequestPB* req, PanicResponsePB* resp, RpcContext* context) override {
    TRACE("Got panic request");
    PANIC_RPC(context, "Test method panicking!");
  }

  void TestInvalidResponse(const TestInvalidResponseRequestPB* req,
                           TestInvalidResponseResponsePB* resp,
                           RpcContext* context) override {
    switch (req->error_type()) {
      case rpc_test::TestInvalidResponseRequestPB_ErrorType_MISSING_REQUIRED_FIELD:
        // Respond without setting the 'resp->response' protobuf field, which is
        // marked as required. This exercises the error path of invalid responses.
        context->RespondSuccess();
        break;
      case rpc_test::TestInvalidResponseRequestPB_ErrorType_RESPONSE_TOO_LARGE:
        resp->mutable_response()->resize(FLAGS_rpc_max_message_size + 1000);
        context->RespondSuccess();
        break;
      default:
        LOG(FATAL);
    }
  }

  bool SupportsFeature(uint32_t feature) const override {
    return feature == FeatureFlags::FOO;
  }

  void AddExactlyOnce(const ExactlyOnceRequestPB* req, ExactlyOnceResponsePB* resp,
                      ::kudu::rpc::RpcContext* context) override {
    if (req->sleep_for_ms() > 0) {
      usleep(req->sleep_for_ms() * 1000);
    }
    // If failures are enabled, cause them some percentage of the time.
    if (req->randomly_fail()) {
      if (rand() % 10 < 3) {
        context->RespondFailure(Status::ServiceUnavailable("Random injected failure."));
        return;
      }
    }
    int result = exactly_once_test_val_ += req->value_to_add();
    resp->set_current_val(result);
    resp->set_current_time_micros(GetCurrentTimeMicros());
    context->RespondSuccess();
  }

  bool AuthorizeDisallowAlice(const google::protobuf::Message* /*req*/,
                              google::protobuf::Message* /*resp*/,
                              RpcContext* context) override {
    if (context->user_credentials().real_user() == "alice") {
      context->RespondFailure(Status::NotAuthorized("alice is not allowed to call this method"));
      return false;
    }
    return true;
  }

  bool AuthorizeDisallowBob(const google::protobuf::Message* /*req*/,
                            google::protobuf::Message* /*resp*/,
                            RpcContext* context) override {
    if (context->user_credentials().real_user() == "bob") {
      context->RespondFailure(Status::NotAuthorized("bob is not allowed to call this method"));
      return false;
    }
    return true;
  }

 private:
  void DoSleep(const SleepRequestPB *req,
               RpcContext *context) {
    TRACE_COUNTER_INCREMENT("test_sleep_us", req->sleep_micros());
    if (Trace::CurrentTrace()) {
      scoped_refptr<Trace> child_trace(new Trace());
      Trace::CurrentTrace()->AddChildTrace("test_child", child_trace.get());
      ADOPT_TRACE(child_trace.get());
      TRACE_COUNTER_INCREMENT("related_trace_metric", 1);
    }

    SleepFor(MonoDelta::FromMicroseconds(req->sleep_micros()));
    context->RespondSuccess();
  }

  std::atomic_int exactly_once_test_val_;

};

const char *GenericCalculatorService::kFullServiceName = "kudu.rpc.GenericCalculatorService";
const char *GenericCalculatorService::kAddMethodName = "Add";
const char *GenericCalculatorService::kSleepMethodName = "Sleep";
const char *GenericCalculatorService::kSendTwoStringsMethodName = "SendTwoStrings";
const char *GenericCalculatorService::kAddExactlyOnce = "AddExactlyOnce";

const char *GenericCalculatorService::kFirstString =
    "1111111111111111111111111111111111111111111111111111111111";
const char *GenericCalculatorService::kSecondString =
    "2222222222222222222222222222222222222222222222222222222222222222222222";

class RpcTestBase : public KuduTest {
 public:
  RpcTestBase()
    : n_worker_threads_(3),
      service_queue_length_(100),
      n_server_reactor_threads_(3),
      keepalive_time_ms_(1000),
      metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_, "test.rpc_test")) {
  }

  void SetUp() override {
    KuduTest::SetUp();
  }

  void TearDown() override {
    if (service_pool_) {
      server_messenger_->UnregisterService(service_name_);
      service_pool_->Shutdown();
    }
    if (server_messenger_) {
      server_messenger_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  std::shared_ptr<Messenger> CreateMessenger(const string &name,
                                             int n_reactors = 1,
                                             bool enable_ssl = false) {
    if (enable_ssl) {
      std::string server_cert_path = GetTestPath("server-cert.pem");
      std::string private_key_path = GetTestPath("server-key.pem");
      CHECK_OK(security::CreateSSLServerCert(server_cert_path));
      CHECK_OK(security::CreateSSLPrivateKey(private_key_path));
      FLAGS_rpc_ssl_server_certificate = server_cert_path;
      FLAGS_rpc_ssl_private_key = private_key_path;
      FLAGS_rpc_ssl_certificate_authority = server_cert_path;
    }
    MessengerBuilder bld(name);
    bld.set_num_reactors(n_reactors);
    bld.set_connection_keepalive_time(
      MonoDelta::FromMilliseconds(keepalive_time_ms_));
    // In order for the keepalive timing to be accurate, we need to scan connections
    // significantly more frequently than the keepalive time. This "coarse timer"
    // granularity determines this.
    bld.set_coarse_timer_granularity(MonoDelta::FromMilliseconds(
                                       std::min(keepalive_time_ms_ / 5, 100)));
    bld.set_metric_entity(metric_entity_);
    std::shared_ptr<Messenger> messenger;
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

    CHECK_EQ(req.x() + req.y(), resp.result());
    return Status::OK();
  }

  void DoTestSidecar(const Proxy &p, int size1, int size2) {
    const uint32_t kSeed = 12345;

    SendTwoStringsRequestPB req;
    req.set_size1(size1);
    req.set_size2(size2);
    req.set_random_seed(kSeed);

    SendTwoStringsResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    CHECK_OK(p.SyncRequest(GenericCalculatorService::kSendTwoStringsMethodName,
                           req, &resp, &controller));

    Slice first = GetSidecarPointer(controller, resp.sidecar1(), size1);
    Slice second = GetSidecarPointer(controller, resp.sidecar2(), size2);

    Random rng(kSeed);
    faststring expected;

    expected.resize(size1);
    RandomString(expected.data(), size1, &rng);
    CHECK_EQ(0, first.compare(Slice(expected)));

    expected.resize(size2);
    RandomString(expected.data(), size2, &rng);
    CHECK_EQ(0, second.compare(Slice(expected)));
  }

  void DoTestExpectTimeout(const Proxy &p, const MonoDelta &timeout) {
    SleepRequestPB req;
    SleepResponsePB resp;
    // Sleep for 500ms longer than the call timeout.
    int sleep_micros = timeout.ToMicroseconds() + 500 * 1000;
    req.set_sleep_micros(sleep_micros);

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
    // And we also shouldn't take the full time that we asked for
    EXPECT_LT(elapsed_millis * 1000, sleep_micros);
    EXPECT_TRUE(s.IsTimedOut());
    LOG(INFO) << "status: " << s.ToString() << ", seconds elapsed: " << sw.elapsed().wall_seconds();
  }

  void StartTestServer(Sockaddr *server_addr, bool enable_ssl = false) {
    DoStartTestServer<GenericCalculatorService>(server_addr, enable_ssl);
  }

  void StartTestServerWithGeneratedCode(Sockaddr *server_addr, bool enable_ssl = false) {
    DoStartTestServer<CalculatorService>(server_addr, enable_ssl);
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

  static Slice GetSidecarPointer(const RpcController& controller, int idx,
                                 int expected_size) {
    Slice sidecar;
    CHECK_OK(controller.GetSidecar(idx, &sidecar));
    CHECK_EQ(expected_size, sidecar.size());
    return Slice(sidecar.data(), expected_size);
  }

  template<class ServiceClass>
  void DoStartTestServer(Sockaddr *server_addr, bool enable_ssl = false) {
    server_messenger_ = CreateMessenger("TestServer", n_server_reactor_threads_, enable_ssl);
    std::shared_ptr<AcceptorPool> pool;
    ASSERT_OK(server_messenger_->AddAcceptorPool(Sockaddr(), &pool));
    ASSERT_OK(pool->Start(2));
    *server_addr = pool->bind_address();
    mem_tracker_ = MemTracker::CreateTracker(-1, "result_tracker");
    result_tracker_.reset(new ResultTracker(mem_tracker_));

    gscoped_ptr<ServiceIf> service(new ServiceClass(metric_entity_, result_tracker_));
    service_name_ = service->service_name();
    scoped_refptr<MetricEntity> metric_entity = server_messenger_->metric_entity();
    service_pool_ = new ServicePool(std::move(service), metric_entity, service_queue_length_);
    server_messenger_->RegisterService(service_name_, service_pool_);
    ASSERT_OK(service_pool_->Init(n_worker_threads_));
  }

 protected:
  string service_name_;
  std::shared_ptr<Messenger> server_messenger_;
  scoped_refptr<ServicePool> service_pool_;
  std::shared_ptr<kudu::MemTracker> mem_tracker_;
  scoped_refptr<ResultTracker> result_tracker_;
  int n_worker_threads_;
  int service_queue_length_;
  int n_server_reactor_threads_;
  int keepalive_time_ms_;

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

} // namespace rpc
} // namespace kudu
#endif
