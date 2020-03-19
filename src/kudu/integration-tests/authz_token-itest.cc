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

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using google::protobuf::util::MessageDifferencer;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DECLARE_bool(master_support_authz_tokens);
DECLARE_bool(tserver_enforce_access_control);
DECLARE_bool(raft_enable_pre_election);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_double(tserver_inject_invalid_authz_token_ratio);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int64(authz_token_validity_seconds);

METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableSchema);

namespace kudu {

class RWMutex;

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using client::AuthenticationCredentialsPB;
using client::sp::shared_ptr;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduError;
using client::KuduInsert;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableCreator;
using security::DataFormat;
using security::PrivateKey;
using security::SignedTokenPB;
using security::TablePrivilegePB;
using security::TokenSigner;
using security::TokenSigningPrivateKeyPB;
using strings::Substitute;

namespace {

// Relatively low timeout used so we don't have to wait too long for an
// "invalid token" error.
const int kRpcTimeoutSecs = 3;
const int kOperationTimeoutSecs = kRpcTimeoutSecs * 3;

// Inserts a single row to the given key-value table for the given key.
Status InsertKeyToTable(KuduTable* table, KuduSession* session, int key) {
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  KuduPartialRow* row = insert->mutable_row();
  RETURN_NOT_OK(row->SetInt32(0, key));
  RETURN_NOT_OK(row->SetInt32(1, key));
  return session->Apply(insert.release());
}

vector<Status> GetSessionErrors(KuduSession* session) {
  vector<KuduError*> errors;
  session->GetPendingErrors(&errors, nullptr);
  vector<Status> ret(errors.size());
  for (int i = 0; i < errors.size(); i++) {
    ret[i] = errors[i]->status();
  }
  ElementDeleter deleter(&errors);
  return ret;
}

// Scans values from the given table.
Status ScanFromTable(KuduTable* table) {
  KuduScanner scanner(table);
  scanner.SetTimeoutMillis(kOperationTimeoutSecs * 1000);
  vector<string> rows;
  return ScanToStrings(&scanner, &rows);
}

} // anonymous namespace

class AuthzTokenTest : public KuduTest {
 public:
  AuthzTokenTest()
      : schema_(KuduSchema::FromSchema(CreateKeyValueTestSchema())) {}
  const char* const kTableName = "test-table";
  const char* const kUser = "token-user";
  const char* const kBadUser = "bad-token-user";

  // Helper to get the authz token for 'table_id' from the client's cache.
  static bool FetchCachedAuthzToken(
      KuduClient* client, const string& table_id, SignedTokenPB* token) {
    return client->data_->FetchCachedAuthzToken(table_id, token);
  }
  // Helper to store the authz token for 'table_id' to the client's cache.
  static void StoreAuthzToken(KuduClient* client,
                              const string& table_id,
                              const SignedTokenPB& token) {
    client->data_->StoreAuthzToken(table_id, token);
  }

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_tserver_enforce_access_control = true;
    FLAGS_authz_token_validity_seconds = 1;

    // Create a table with a basic schema.
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(SetupClientAndTable());
  }

  // Sets up the client_ and client_table_ members.
  Status SetupClientAndTable() {
    RETURN_NOT_OK(CreateClientForUser(kUser, &client_));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(kTableName)
                               .schema(&schema_)
                               .num_replicas(1)
                               .set_range_partition_columns({ "key" })
                               .Create());
    RETURN_NOT_OK(client_->OpenTable(kTableName, &client_table_));
    table_id_ = client_table_->id();
    return Status::OK();
  }

  // Inserts the next appropriate row to the table.
  Status InsertToTable(KuduTable* table) {
    shared_ptr<KuduSession> session(table->client()->NewSession());
    RETURN_NOT_OK(InsertKeyToTable(table, session.get(), next_row_key_++));
    return session->Flush();
  }

  // Creates a client for the given user.
  Status CreateClientForUser(const string& user, shared_ptr<KuduClient>* client) const {
    // Many tests will expect operations to fail, so let's get there quicker by
    // setting a low timeout.
    KuduClientBuilder client_builder;
    client_builder.default_rpc_timeout(MonoDelta::FromSeconds(kRpcTimeoutSecs));
    string authn_creds;
    AuthenticationCredentialsPB authn_pb;
    authn_pb.set_real_user(user);
    CHECK(authn_pb.SerializeToString(&authn_creds));
    client_builder.import_authentication_credentials(std::move(authn_creds));
    return cluster_->CreateClient(&client_builder, client);
  }

  // Gets the current number of GetTableSchema requests the master has serviced.
  // This increments whenever a client opens a table or gets a new authz token.
  uint64_t NumGetTableSchemaRequests() const {
    const auto& ent = cluster_->mini_master()->master()->metric_entity();
    return METRIC_handler_latency_kudu_master_MasterService_GetTableSchema
        .Instantiate(ent)->TotalCount();
  }

  // Inserts the next row into the table, expecting an error. Returns the
  // session error, rather than the usual coarse-grained IOError.
  Status InsertToTableSessionError(KuduTable* table) {
    KuduClient* client = table->client();
    shared_ptr<KuduSession> session = client->NewSession();
    RETURN_NOT_OK(InsertKeyToTable(table, session.get(), next_row_key_++));
    Status s = session->Flush();
    if (!s.IsIOError()) {
      return s;
    }
    vector<Status> errors = GetSessionErrors(session.get());
    if (errors.size() != 1) {
      return Status::RuntimeError(Substitute("expected 1 error, got $0",
                                  errors.size()));
    }
    return errors[0];
  }

 protected:
  const KuduSchema schema_;
  unique_ptr<InternalMiniCluster> cluster_;

  // Client authenticated as the default user.
  shared_ptr<KuduClient> client_;

  // Table created with 'client_'.
  shared_ptr<KuduTable> client_table_;
  string table_id_;

  // The next row key to insert.
  int next_row_key_ = 0;
};

namespace {

// Functors with which the client can send requests.
Status InsertRequestor(AuthzTokenTest* test, KuduTable* table) {
  return test->InsertToTable(table);
}
Status ScanRequestor(AuthzTokenTest* /*test*/, KuduTable* table) {
  return ScanFromTable(table);
}

} // anonymous namespace

// Tests parameterized on different data operations.
typedef std::function<Status(AuthzTokenTest*, KuduTable*)> RequestorFunc;
class ReacquireAuthzTokenTest : public AuthzTokenTest,
                                public ::testing::WithParamInterface<RequestorFunc> {};

// Test scenarios that lead the client to retrieve a new token.
TEST_P(ReacquireAuthzTokenTest, TestInvalidAuthzTokens) {
  auto client_func = GetParam();
  // First, let's do a sanity check that initial authz tokens allow the user to
  // perform all actions.
  SignedTokenPB first_token;
  ASSERT_TRUE(FetchCachedAuthzToken(client_.get(), table_id_, &first_token));
  ASSERT_OK(client_func(this, client_table_.get()));

  // The above operations shouldn't have required getting a new token.
  SignedTokenPB same_token;
  ASSERT_TRUE(FetchCachedAuthzToken(client_.get(), table_id_, &same_token));
  ASSERT_TRUE(MessageDifferencer::Equals(first_token, same_token));

  shared_ptr<KuduClient> bad_client;
  ASSERT_OK(CreateClientForUser(kBadUser, &bad_client));
  shared_ptr<KuduTable> bad_table;
  ASSERT_OK(bad_client->OpenTable(kTableName, &bad_table));

  LOG(INFO) << "Trying to use the wrong user's token...";
  SignedTokenPB bad_token;
  ASSERT_TRUE(FetchCachedAuthzToken(client_.get(), table_id_, &bad_token));
  StoreAuthzToken(bad_client.get(), table_id_, bad_token);

  // The bad client should succeed after being told go retrieve a new token for
  // the correct user. Check that it received a different token.
  ASSERT_OK(client_func(this, bad_table.get()));
  SignedTokenPB new_token;
  ASSERT_TRUE(FetchCachedAuthzToken(bad_client.get(), table_id_, &new_token));
  ASSERT_FALSE(MessageDifferencer::Equals(bad_token, new_token));

  // Replace the token used by the client with one that is malformed by
  // messing with the token data. The server should respond such that the
  // client circled back to the master and got a new token.
  LOG(INFO) << "Trying to use a bad signature...";
  string bad_signature = std::move(*new_token.mutable_signature());
  // Flip the bits of the signature.
  for (int i = 0; i < bad_signature.length(); i++) {
    auto& byte = bad_signature[i];
    byte = ~byte;
  }
  bad_token = std::move(new_token);
  bad_token.set_token_data(std::move(bad_signature));
  StoreAuthzToken(bad_client.get(), table_id_, bad_token);
  ASSERT_OK(client_func(this, bad_table.get()));

  // The client should have received a new token.
  ASSERT_TRUE(FetchCachedAuthzToken(bad_client.get(), table_id_, &new_token));
  ASSERT_FALSE(MessageDifferencer::Equals(bad_token, new_token));
}

TEST_P(ReacquireAuthzTokenTest, TestExpiredAuthzTokens) {
  // We sleep for a bit to allow the expiration of tokens.
  SKIP_IF_SLOW_NOT_ALLOWED();
  auto client_func = GetParam();

  // Ensure that expired authz tokens will lead the client to retry with a new
  // token upon writing/scanning.
  uint64_t initial_reqs = NumGetTableSchemaRequests();
  SleepFor(MonoDelta::FromSeconds(FLAGS_authz_token_validity_seconds + 1));
  ASSERT_OK(client_func(this, client_table_.get()));
  ASSERT_GT(NumGetTableSchemaRequests(), initial_reqs);
}

INSTANTIATE_TEST_CASE_P(RequestorFuncs, ReacquireAuthzTokenTest,
    ::testing::ValuesIn(vector<RequestorFunc>({ &InsertRequestor, &ScanRequestor })));

// Test to ensure tokens with no privileges will disallow operations.
TEST_F(AuthzTokenTest, TestUnprivilegedAuthzTokens) {
  // Replace the token used by the client with one that has no permissions.
  // Since the token is well-formed, but does not have the sufficient
  // privileges to perform the actions, the client going back to the master
  // for a new token will not work, and the user will see an error.
  LOG(INFO) << "Trying to use an unprivileged token...";
  SignedTokenPB unprivileged_token;
  TablePrivilegePB no_privilege;
  no_privilege.set_table_id(table_id_);
  ASSERT_OK(cluster_->mini_master()->master()->token_signer()->GenerateAuthzToken(
      kUser, std::move(no_privilege), &unprivileged_token));
  StoreAuthzToken(client_.get(), table_id_, unprivileged_token);

  shared_ptr<KuduSession> bad_session(client_->NewSession());
  ASSERT_OK(InsertKeyToTable(client_table_.get(), bad_session.get(), next_row_key_++));

  // Write sessions will accumulate a bunch of non-authorized errors, veiling
  // them in an IOError.
  Status s = bad_session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  vector<Status> errors = GetSessionErrors(bad_session.get());
  for (const auto& e : errors) {
    ASSERT_TRUE(e.IsRemoteError()) << e.ToString();
    ASSERT_STR_CONTAINS(e.ToString(), "Not authorized");
  }

  // Scans will return a remote error with an appropriate message.
  s = ScanFromTable(client_table_.get());
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
}

// Test that ensures we retry when we send a token signed by a TSK that hasn't
// percolated to the tservers. In such cases, the tablet server should respond
// with an ERROR_UNAVAILABLE error and the request should be retried.
TEST_F(AuthzTokenTest, TestUnknownTsk) {
  // Create a TSK with a high enough sequence number that it will be unknown to
  // the server.
  TokenSigningPrivateKeyPB tsk;
  PrivateKey private_key;
  ASSERT_OK(GeneratePrivateKey(/*num_bits=*/512, &private_key));
  string private_key_str_der;
  ASSERT_OK(private_key.ToString(&private_key_str_der, DataFormat::DER));
  tsk.set_rsa_key_der(private_key_str_der);
  tsk.set_key_seq_num(100);
  tsk.set_expire_unix_epoch_seconds(WallTime_Now() + 3600);

  TablePrivilegePB privilege;
  privilege.set_table_id(table_id_);
  privilege.set_scan_privilege(true);
  privilege.set_insert_privilege(true);

  // Create a token signer to use our surprise TSK. The intervals don't matter.
  TokenSigner signer(100, 100, 100);
  ASSERT_OK(signer.ImportKeys({ tsk }));
  SignedTokenPB token;
  ASSERT_OK(signer.GenerateAuthzToken(kUser, std::move(privilege), &token));
  StoreAuthzToken(client_.get(), table_id_, token);

  // The operations will see ERROR_UNAVAILABLE and keep retrying, hoping for
  // the TSK to make its way to the server.
  Status s = ScanFromTable(client_table_.get());
  LOG(INFO) << s.ToString();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  s = InsertToTableSessionError(client_table_.get());
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Only after we import the TSK do operations succeed.
  ASSERT_OK(cluster_->mini_master()->master()->token_signer()->ImportKeys({ tsk }));
  ASSERT_OK(ScanFromTable(client_table_.get()));
  ASSERT_OK(InsertToTable(client_table_.get()));
}

// Test what happens when the single-master deployment responds with a
// retriable error when getting a new authz token.
TEST_F(AuthzTokenTest, TestSingleMasterUnavailable) {
  // We sleep in this test to ensure our scan has time to retry.
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Warm the client's meta cache so it doesn't need to go to the master for
  // the location of the tserver.
  ASSERT_OK(InsertToTable(client_table_.get()));

  // Set up the client such that its first operation will require it to go back
  // to the master (in this case, by giving it a token for the wrong user).
  shared_ptr<KuduClient> bad_client;
  ASSERT_OK(CreateClientForUser("bad-token-user", &bad_client));
  shared_ptr<KuduTable> bad_table;
  ASSERT_OK(bad_client->OpenTable(kTableName, &bad_table));
  SignedTokenPB bad_token;
  ASSERT_TRUE(FetchCachedAuthzToken(bad_client.get(), table_id_, &bad_token));
  StoreAuthzToken(client_.get(), table_id_, bad_token);

  // Take the leader lock on the master, which will prevent successful attempts
  // to get a new token, but will allow retries.
  std::unique_lock<RWMutex> leader_lock(
      cluster_->mini_master()->master()->catalog_manager()->leader_lock_);

  // After a while, the client operation will time out.
  Status s = ScanFromTable(client_table_.get());
  LOG(INFO) << s.ToString();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // If we let go of the leader lock while the operation is still in flight,
  // the operation should succeed. At this point, the client still shouldn't
  // have an authz token.
  thread scanner([&] {
    s = ScanFromTable(client_table_.get());
  });

  // Wait for a full RPC timeout to make the scan retry once more before
  // letting go of the leader lock.
  SleepFor(MonoDelta::FromSeconds(kRpcTimeoutSecs + 1));
  leader_lock.unlock();
  scanner.join();
  ASSERT_OK(s);
}

// Test with utilities to prevent the master(s) from providing authz tokens.
// The test is also configured such that the masters will frequently change
// leadership.
class BadMultiMasterAuthzTokenTest : public AuthzTokenTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // We're going to make elections more frequent, so set some non-runtime
    // flags up front. The values for these and the below flags are chosen to
    // not be flaky, even running with stress in TSAN mode.
    FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
    FLAGS_raft_heartbeat_interval_ms = 200;

    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(SetupClientAndTable());

    // Enforce access control, and set the rest of the election flags.
    FLAGS_tserver_enforce_access_control = true;
    FLAGS_raft_enable_pre_election = false;
    FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms / 3;
  }
};

// Test what happens when the multimaster deployment undergoes frequent leader
// changes. Tokens should still be issued and failures to get a token should be
// retried.
TEST_F(BadMultiMasterAuthzTokenTest, TestMasterElectionStorms) {
  // Set up the tablet servers such that they'll force the client to go back to
  // the master for a new token.
  FLAGS_tserver_inject_invalid_authz_token_ratio = 1.0;

  // Despite the master leader elections, new tokens should be receieved.
  // After a while, operations should time out because the authz tokens are all
  // invalid. The scanner will enrich the returned status with the last error
  // received from the tablet server.
  Status s = ScanFromTable(client_table_.get());
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");

  // Do the same for inserts.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(InsertKeyToTable(client_table_.get(), session.get(), next_row_key_++));
  s = session->Flush();
  vector<Status> errors = GetSessionErrors(session.get());
  for (const auto& e : errors) {
    ASSERT_TRUE(e.IsTimedOut()) << e.ToString();
    // TODO(awong): refactor WriteRpc so it spits out the tserver error that
    // caused it to attempt getting another token.
    ASSERT_STR_CONTAINS(e.ToString(), "RetrieveAuthzToken timed out");
  }

  // Now ease up the error injection on the tserver to ensure the tokens we get
  // are useable.
  FLAGS_tserver_inject_invalid_authz_token_ratio = 0.5;
  ASSERT_OK(ScanFromTable(client_table_.get()));
  ASSERT_OK(InsertToTable(client_table_.get()));
}


// Test in which the master does not support creating authz tokens.
class LegacyMasterAuthzTokenTest : public AuthzTokenTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_master_support_authz_tokens = false;
    FLAGS_tserver_enforce_access_control = false;
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(SetupClientAndTable());
  }
};

// Ensures the client can still communicate with servers that do not support
// authz tokens.
TEST_F(LegacyMasterAuthzTokenTest, TestAuthzTokensNotSupported) {
  // Client should have no problems connecting to an old cluster.
  ASSERT_OK(InsertToTable(client_table_.get()));
  ASSERT_OK(ScanFromTable(client_table_.get()));

  // In the unexpected case that the tservers enforce access control but we
  // have an old master, a scan will fail upon being asked to reacquire an
  // authz token, learning it is not supported.
  FLAGS_tserver_enforce_access_control = true;
  Status s = ScanFromTable(client_table_.get());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "does not support RetrieveAuthzToken");

  // The same will happen for writes.
  s = InsertToTableSessionError(client_table_.get());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "does not support RetrieveAuthzToken");
}

} // namespace kudu
