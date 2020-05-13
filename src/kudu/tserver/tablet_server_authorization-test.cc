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

#include <stdint.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(tserver_enforce_access_control);
DECLARE_double(tserver_inject_invalid_authz_token_ratio);

namespace kudu {

using pb_util::SecureShortDebugString;
using rpc::ErrorStatusPB;
using rpc::RpcController;
using security::ColumnPrivilegePB;
using security::PrivateKey;
using security::SignedTokenPB;
using security::TablePrivilegePB;
using security::TokenSigner;
using security::TokenSigningPrivateKeyPB;
using security::TokenSigningPublicKeyPB;
using security::TokenVerifier;
using tablet::TabletReplica;
using tablet::WritePrivileges;
using tablet::WritePrivilegeToString;
using tablet::WritePrivilegeType;

namespace tserver {

namespace {

// Verifies the expected response for an invalid/malformed token.
void CheckInvalidAuthzToken(const Status& s, const RpcController& rpc) {
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
  ASSERT_TRUE(rpc.error_response()) << "Expected an error response";
  ASSERT_TRUE(rpc.error_response()->code() == ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN)
      << SecureShortDebugString(*rpc.error_response());
}

// Gets a private key for the given sequence number.
TokenSigningPrivateKeyPB GetTokenSigningPrivateKey(int seq_num) {
  TokenSigningPrivateKeyPB tsk;
  PrivateKey private_key;
  CHECK_OK(GeneratePrivateKey(/*num_bits=*/512, &private_key));
  string private_key_str_der;
  CHECK_OK(private_key.ToString(&private_key_str_der, security::DataFormat::DER));
  tsk.set_rsa_key_der(private_key_str_der);
  tsk.set_key_seq_num(seq_num);
  tsk.set_expire_unix_epoch_seconds(WallTime_Now() + 3600);
  return tsk;
}

// Test-param argument to instantiate various tserver requests and send the
// appropriate proxy calls.
typedef std::function<Status(const Schema&, const SignedTokenPB*, TabletServerServiceProxy*,
                             RpcController*)> RequestorFunc;

Status WriteGenerator(const Schema& schema, const SignedTokenPB* token,
                      TabletServerServiceProxy* proxy, RpcController* rpc) {
  WriteRequestPB req;
  req.set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema, 1234, 5678, "hello world",
                 req.mutable_row_operations());
  if (token) {
    *req.mutable_authz_token() = *token;
  }
  WriteResponsePB resp;
  LOG(INFO) << "Sending write request";
  return proxy->Write(req, &resp, rpc);
}

Status ScanGenerator(const Schema& schema, const SignedTokenPB* token,
                     TabletServerServiceProxy* proxy, RpcController* rpc) {
  ScanRequestPB req;
  req.set_call_seq_id(0);
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
  if (token) {
    *scan->mutable_authz_token() = *token;
  }
  ScanResponsePB resp;
  LOG(INFO) << "Sending scan request";
  return proxy->Scan(req, &resp, rpc);
}

Status SplitKeyRangeGenerator(const Schema& /*schema*/, const SignedTokenPB* token,
                              TabletServerServiceProxy* proxy, RpcController* rpc) {
  SplitKeyRangeRequestPB req;
  req.set_tablet_id(TabletServerTestBase::kTabletId);
  if (token) {
    *req.mutable_authz_token() = *token;
  }
  SplitKeyRangeResponsePB resp;
  LOG(INFO) << "Sending split-key-range request";
  return proxy->SplitKeyRange(req, &resp, rpc);
}

Status ChecksumGenerator(const Schema& schema, const SignedTokenPB* token,
                         TabletServerServiceProxy* proxy, RpcController* rpc) {
  ChecksumRequestPB req;
  NewScanRequestPB* scan = req.mutable_new_request();
  scan->set_tablet_id(TabletServerTestBase::kTabletId);
  RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
  if (token) {
    *scan->mutable_authz_token() = *token;
  }
  ChecksumResponsePB resp;
  LOG(INFO) << "Sending checksum scan request";
  return proxy->Checksum(req, &resp, rpc);
}

} // anonymous namespace

class AuthzTabletServerTestBase : public TabletServerTestBase {
 public:
  const string kUser = "dan";

  AuthzTabletServerTestBase()
      : prng_(SeedRandom()) {
  }

  void SetUp() override {
    FLAGS_tserver_enforce_access_control = true;
    NO_FATALS(TabletServerTestBase::SetUp());
    NO_FATALS(StartTabletServer(/*num_data_dirs=*/1));

    rpc::UserCredentials user;
    user.set_real_user(kUser);
    proxy_->set_user_credentials(user);

    TokenSigningPrivateKeyPB tsk = GetTokenSigningPrivateKey(1);
    shared_ptr<TokenVerifier> verifier(new TokenVerifier());
    // These tests aren't targeted at testing expiration, so pass in arbitrary
    // expiration values.
    signer_.reset(new TokenSigner(3600, 3600, 3600, verifier));
    ASSERT_OK(signer_->ImportKeys({ tsk }));
    public_keys = verifier->ExportKeys();
    ASSERT_OK(mini_server_->server()->mutable_token_verifier()->ImportKeys(public_keys));
  }

 protected:

  // Signer used to create authz tokens.
  unique_ptr<TokenSigner> signer_;

  // Initial set of public keys to use to import.
  vector<TokenSigningPublicKeyPB> public_keys;

  // Generates various random selections in the tests.
  mutable Random prng_;
};

class AuthzTabletServerTest : public AuthzTabletServerTestBase,
                              public testing::WithParamInterface<RequestorFunc> {};

TEST_P(AuthzTabletServerTest, TestInvalidAuthzTokens) {
  // Set up a privilege that permits everything. Even with these privileges,
  // invalid authz tokens will prevent access.
  TablePrivilegePB privilege;
  privilege.set_table_id(kTableId);
  privilege.set_scan_privilege(true);
  privilege.set_insert_privilege(true);
  privilege.set_update_privilege(true);
  privilege.set_delete_privilege(true);

  // Test various "invalid token" scenarios.
  typedef std::function<SignedTokenPB(void)> TokenCreator;
  vector<TokenCreator> token_creators;
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token with a bad signature";
    SignedTokenPB token;
    CHECK_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
    string bad_signature = token.signature();
    // Flip the bits in the signature.
    for (int i = 0; i < bad_signature.length(); i++) {
      char* byte = &bad_signature[i];
      *byte = ~*byte;
    }
    token.set_token_data(std::move(bad_signature));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token with no signature";
    SignedTokenPB token;
    CHECK_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
    token.clear_signature();
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating token for a different user";
    SignedTokenPB token;
    CHECK_OK(signer_->GenerateAuthzToken("bad-dan", privilege, &token));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating authn token instead of authz token";
    SignedTokenPB token;
    CHECK_OK(signer_->GenerateAuthnToken(kUser, &token));
    return token;
  });
  token_creators.emplace_back([&] {
    LOG(INFO) << "Generating expired authz token";
    TokenSigningPrivateKeyPB tsk = GetTokenSigningPrivateKey(2);
    shared_ptr<TokenVerifier> verifier(new TokenVerifier());
    TokenSigner expired_signer(3600, /*authz_token_validity_seconds=*/1, 3600, verifier);
    CHECK_OK(expired_signer.ImportKeys({ tsk }));
    vector<TokenSigningPublicKeyPB> expired_public_keys = verifier->ExportKeys();
    CHECK_OK(mini_server_->server()->mutable_token_verifier()->ImportKeys(public_keys));

    SignedTokenPB token;
    CHECK_OK(expired_signer.GenerateAuthzToken(kUser, privilege, &token));
    // Wait for the token to expire.
    SleepFor(MonoDelta::FromSeconds(3));
    return token;
  });

  const auto& send_req = GetParam();
  // Run all of the above "invalid token" scenarios against the above
  // requests.
  for (const auto& token_creator : token_creators) {
    RpcController rpc;
    const SignedTokenPB token = token_creator();
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }

  // Send a request with no token. This is also considered an "invalid token".
  {
    LOG(INFO) << "Generating request with no authz token";
    RpcController rpc;
    Status s = send_req(schema_, nullptr, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }
  // Now test a valid token that has no privileges. This is flat-out
  // disallowed and "fatal".
  {
    LOG(INFO) << "Generating request with no privileges";
    SignedTokenPB token;
    TablePrivilegePB empty;
    empty.set_table_id(kTableId);
    ASSERT_OK(signer_->GenerateAuthzToken(kUser, empty, &token));
    RpcController rpc;
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
    ASSERT_TRUE(rpc.error_response());
    ASSERT_TRUE(rpc.error_response()->code() == ErrorStatusPB::FATAL_UNAUTHORIZED)
        << SecureShortDebugString(*rpc.error_response());
  }
  // Create a healthy token but inject an error.
  {
    LOG(INFO) << "Generating healthy request but injecting error";
    google::FlagSaver saver;
    FLAGS_tserver_inject_invalid_authz_token_ratio = 1.0;
    SignedTokenPB token;
    ASSERT_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
    RpcController rpc;
    Status s = send_req(schema_, &token, proxy_.get(), &rpc);
    NO_FATALS(CheckInvalidAuthzToken(s, rpc));
  }
  // Create a healthy token.
  {
    LOG(INFO) << "Generating healthy request";
    SignedTokenPB token;
    ASSERT_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
    RpcController rpc;
    ASSERT_OK(send_req(schema_, &token, proxy_.get(), &rpc));
    ASSERT_FALSE(rpc.error_response());
  }
}

INSTANTIATE_TEST_CASE_P(RequestorFuncs, AuthzTabletServerTest,
    ::testing::Values(&WriteGenerator, &ScanGenerator,
                      &SplitKeyRangeGenerator, &ChecksumGenerator));

namespace {

// Boolean to indicate the expected result of authorization.
enum class ExpectedAuthz {
  ALLOWED,
  DENIED
};

// Boolean to indicate usage of deprecated fields.
enum class DeprecatedField {
  USE,
  DONT_USE
};

// Enum indicating different non-standard scenarios we need to make sure are
// handled appropriately.
enum class SpecialColumn {
  // A malicious user may try to discover the presence of columns by misnaming
  // columns.
  MISNAMED,

  // A user may want to perform a scan on a virtual column, i.e. a valid column
  // that does not exist in the tablet but exists in the projection.
  VIRTUAL,

  NONE,
};

// Encapsulates entities that describe a scan that are relevant to
// authorization, used for easier composability of tests. With a schema, this
// can be used to generate scan requests.
struct ScanDescriptor {
  // Whether this describes a scan using the primary key (e.g. for ordering).
  bool use_pk;

  // The column names to project.
  unordered_set<string> projected_cols;

  // The column names to predicate on.
  unordered_set<string> predicated_cols;

  string ToString() const {
    set<string> sorted_projected(projected_cols.begin(), projected_cols.end());
    set<string> sorted_predicated(predicated_cols.begin(), predicated_cols.end());
    return Substitute("use_pk: $0, projected_cols: [$1], predicated_cols: [$2]", use_pk,
                      JoinStrings(sorted_projected, ", "), JoinStrings(sorted_predicated, ", "));
  }
};

// Default variable names for the scan-related tests below.
constexpr char kScanTableId[] = "scan-table-id";
constexpr char kScanTabletId[] = "scan-tablet-id";
constexpr char kDummyColumn[] = "not-my-column";

// Mapping of column names to column IDs.
typedef unordered_map<string, ColumnId> ColumnNamesToIds;

// Encapsulates the scan-related privileges that an authz token can contain,
// used for easier composability of tests.
struct ScanPrivileges {
  // Whether the privilege has full scan privileges.
  bool full_privileges;

  // The column names that are allowed to be scanned.
  unordered_set<string> col_privileges;

  // Table ID that these privileges are associated with. If empty, a default
  // table ID will be used.
  string table_id;

  // Translates the privileges into a TablePrivilegePB for use in a token,
  // using the column IDs in 'name_to_id'.
  TablePrivilegePB ToPB(const ColumnNamesToIds& name_to_id) const {
    TablePrivilegePB pb;
    if (full_privileges) {
      pb.set_scan_privilege(true);
    }
    ColumnPrivilegePB col_privilege;
    col_privilege.set_scan_privilege(true);
    for (const auto& col_name : col_privileges) {
      const auto& col_id = FindOrDie(name_to_id, col_name);
      InsertOrDie(pb.mutable_column_privileges(), col_id, col_privilege);
    }
    pb.set_table_id(table_id.empty() ? kScanTableId : table_id);
    return pb;
  }

  string ToString() const {
    set<string> sorted_cols(col_privileges.begin(), col_privileges.end());
    return Substitute("full_privileges: $0, col_privileges: [$1]",
                      full_privileges, JoinStrings(sorted_cols, ", "));
  }
};

// Utility function to unwrap RPC response errors.
template<class Resp>
Status CheckNoErrors(const Resp& resp) {
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

// Generates an encoded key of the given value for the given schema.
string GenerateEncodedKey(int32_t val, const Schema& schema) {
  EncodedKeyBuilder builder(&schema);
  for (int i = 0; i < schema.num_key_columns(); i++) {
    DCHECK_EQ(INT32, schema.column(i).type_info()->physical_type());
    builder.AddColumnKey(&val);
  }
  unique_ptr<EncodedKey> key(builder.BuildEncodedKey());
  Slice slice = key->encoded_key();
  return slice.ToString();
}

// Returns a column schema PB that matches 'col', but has a different name.
void MisnamedColumnSchemaToPB(const ColumnSchema& col, ColumnSchemaPB* pb) {
  ColumnSchemaToPB(ColumnSchema(kDummyColumn, col.type_info()->physical_type(), col.is_nullable(),
                   col.read_default_value(), col.write_default_value(), col.attributes(),
                   col.type_attributes()), pb);
}

} // anonymous namespace

// Functor to parameterize tests with that generates scan-like requests (e.g.
// Scans, Checksums) given a ScanDescriptor (for the request contents) and
// ScanPrivileges (for an attached authz token).
class ScanPrivilegeAuthzTest;

typedef std::function<Status(ScanPrivilegeAuthzTest*,
                             const ScanDescriptor&,
                             const ScanPrivileges&)> ScanFunc;

// Parameterized based on the scan request function and whether or not the scan
// request should use the primary key.
class ScanPrivilegeAuthzTest : public AuthzTabletServerTestBase,
                               public ::testing::WithParamInterface<std::tuple<ScanFunc, bool>> {
 public:
  static constexpr int kNumKeys = 5;
  static constexpr int kNumVals = 5;

  void SetUp() override {
    NO_FATALS(AuthzTabletServerTestBase::SetUp());
    SchemaBuilder schema_builder;
    for (int i = 0; i < kNumKeys; i++) {
      const string key = Substitute("key$0", i);
      schema_builder.AddKeyColumn(key, DataType::INT32);
      col_names_.emplace_back(key);
    }
    for (int i = 0; i < kNumVals; i++) {
      const string val = Substitute("val$0", kNumKeys + i);
      schema_builder.AddColumn(ColumnSchema(val, DataType::INT32),
                               /*is_key=*/false);
      col_names_.emplace_back(val);
    }
    schema_ = schema_builder.Build();

    // Put together a map from column name to ID so we can put together
    // ID-based tokens based on column names.
    for (int i = 0; i < schema_.num_columns(); i++) {
      ColumnId column_id = schema_.column_id(i);
      EmplaceOrDie(&name_to_id_, schema_.column_by_id(column_id).name(), column_id);
    }
    ASSERT_OK(mini_server_->AddTestTablet(kScanTableId, kScanTabletId, schema_));
    scoped_refptr<TabletReplica> replica;
    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kScanTabletId, &replica));
    ASSERT_OK(WaitForTabletRunning(kScanTabletId));
  }

  // Returns a signed token for the given scan privileges.
  Status GenerateScanAuthzToken(const ScanPrivileges& privilege, SignedTokenPB* authz_token) const {
    TablePrivilegePB privilege_pb = privilege.ToPB(name_to_id_);
    return signer_->GenerateAuthzToken(kUser, std::move(privilege_pb), authz_token);
  }

  // Populates fields of a NewScanRequestPB based on the scan descriptor,
  // including an authz token based on 'privilege'.
  NewScanRequestPB GenerateScanRequest(const ScanDescriptor& scan,
                                       const ScanPrivileges& privilege,
                                       DeprecatedField range_predicate,
                                       SpecialColumn special_col) const {
    NewScanRequestPB pb;
    pb.set_tablet_id(kScanTabletId);
    Schema client_schema = schema_.CopyWithoutColumnIds();
    if (scan.use_pk) {
      pb.set_order_mode(ORDERED);
      // Ordered scans must be snapshot scans.
      pb.set_read_mode(READ_AT_SNAPSHOT);
    } else {
      pb.set_order_mode(UNORDERED);
    }
    // Set some arbitrary bounds; the values don't matter for authorization.
    int32_t inclusive_lower_bound = 0;
    int32_t exclusive_upper_bound = 10;
    int32_t inclusive_upper_bound = exclusive_upper_bound - 1;
    for (const auto& col_name : scan.predicated_cols) {
      // Also test our deprecated predicate API and our new one; the deprecated
      // API is still available for backwards compatability and is thus fair
      // game for authorization.
      if (range_predicate == DeprecatedField::USE) {
        ColumnRangePredicatePB* range = pb.add_deprecated_range_predicates();
        int col_idx = schema_.find_column(col_name);
        ColumnSchemaToPB(client_schema.column(col_idx), range->mutable_column());
        range->mutable_lower_bound()->append(
            reinterpret_cast<char*>(&inclusive_lower_bound), sizeof(inclusive_lower_bound));
        range->mutable_inclusive_upper_bound()->append(
            reinterpret_cast<char*>(&inclusive_upper_bound), sizeof(inclusive_upper_bound));
      } else {
        ColumnPredicatePB* pred = pb.add_column_predicates();
        pred->set_column(col_name);
        ColumnPredicatePB::Range* range = pred->mutable_range();
        range->mutable_lower()->append(
            reinterpret_cast<char*>(&inclusive_lower_bound), sizeof(inclusive_lower_bound));
        range->mutable_upper()->append(
            reinterpret_cast<char*>(&exclusive_upper_bound), sizeof(exclusive_upper_bound));
      }
    }
    // Determine which column to sabotage if needed.
    boost::optional<string> misnamed_col;
    if (special_col == SpecialColumn::MISNAMED) {
      misnamed_col = SelectRandomElement<unordered_set<string>, string, Random>(
          scan.projected_cols, &prng_);
    }
    for (const auto& col_name : scan.projected_cols) {
      int col_idx = schema_.find_column(col_name);
      auto* projected_column = pb.add_projected_columns();
      if (misnamed_col && col_name == *misnamed_col) {
        CHECK(special_col == SpecialColumn::MISNAMED);
        MisnamedColumnSchemaToPB(client_schema.column(col_idx), projected_column);
      } else {
        ColumnSchemaToPB(client_schema.column(col_idx), projected_column);
      }
    }
    if (special_col == SpecialColumn::VIRTUAL) {
      auto* projected_column = pb.add_projected_columns();
      bool default_bool = false;
      ColumnSchemaToPB(ColumnSchema("is_deleted", DataType::IS_DELETED, /*is_nullable=*/false,
                                    /*read_default=*/&default_bool, nullptr), projected_column);
    }
    CHECK_OK(GenerateScanAuthzToken(privilege, pb.mutable_authz_token()));
    return pb;
  }

  // Populates fields of a split-key request based on the scan descriptor.
  SplitKeyRangeRequestPB GenerateSplitKeyRequest(const ScanDescriptor& scan,
                                                 SpecialColumn special_col) const {
    // Split key requests have no projections and therefore can't use virtual
    // columns that don't exist in the tablet schema (e.g. IS_DELETED columns).
    CHECK(special_col == SpecialColumn::MISNAMED || special_col == SpecialColumn::NONE);

    // Split-key requests are special in that they are really just projecting
    // and predicating on the same set of columns. Since that's the case, just
    // create a request that has the union of the described scan.
    unordered_set<string> cols = scan.projected_cols;
    cols.insert(scan.predicated_cols.begin(), scan.predicated_cols.end());
    SplitKeyRangeRequestPB split_pb;
    split_pb.set_tablet_id(kScanTabletId);
    Schema client_schema = schema_.CopyWithoutColumnIds();

    // Determine which column to sabotage if needed.
    boost::optional<string> misnamed_col;
    if (special_col == SpecialColumn::MISNAMED) {
      misnamed_col = SelectRandomElement<unordered_set<string>, string, Random>(cols, &prng_);
    }
    for (const auto& col_name : cols) {
      int col_idx = client_schema.find_column(col_name);
      if (misnamed_col && col_name == *misnamed_col) {
        MisnamedColumnSchemaToPB(client_schema.column(col_idx), split_pb.add_columns());
      } else {
        ColumnSchemaToPB(client_schema.column(col_idx), split_pb.add_columns());
      }
    }
    // Set an arbitrary chunk size.
    split_pb.set_target_chunk_size_bytes(100);

    // Set arbitrary primary key bounds if needed.
    if (scan.use_pk) {
      *split_pb.mutable_start_primary_key() = GenerateEncodedKey(0, schema_);
      *split_pb.mutable_stop_primary_key() = GenerateEncodedKey(100, schema_);
    }
    return split_pb;
  }

  // Sends a scan based on 'scan' with a token described by 'privilege'.
  Status SendNewScan(const ScanDescriptor& scan, const ScanPrivileges& privilege,
                     DeprecatedField range_predicate, SpecialColumn special_col) const {
    ScanResponsePB resp;
    RpcController rpc;
    ScanRequestPB req;
    *req.mutable_new_scan_request() = GenerateScanRequest(scan, privilege,
                                                          range_predicate, special_col);
    req.set_call_seq_id(0);
    RETURN_NOT_OK(proxy_->Scan(req, &resp, &rpc));
    return CheckNoErrors(resp);
  }

  // Sends a checksum scan based on 'scan' with a token described by
  // 'privilege'.
  Status SendChecksum(const ScanDescriptor& scan, const ScanPrivileges& privilege,
                      DeprecatedField range_predicate, SpecialColumn special_col) const {
    ChecksumResponsePB resp;
    RpcController rpc;
    ChecksumRequestPB req;
    NewScanRequestPB* new_scan_req = req.mutable_new_request();
    *new_scan_req = GenerateScanRequest(scan, privilege, range_predicate, special_col);
    req.set_call_seq_id(0);
    RETURN_NOT_OK(proxy_->Checksum(req, &resp, &rpc));
    return CheckNoErrors(resp);
  }

  // Sends a split-key request based on 'scan' with a token described by
  // 'privilege'.
  Status SendSplitKey(const ScanDescriptor& scan, const ScanPrivileges& privilege,
                      SpecialColumn special_col) const {
    SplitKeyRangeResponsePB resp;
    RpcController rpc;
    SplitKeyRangeRequestPB req = GenerateSplitKeyRequest(scan, special_col);
    RETURN_NOT_OK(GenerateScanAuthzToken(privilege, req.mutable_authz_token()));
    RETURN_NOT_OK(proxy_->SplitKeyRange(req, &resp, &rpc));
    return CheckNoErrors(resp);
  }

  // Sends a scan request and checks that the response matches the expected
  // output based on 'is_authorized'.
  void CheckPrivileges(const ScanFunc& send_req, const ScanDescriptor& scan,
                       const ScanPrivileges& privileges, ExpectedAuthz is_authorized,
                       const char* error = "not authorized") {
    Status s = send_req(this, scan, privileges);
    if (is_authorized == ExpectedAuthz::ALLOWED) {
      ASSERT_OK(s);
    } else {
      ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), error);
    }
  }

  // Returns a randomly selected set of column names, of at least size
  // 'min_returned'.
  unordered_set<string> RandomColumnNames(int min_returned = 0) const {
    vector<string> rand_privileges = SelectRandomSubset<vector<string>, string, Random>(
        col_names_, min_returned, &prng_);
    unordered_set<string> rand_set(rand_privileges.begin(), rand_privileges.end());
    return rand_set;
  }

 protected:
  Schema schema_;

  // The column names, the first `kNumKeys` of which are keys.
  vector<string> col_names_;

  // Mapping from column names to column ID, useful for building tokens (which
  // are ID-based) from client-side info (name-based).
  ColumnNamesToIds name_to_id_;
};

namespace {

// Functors for performing scan-like requests with which to parameterize tests.
template<DeprecatedField d, SpecialColumn c>
Status ScanRequestor(ScanPrivilegeAuthzTest* test, const ScanDescriptor& scan,
                     const ScanPrivileges& privileges) {
  return test->SendNewScan(scan, privileges, d, c);
}
template<DeprecatedField d, SpecialColumn c>
Status ChecksumRequestor(ScanPrivilegeAuthzTest* test, const ScanDescriptor& scan,
                     const ScanPrivileges& privileges) {
  return test->SendChecksum(scan, privileges, d, c);
}
template<DeprecatedField d, SpecialColumn c>
Status SplitKeyRangeRequestor(ScanPrivilegeAuthzTest* test, const ScanDescriptor& scan,
                     const ScanPrivileges& privileges) {
  return test->SendSplitKey(scan, privileges, c);
}

// Removes a column at random from 'privilege' out of those in 'candidates'.
// Populates 'removed' with the column name that was removed, and returns
// whether anything was actually removed.
bool RemoveScanPrivilege(const unordered_set<string>& candidates,
                     ScanPrivileges* privilege, string* removed) {
  if (candidates.empty()) {
    return false;
  }
  vector<string> candidates_list(candidates.begin(), candidates.end());
  int index_to_remove = rand() % candidates.size();
  string to_remove = candidates_list[index_to_remove];
  const auto& col_privileges = privilege->col_privileges;
  const auto& iter_to_remove = col_privileges.find(to_remove);
  if (iter_to_remove == col_privileges.end()) {
    return false;
  }
  privilege->col_privileges.erase(iter_to_remove);
  *removed = to_remove;
  return true;
}

// Removes a column privilege at random from 'privilege'.
void RemoveColumnPrivilege(ScanPrivileges* privilege) {
  string removed;
  CHECK(RemoveScanPrivilege(privilege->col_privileges, privilege, &removed));
  LOG(INFO) << Substitute("Removed privilege for column $0", removed);
}

} // anonymous namespace

// Test scan privileges when not authorized with full scan privileges.
TEST_P(ScanPrivilegeAuthzTest, TestPartialScanPrivileges) {
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  // Put together a scan that projects and predicates on some columns.
  ScanDescriptor scan = {
    .use_pk = use_pk,
    .projected_cols = { "key1", "key2", "val5", "val6" },
    .predicated_cols = { "key3", "val7" },
  };
  ScanPrivileges privileges;
  if (!use_pk) {
    // For a scan that doesn't use the primary key, we only need the privileges
    // on the union of the projected columns and the predicate columns.
    privileges = {
      .full_privileges = false,
      .col_privileges = { "key1", "key2", "key3", "val5", "val6", "val7" }
    };
  } else {
    // For a scan that does use the primary key, we also need to include the
    // full list of columns that comprise the primary key.
    privileges = {
      .full_privileges = false,
      .col_privileges = { "key0", "key1", "key2", "key3", "key4", "val5", "val6", "val7" }
    };
  }
  NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
  RemoveColumnPrivilege(&privileges);
  NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
}

// Similar to the above test, but randomized.
TEST_P(ScanPrivilegeAuthzTest, TestPartialScanPrivilegesRandomized) {
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  ScanDescriptor scan = {
    .use_pk = use_pk,
    // Some scan-like requests treat 0 projected columns specially (e.g. this
    // is a count operation projecting all columns). For the purposes of
    // checking all other cases, enforce that we project at least one column.
    .projected_cols = RandomColumnNames(/*min_returned=*/1),
    .predicated_cols = RandomColumnNames(),
  };
  // We'll start with all column privileges and widdle our way down, avoiding
  // removal of columns that we need to perform our scan.
  ScanPrivileges privileges = {
    .full_privileges = false,
    .col_privileges = unordered_set<string>(col_names_.begin(), col_names_.end())
  };
  // Keep track of the columns we need -- the projected columns, predicated
  // columns, and primary keys if the scan calls for it.
  unordered_set<string> required_columns(scan.projected_cols.begin(), scan.projected_cols.end());
  required_columns.insert(scan.predicated_cols.begin(), scan.predicated_cols.end());
  if (use_pk) {
    for (int i = 0; i < kNumKeys; i++) {
      required_columns.insert(col_names_[i]);
    }
  }
  unordered_set<string> unneeded_cols(col_names_.begin(), col_names_.end());
  for (const string& col : required_columns) {
    unneeded_cols.erase(col);
  }
  // Remove a bunch of unneeded columns first. We should continue to be
  // authorized to scan.
  int unneeded_cols_to_remove = unneeded_cols.empty() ? 0 : rand() % unneeded_cols.size();
  string removed;
  for (int i = 0; i < unneeded_cols_to_remove; i++) {
    CHECK(RemoveScanPrivilege(unneeded_cols, &privileges, &removed));
    unneeded_cols.erase(removed);
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
  }
  // The moment we remove a required column, we should be denied access.
  ASSERT_TRUE(RemoveScanPrivilege(required_columns, &privileges, &removed));
  LOG(INFO) << Substitute("Removed privilege for column $0", removed);
  SCOPED_TRACE(privileges.ToString());
  SCOPED_TRACE(scan.ToString());
  NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
}

// Test that we can scan anything when granted full scan privileges.
TEST_P(ScanPrivilegeAuthzTest, TestFullScanPrivileges) {
  const int kNumRequests = 10;
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  for (int i = 0; i < kNumRequests; i++) {
    ScanPrivileges privileges = {
      .full_privileges = true,
    };
    // Add privileges at random. Since we have full scan privileges, these
    // shouldn't affect our ability to scan whatsoever, but let's do so as a
    // sanity check.
    privileges.col_privileges = RandomColumnNames();

    // Randomly generate a scan. Whatever it is, we should be able to scan it.
    ScanDescriptor scan = {
      .use_pk = use_pk,
      .projected_cols = RandomColumnNames(),
      .predicated_cols = RandomColumnNames()
    };
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
  }
}

// Test that we get something sensible when using a token that doesn't match
// the request's table ID.
TEST_P(ScanPrivilegeAuthzTest, TestWrongTableId) {
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  // Set up a scan that we are authorized to do, but generate a token with the
  // wrong table ID for it.
  ScanPrivileges privileges = {
    .full_privileges = true,
    .col_privileges = unordered_set<string>(),
    .table_id = "wrong-table-id",
  };
  ScanDescriptor scan = {
    .use_pk = use_pk,
    .projected_cols = RandomColumnNames(),
    .predicated_cols = RandomColumnNames()
  };
  const auto check_wrong_table = [&] {
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED,
        "authorization token is for the wrong table ID"));
  };
  NO_FATALS(check_wrong_table());
  // Do the same for a scan that we aren't authorized to perform.
  privileges.full_privileges = false;
  NO_FATALS(check_wrong_table());
}

INSTANTIATE_TEST_CASE_P(RequestorFuncs, ScanPrivilegeAuthzTest,
    ::testing::Combine(
        ::testing::ValuesIn(vector<ScanFunc>({
            &ScanRequestor<DeprecatedField::DONT_USE, SpecialColumn::NONE>,
            &ScanRequestor<DeprecatedField::USE, SpecialColumn::NONE>,
            &ChecksumRequestor<DeprecatedField::DONT_USE, SpecialColumn::NONE>,
            &ChecksumRequestor<DeprecatedField::USE, SpecialColumn::NONE>,
            &SplitKeyRangeRequestor<DeprecatedField::DONT_USE, SpecialColumn::NONE>,
            &SplitKeyRangeRequestor<DeprecatedField::USE, SpecialColumn::NONE>
        })),
        ::testing::Bool()));

class ScanPrivilegeNoProjectionAuthzTest : public ScanPrivilegeAuthzTest {};

// Test that for scans and checksums that have no projection, we require
// privileges on all columns.
TEST_P(ScanPrivilegeNoProjectionAuthzTest, TestNoProjection) {
  const int kNumRequests = 10;
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  for (int i = 0; i < kNumRequests; i++) {
    ScanPrivileges privileges = {
      .full_privileges = false,
      .col_privileges = unordered_set<string>(col_names_.begin(), col_names_.end()),
    };
    // Randomly generate a scan with no projected columns.
    ScanDescriptor scan = {
      .use_pk = use_pk,
      .projected_cols = unordered_set<string>(),
      .predicated_cols = RandomColumnNames()
    };
    {
      SCOPED_TRACE(privileges.ToString());
      SCOPED_TRACE(scan.ToString());
      NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
    }
    RemoveColumnPrivilege(&privileges);
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
  }
}
INSTANTIATE_TEST_CASE_P(RequestorFuncs, ScanPrivilegeNoProjectionAuthzTest,
    ::testing::Combine(
        ::testing::ValuesIn(vector<ScanFunc>({
            &ScanRequestor<DeprecatedField::DONT_USE, SpecialColumn::NONE>,
            &ScanRequestor<DeprecatedField::USE, SpecialColumn::NONE>,
            &ChecksumRequestor<DeprecatedField::DONT_USE, SpecialColumn::NONE>,
            &ChecksumRequestor<DeprecatedField::USE, SpecialColumn::NONE>,
        })),
        ::testing::Bool()));

class ScanPrivilegeVirtualColumnsTest : public ScanPrivilegeAuthzTest {};

TEST_P(ScanPrivilegeVirtualColumnsTest, TestNoProjection) {
  const int kNumRequests = 10;
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  for (int i = 0; i < kNumRequests; i++) {
    ScanPrivileges privileges = {
      .full_privileges = false,
      .col_privileges = unordered_set<string>(col_names_.begin(), col_names_.end()),
    };
    // Randomly generate a scan with no projected columns.
    ScanDescriptor scan = {
      .use_pk = use_pk,
      .projected_cols = RandomColumnNames(/*min_returned=*/1),
      .predicated_cols = RandomColumnNames()
    };
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
    RemoveColumnPrivilege(&privileges);
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
  }
}

class ScanPrivilegeWithBadNamesTest: public ScanPrivilegeAuthzTest {};

// Send a request with a projection on a column that don't exist. Unless the
// user has full scan privileges, the client should just get back a
// non-authorized error, rather than a more information-rich one.
TEST_P(ScanPrivilegeWithBadNamesTest, TestColumnNotFound) {
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  ScanDescriptor scan = {
    .use_pk = use_pk,
    .projected_cols = RandomColumnNames(/*min_returned=*/1),
    .predicated_cols = RandomColumnNames(),
  };
  ScanPrivileges privileges = {
    .full_privileges = false,
    .col_privileges = unordered_set<string>(col_names_.begin(), col_names_.end())
  };
  {
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
  }
  privileges = {
    .full_privileges = true,
  };
  // Now send the request with full scan privileges. We should be able to see
  // the bad column name.
  SCOPED_TRACE(privileges.ToString());
  SCOPED_TRACE(scan.ToString());
  Status s = req_func(this, scan, privileges);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), kDummyColumn);
}
INSTANTIATE_TEST_CASE_P(RequestorFuncs, ScanPrivilegeWithBadNamesTest,
    ::testing::Combine(
        ::testing::ValuesIn(vector<ScanFunc>({
            &ScanRequestor<DeprecatedField::DONT_USE, SpecialColumn::MISNAMED>,
            &ScanRequestor<DeprecatedField::USE, SpecialColumn::MISNAMED>,
            &ChecksumRequestor<DeprecatedField::DONT_USE, SpecialColumn::MISNAMED>,
            &ChecksumRequestor<DeprecatedField::USE, SpecialColumn::MISNAMED>,
            &SplitKeyRangeRequestor<DeprecatedField::DONT_USE, SpecialColumn::MISNAMED>,
            &SplitKeyRangeRequestor<DeprecatedField::USE, SpecialColumn::MISNAMED>
        })),
        ::testing::Bool()));

class ScanPrivilegeWithVirtualColumnsTest: public ScanPrivilegeAuthzTest {};

TEST_P(ScanPrivilegeWithVirtualColumnsTest, TestIsDeletedColumn) {
  const ScanFunc& req_func = std::get<0>(GetParam());
  bool use_pk = std::get<1>(GetParam());
  ScanDescriptor scan = {
    .use_pk = use_pk,
    .projected_cols = RandomColumnNames(/*min_returned=*/1),
    .predicated_cols = RandomColumnNames(),
  };

  // Send out the request with full scan privileges.
  ScanPrivileges privileges = {
    .full_privileges = true,
  };
  {
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
  }
  privileges = {
    .full_privileges = false,
    .col_privileges = unordered_set<string>(col_names_.begin(), col_names_.end())
  };
  {
    SCOPED_TRACE(privileges.ToString());
    SCOPED_TRACE(scan.ToString());
    NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::ALLOWED));
  }

  // Generate privileges that don't have all column privileges. The presence
  // of a virtual column should require all privileges, so the request should
  // be denied.
  RemoveColumnPrivilege(&privileges);
  SCOPED_TRACE(privileges.ToString());
  SCOPED_TRACE(scan.ToString());
  NO_FATALS(CheckPrivileges(req_func, scan, privileges, ExpectedAuthz::DENIED));
}
INSTANTIATE_TEST_CASE_P(RequestorFuncs, ScanPrivilegeWithVirtualColumnsTest,
    ::testing::Combine(
        ::testing::ValuesIn(vector<ScanFunc>({
            &ScanRequestor<DeprecatedField::DONT_USE, SpecialColumn::VIRTUAL>,
            &ScanRequestor<DeprecatedField::USE, SpecialColumn::VIRTUAL>,
            &ChecksumRequestor<DeprecatedField::DONT_USE, SpecialColumn::VIRTUAL>,
            &ChecksumRequestor<DeprecatedField::USE, SpecialColumn::VIRTUAL>,
        })),
        ::testing::Bool()));

namespace {

struct WriteOpDescriptor {
  // Op type for the write request.
  RowOperationsPB::Type op_type;

  // Key value.
  int32_t key;

  // Value to populate the columns with. Ignored if this is a DELETE op.
  int32_t val;
};

string WritesToString(const vector<WriteOpDescriptor>& writes) {
  vector<string> write_strs;
  for (const auto& write : writes) {
    write_strs.emplace_back(Substitute("$0 ($1, $2)",
        RowOperationsPB_Type_Name(write.op_type), write.key, write.val));
  }
  return Substitute("Write request: { $0 }", JoinStrings(write_strs, ", "));
}

string WritePrivilegesToString(const WritePrivileges& privileges) {
  vector<string> privs;
  for (const auto& privilege : privileges) {
    privs.emplace_back(WritePrivilegeToString(privilege));
  }
  return Substitute("Privileges: { $0 }", JoinStrings(privs, ", "));
}

} // anonymous namespace

class WritePrivilegeAuthzTest : public AuthzTabletServerTestBase {
 public:
  WriteRequestPB BuildRequest(const vector<WriteOpDescriptor>& write_ops,
                              const WritePrivileges& privileges) const {
    WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    CHECK_OK(SchemaToPB(schema_, req.mutable_schema()));
    RowOperationsPB* data = req.mutable_row_operations();
    for (const auto& write : write_ops) {
      const auto& op_type = write.op_type;
      if (op_type == RowOperationsPB::DELETE) {
        AddTestKeyToPB(op_type, schema_, write.key, data);
      } else {
        AddTestRowWithNullableStringToPB(op_type, schema_, write.key, write.val,
                                         /*string_val=*/nullptr, data);
      }
    }
    CHECK_OK(GenerateWriteAuthzToken(privileges, req.mutable_authz_token()));
    return req;
  }

  Status GenerateWriteAuthzToken(const WritePrivileges& privileges,
                                 SignedTokenPB* authz_token) const {
    TablePrivilegePB privilege_pb;
    privilege_pb.set_table_id(kTableId);
    for (const auto& privilege : privileges) {
      switch (privilege) {
        case WritePrivilegeType::DELETE:
          privilege_pb.set_delete_privilege(true);
          break;
        case WritePrivilegeType::INSERT:
          privilege_pb.set_insert_privilege(true);
          break;
        case WritePrivilegeType::UPDATE:
          privilege_pb.set_update_privilege(true);
          break;
      }
    }
    return signer_->GenerateAuthzToken(kUser, std::move(privilege_pb), authz_token);
  }

  Status SendWrite(const vector<WriteOpDescriptor>& write_ops,
                   const WritePrivileges& privileges) const {
    WriteRequestPB req = BuildRequest(write_ops, privileges);
    WriteResponsePB resp;
    RpcController rpc;
    RETURN_NOT_OK(proxy_->Write(req, &resp, &rpc));
    LOG(INFO) << Substitute("Received response: $0", SecureShortDebugString(resp));
    return CheckNoErrors(resp);
  }

  // Checks that the write operations need the privileges in
  // 'required_privileges' by:
  // 1. generating a random set of privileges,
  // 2. making sure that a required privilege is missing,
  // 3. ensuring that the write request with missing privileges is rejected,
  // 4. adding back all the required privileges, and
  // 5. validating that the write can then proceed.
  void CheckWritePrivileges(const vector<WriteOpDescriptor>& write_ops,
                            const WritePrivileges& required_privileges) {
    // Generate a random set of privileges, but make sure it is missing a
    // required privilege.
    WritePrivileges privileges = RandomWritePrivileges();
    ASSERT_FALSE(required_privileges.empty());
    if (!privileges.empty()) {
      const auto& priv_to_remove = SelectRandomElement<WritePrivileges, WritePrivilegeType, Random>(
          required_privileges, &prng_);
      LOG(INFO) << "Removing write privilege: " << WritePrivilegeToString(priv_to_remove);
      privileges.erase(priv_to_remove);
    }
    SCOPED_TRACE(WritesToString(write_ops));
    {
      // With a required privilege missing, the write request should be
      // rejected.
      Status s = SendWrite(write_ops, privileges);
      SCOPED_TRACE(WritePrivilegesToString(privileges));
      ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "not authorized");
    }
    // Adding the required privileges should permit our operations.
    for (const auto& p : required_privileges) {
      InsertIfNotPresent(&privileges, p);
    }
    SCOPED_TRACE(WritePrivilegesToString(privileges));
    ASSERT_OK(SendWrite(write_ops, privileges));
  }

  // Returns a randomly selected set of write operation types to be used for
  // sending write requests. Always returns at least one type.
  RowOpTypes RandomOpTypes() const {
    static const vector<RowOperationsPB::Type> write_op_types = {
      RowOperationsPB::DELETE,
      RowOperationsPB::INSERT,
      RowOperationsPB::UPDATE,
      RowOperationsPB::UPSERT,
    };
    RowOpTypes types;
    types.reset(SelectRandomSubset<
        vector<RowOperationsPB::Type>, RowOperationsPB::Type, Random>(
        write_op_types, /*min_to_return=*/1, &prng_));
    return types;
  }

  // Returns a randomly selected set of write privileges to be used for
  // generating authz tokens. May be empty.
  WritePrivileges RandomWritePrivileges() const {
    static const vector<WritePrivilegeType> write_privilege_types {
      WritePrivilegeType::DELETE,
      WritePrivilegeType::INSERT,
      WritePrivilegeType::UPDATE,
    };
    vector<WritePrivilegeType> rand_types =
      SelectRandomSubset<vector<WritePrivilegeType>, WritePrivilegeType, Random>(
          write_privilege_types, /*min_to_return=*/0, &prng_);
    WritePrivileges privileges;
    privileges.reset(rand_types);
    return privileges;
  }
};

// Simple test for individual write operations.
TEST_F(WritePrivilegeAuthzTest, TestSingleWriteOperations) {
  {
    vector<WriteOpDescriptor> batch({
      { RowOperationsPB::INSERT, /*key=*/0, /*val=*/0 }
    });
    NO_FATALS(CheckWritePrivileges(batch, WritePrivileges{ WritePrivilegeType::INSERT }));
  }
  {
    vector<WriteOpDescriptor> batch({
      { RowOperationsPB::UPDATE, /*key=*/0, /*val=*/1234 }
    });
    NO_FATALS(CheckWritePrivileges(batch, WritePrivileges{ WritePrivilegeType::UPDATE }));
  }
  {
    vector<WriteOpDescriptor> batch({
      { RowOperationsPB::UPSERT, /*key=*/0, /*val=*/3465 }
    });
    NO_FATALS(CheckWritePrivileges(batch, WritePrivileges{ WritePrivilegeType::INSERT,
                                                           WritePrivilegeType::UPDATE }));
  }
  {
    vector<WriteOpDescriptor> batch({
      { RowOperationsPB::DELETE, /*key=*/0, /*val=*/0 }
    });
    NO_FATALS(CheckWritePrivileges(batch, WritePrivileges{ WritePrivilegeType::DELETE }));
  }
}

// Like the above test, but sent in a batch.
TEST_F(WritePrivilegeAuthzTest, TestWritesBatch) {
  vector<WriteOpDescriptor> batch({
    { RowOperationsPB::INSERT, /*key=*/0, /*val=*/0 },
    { RowOperationsPB::UPDATE, /*key=*/0, /*val=*/1234 },
    { RowOperationsPB::UPSERT, /*key=*/0, /*val=*/3465 },
    { RowOperationsPB::DELETE, /*key=*/0, /*val=*/0 },
  });
  NO_FATALS(CheckWritePrivileges(batch, WritePrivileges{ WritePrivilegeType::INSERT,
                                                         WritePrivilegeType::UPDATE,
                                                         WritePrivilegeType::DELETE }));
}

// Like the above test, but randomized. Note: we only care about authorizing
// the requests, not checking the results. Hence our lack of care in selecting
// which keys to send over.
TEST_F(WritePrivilegeAuthzTest, TestWritesRandomized) {
  const int kNumOps = 10;
  const auto op_types = RandomOpTypes();
  vector<WriteOpDescriptor> batch;
  WritePrivileges required_privileges;
  for (int i = 0; i < kNumOps; i++) {
    const auto op_type = SelectRandomElement<RowOpTypes, RowOperationsPB::Type, Random>(
        op_types, &prng_);
    batch.emplace_back(WriteOpDescriptor({
      .op_type = op_type,
      .key = rand(),
      .val = rand(),
    }));
    AddWritePrivilegesForRowOperations(op_type, &required_privileges);
  }
  LOG(INFO) << WritesToString(batch);
  LOG(INFO) << WritePrivilegesToString(required_privileges);
  NO_FATALS(CheckWritePrivileges(batch, required_privileges));
}

} // namespace tserver
} // namespace kudu
