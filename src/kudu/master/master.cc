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

#include "kudu/master/master.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/location_cache.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/master_path_handlers.h"
#include "kudu/master/master_service.h"
#include "kudu/master/ts_manager.h"
#include "kudu/master/txn_manager.h"
#include "kudu/master/txn_manager_service.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/service_if.h"
#include "kudu/security/token_signer.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/startup_path_handler.h"
#include "kudu/server/webserver.h"
#include "kudu/tserver/tablet_copy_service.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/timer.h"
#include "kudu/util/version_info.h"

namespace kudu {
namespace rpc {
class Messenger;
}  // namespace rpc
}  // namespace kudu

DEFINE_int32(master_registration_rpc_timeout_ms, 1500,
             "Timeout for retrieving master registration over RPC.");
TAG_FLAG(master_registration_rpc_timeout_ms, experimental);

DEFINE_int64(tsk_rotation_seconds, 60 * 60 * 24 * 1,
             "Number of seconds between consecutive activations of newly "
             "generated TSKs (Token Signing Keys).");
TAG_FLAG(tsk_rotation_seconds, advanced);
TAG_FLAG(tsk_rotation_seconds, experimental);

DEFINE_int64(authn_token_validity_seconds, 60 * 60 * 24 * 7,
             "Period of time for which an issued authentication token is valid. "
             "Clients will automatically attempt to reacquire a token after the "
             "validity period expires.");
TAG_FLAG(authn_token_validity_seconds, experimental);

DEFINE_int64(authz_token_validity_seconds, 60 * 5,
             "Period of time for which an issued authorization token is valid. "
             "Clients will automatically attempt to reacquire a token after the "
             "validity period expires.");
TAG_FLAG(authz_token_validity_seconds, experimental);

DEFINE_int32(check_expired_table_interval_seconds, 60,
             "Interval (in seconds) to check whether there is any soft_deleted table "
             "with expired reservation period. Such tables will be purged and cannot "
             "be recalled after that.");

DEFINE_string(location_mapping_cmd, "",
              "A Unix command which takes a single argument, the IP address or "
              "hostname of a tablet server or client, and returns the location "
              "string for the tablet server. A location string begins with a / "
              "and consists of /-separated tokens each of which contains only "
              "characters from the set [a-zA-Z0-9_-.]. If the cluster is not "
              "using location awareness features this flag should not be set.");

DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_bool(txn_manager_enabled);

using kudu::consensus::RaftPeerPB;
using kudu::fs::ErrorHandlerType;
using kudu::rpc::ServiceIf;
using kudu::security::TokenSigner;
using kudu::transactions::TxnManager;
using kudu::transactions::TxnManagerServiceImpl;
using kudu::tserver::ConsensusServiceImpl;
using kudu::tserver::TabletCopyServiceImpl;
using std::min;
using std::nullopt;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {
class RpcContext;
}  // namespace rpc
}  // namespace kudu

namespace kudu {
namespace master {

namespace {
constexpr const char* kReplaceMasterMessage =
    "this master may return incorrect results and should be replaced";
void CrashMasterOnDiskError(const string& uuid) {
  LOG(FATAL) << Substitute("Disk error detected on data directory $0: $1",
                           uuid, kReplaceMasterMessage);
}
void CrashMasterOnCFileCorruption(const string& tablet_id) {
  LOG(FATAL) << Substitute("CFile corruption detected on system catalog $0: $1",
                           tablet_id, kReplaceMasterMessage);
}
void CrashMasterOnKudu2233Corruption(const string& tablet_id) {
  LOG(FATAL) << Substitute("KUDU-2233 corruption detected on system catalog $0: $1 ",
                           tablet_id, kReplaceMasterMessage);
}

// TODO(Alex Feinberg) this method should be moved to a separate class (along with
// ListMasters), so that it can also be used in TS and client when
// bootstrapping.
Status GetMasterEntryForHost(const shared_ptr<rpc::Messenger>& messenger,
                             const HostPort& hostport,
                             ServerEntryPB* e) {
  Sockaddr sockaddr;
  RETURN_NOT_OK(SockaddrFromHostPort(hostport, &sockaddr));
  MasterServiceProxy proxy(messenger, sockaddr, hostport.host());
  GetMasterRegistrationRequestPB req;
  GetMasterRegistrationResponsePB resp;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(FLAGS_master_registration_rpc_timeout_ms));
  RETURN_NOT_OK(proxy.GetMasterRegistration(req, &resp, &controller));
  e->mutable_instance_id()->CopyFrom(resp.instance_id());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  e->mutable_registration()->CopyFrom(resp.registration());
  e->set_role(resp.role());
  if (resp.has_cluster_id()) {
    e->set_cluster_id(resp.cluster_id());
  }
  if (resp.has_member_type()) {
    e->set_member_type(resp.member_type());
  }
  return Status::OK();
}
}  // anonymous namespace

Master::Master(const MasterOptions& opts)
    : KuduServer("Master", opts, "kudu.master"),
      state_(kStopped),
      catalog_manager_(new CatalogManager(this)),
      txn_manager_(FLAGS_txn_manager_enabled ? new TxnManager(this) : nullptr),
      path_handlers_(new MasterPathHandlers(this)),
      opts_(opts),
      registration_initialized_(false) {
  const auto& location_cmd = FLAGS_location_mapping_cmd;
  if (!location_cmd.empty()) {
    location_cache_.reset(new LocationCache(location_cmd, metric_entity_.get()));
  }
  ts_manager_.reset(new TSManager(location_cache_.get(), metric_entity_));
}

Master::~Master() {
  ShutdownImpl();
}

Status Master::Init() {
  CHECK_EQ(kStopped, state_);

  cfile::BlockCache::GetSingleton()->StartInstrumentation(
      metric_entity(), opts_.block_cache_metrics_policy());

  RETURN_NOT_OK(ThreadPoolBuilder("init").set_max_threads(1).Build(&init_pool_));
  startup_path_handler_->set_is_tablet_server(false);
  RETURN_NOT_OK(KuduServer::Init());

  if (web_server_) {
    RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));
  }

  maintenance_manager_.reset(new MaintenanceManager(
      MaintenanceManager::kDefaultOptions,
      fs_manager_->uuid(),
      metric_entity()));

  // The certificate authority object is initialized upon loading
  // CA private key and certificate from the system table when the server
  // becomes a leader.
  cert_authority_.reset(new MasterCertAuthority(fs_manager_->uuid()));

  // The TokenSigner loads its keys during catalog manager initialization.
  token_signer_.reset(new TokenSigner(
      FLAGS_authn_token_validity_seconds,
      FLAGS_authz_token_validity_seconds,
      FLAGS_tsk_rotation_seconds,
      messenger_->shared_token_verifier()));

  state_ = kInitialized;
  return Status::OK();
}

Status Master::Start() {
  RETURN_NOT_OK(StartAsync());
  RETURN_NOT_OK(WaitForCatalogManagerInit());
  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  return Status::OK();
}

Status Master::StartAsync() {
  CHECK_EQ(kInitialized, state_);
  fs_manager_->SetErrorNotificationCb(ErrorHandlerType::DISK_ERROR,
                                      &CrashMasterOnDiskError);
  fs_manager_->SetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION,
                                      &CrashMasterOnCFileCorruption);
  fs_manager_->SetErrorNotificationCb(ErrorHandlerType::KUDU_2233_CORRUPTION,
                                      &CrashMasterOnKudu2233Corruption);

  RETURN_NOT_OK(maintenance_manager_->Start());

  unique_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  unique_ptr<ServiceIf> consensus_service(
      new ConsensusServiceImpl(this, catalog_manager_.get()));
  unique_ptr<ServiceIf> tablet_copy_service(
      new TabletCopyServiceImpl(this, catalog_manager_.get()));
  unique_ptr<ServiceIf> txn_manager_service(
      txn_manager_ ? new TxnManagerServiceImpl(this) : nullptr);

  RETURN_NOT_OK(RegisterService(std::move(impl)));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(RegisterService(std::move(tablet_copy_service)));
  if (txn_manager_service) {
    RETURN_NOT_OK(RegisterService(std::move(txn_manager_service)));
  }
  RETURN_NOT_OK(KuduServer::Start());

  // Now that we've bound, construct our ServerRegistrationPB.
  RETURN_NOT_OK(InitMasterRegistration());

  // Start initializing the catalog manager.
  RETURN_NOT_OK(init_pool_->Submit([this]() {
    this->InitCatalogManagerTask();
  }));

  if (txn_manager_ && !FLAGS_txn_manager_lazily_initialized) {
    // Start initializing the TxnManager.
    RETURN_NOT_OK(ScheduleTxnManagerInit());
  }

  // Soft-delete related functions are not supported when HMS is enabled.
  if (!hms::HmsCatalog::IsEnabled()) {
    RETURN_NOT_OK(StartExpiredReservedTablesDeleterThread());
  }
  state_ = kRunning;

  return Status::OK();
}

void Master::InitCatalogManagerTask() {
  startup_path_handler_->initialize_master_catalog_progress()->Start();
  Status s = InitCatalogManager();
  if (!s.ok()) {
    LOG(ERROR) << "Unable to init master catalog manager: " << s.ToString();
  }
  catalog_manager_init_status_.Set(s);
  startup_path_handler_->initialize_master_catalog_progress()->Stop();
}

Status Master::InitCatalogManager() {
  if (catalog_manager_->IsInitialized()) {
    return Status::IllegalState("Catalog manager is already initialized");
  }
  RETURN_NOT_OK_PREPEND(catalog_manager_->Init(is_first_run_),
                        "Unable to initialize catalog manager");
  return Status::OK();
}

Status Master::WaitForCatalogManagerInit() const {
  CHECK_EQ(state_, kRunning);
  return catalog_manager_init_status_.Get();
}

Status Master::ScheduleTxnManagerInit() {
  DCHECK(txn_manager_);
  return init_pool_->Submit([this]() { this->InitTxnManagerTask(); });
}

void Master::InitTxnManagerTask() {
  DCHECK(txn_manager_);
  // For successful TxnManager's initialization it's necessary to have enough
  // tablet servers running in a Kudu cluster. Since Kudu master can be started
  // up in environments where tablet servers start long after the master's
  // startup, this task retries indefinitely to initialize TxnManager and
  // make it ready to handle requests in case of non-lazy initialization mode
  // (the latter is controlled by the --txn_manager_lazily_initialized flag).
  Status s;
  while (true) {
    if (state_ == kStopping || state_ == kStopped) {
      s = Status::Incomplete("shut down while trying to initialize TxnManager");
      break;
    }
    s = InitTxnManager();
    if (s.ok()) {
      break;
    }
    // TODO(aserbin): if retrying every second looks too often, consider adding
    //                exponential back-off and adding condition variables to
    //                wake up a long-awaiting task and retry initialization
    //                right away when TxnManager receives a call.
    static const MonoDelta kRetryInterval = MonoDelta::FromSeconds(1);
    KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
        "$0: unable to init TxnManager, will retry in $1",
        s.ToString(), kRetryInterval.ToString());
    SleepFor(kRetryInterval);
  }
  txn_manager_init_status_.Set(s);
}

Status Master::InitTxnManager() {
  if (!txn_manager_) {
    return Status::IllegalState("TxnManager is not enabled");
  }
  RETURN_NOT_OK_PREPEND(txn_manager_->Init(), "unable to initialize TxnManager");
  return Status::OK();
}

Status Master::WaitForTxnManagerInit(MonoTime deadline) const {
  if (deadline.Initialized()) {
    const Status* s = txn_manager_init_status_.WaitFor(deadline - MonoTime::Now());
    if (!s) {
      return Status::TimedOut("timed out waiting for TxnManager to initialize");
    }
  }
  return txn_manager_init_status_.Get();
}

Status Master::WaitUntilCatalogManagerIsLeaderAndReadyForTests(const MonoDelta& timeout) {
  Status s;
  MonoTime start = MonoTime::Now();
  int backoff_ms = 1;
  const int kMaxBackoffMs = 256;
  do {
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog_manager_.get());
      if (l.first_failed_status().ok()) {
        return Status::OK();
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(backoff_ms));
    backoff_ms = min(backoff_ms << 1, kMaxBackoffMs);
  } while (MonoTime::Now() < (start + timeout));
  return Status::TimedOut("Maximum time exceeded waiting for master leadership",
                          s.ToString());
}

Status Master::GetMasterRegistration(ServerRegistrationPB* reg,
                                     bool use_external_addr) const {
  if (!registration_initialized_.load(std::memory_order_acquire)) {
    return Status::ServiceUnavailable("Master startup not complete");
  }
  reg->CopyFrom(registration_);
  if (use_external_addr) {
    DCHECK_GT(reg->rpc_proxy_addresses_size(), 0);
    if (reg->rpc_proxy_addresses_size() > 0) {
      reg->clear_rpc_addresses();
      reg->mutable_rpc_addresses()->CopyFrom(reg->rpc_proxy_addresses());
    }

    // TODO(aserbin): uncomment once the webserver proxy advertised addresses
    //                are properly propagated in the code
    //DCHECK_GT(reg->proxy_http_addresses_size(), 0);
    if (reg->http_proxy_addresses_size() > 0) {
      reg->clear_http_addresses();
      reg->mutable_http_addresses()->CopyFrom(reg->http_proxy_addresses());
    }
  }
  return Status::OK();
}

Status Master::InitMasterRegistration() {
  CHECK(!registration_initialized_.load());

  ServerRegistrationPB reg;
  vector<HostPort> hps;
  RETURN_NOT_OK(rpc_server()->GetAdvertisedHostPorts(&hps));
  for (const auto& hp : hps) {
    *reg.add_rpc_addresses() = HostPortToPB(hp);
  }

  if (web_server()) {
    vector<Sockaddr> http_addrs;
    RETURN_NOT_OK(web_server()->GetAdvertisedAddresses(&http_addrs));
    RETURN_NOT_OK(AddHostPortPBs(http_addrs, reg.mutable_http_addresses()));
    reg.set_https_enabled(web_server()->IsSecure());
  }
  reg.set_software_version(VersionInfo::GetVersionInfo());
  reg.set_start_time(start_walltime_);

  registration_.Swap(&reg);
  registration_initialized_.store(true);

  return Status::OK();
}

void Master::ShutdownImpl() {
  if (kInitialized == state_ || kRunning == state_) {
    const string name = rpc_server_->ToString();
    LOG(INFO) << "Master@" << name << " shutting down...";
    state_ = kStopping;

    // 1. Stop accepting new RPCs.
    UnregisterAllServices();

    // 2. Shut down the master's subsystems.
    init_pool_->Shutdown();
    maintenance_manager_->Shutdown();
    catalog_manager_->Shutdown();
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::DISK_ERROR);
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION);

    // 3. Shut down generic subsystems.
    KuduServer::Shutdown();
    LOG(INFO) << "Master@" << name << " shutdown complete.";
  }
  state_ = kStopped;
}

Status Master::StartExpiredReservedTablesDeleterThread() {
  return Thread::Create("master",
                        "expired-reserved-tables-deleter",
                        [this]() { this->ExpiredReservedTablesDeleterThread(); },
                        &expired_reserved_tables_deleter_thread_);
}

void Master::ExpiredReservedTablesDeleterThread() {
  // How often to attempt to delete expired tables.
  const MonoDelta kWait = MonoDelta::FromSeconds(FLAGS_check_expired_table_interval_seconds);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(DeleteExpiredReservedTables(), "Unable to delete expired reserved tables");
  }
}

Status Master::DeleteExpiredReservedTables() {
  CatalogManager::ScopedLeaderSharedLock l(catalog_manager());
  if (!l.first_failed_status().ok()) {
    // Skip checking if this master is not leader.
    return Status::OK();
  }

  ListTablesRequestPB list_req;
  ListTablesResponsePB list_resp;
  // Set for soft_deleted table map.
  list_req.set_show_soft_deleted(true);
  RETURN_NOT_OK(catalog_manager_->ListTables(&list_req, &list_resp, nullopt));
  for (const auto& table : list_resp.tables()) {
    bool is_soft_deleted_table = false;
    bool is_expired_table = false;
    TableIdentifierPB table_identifier;
    table_identifier.set_table_name(table.name());
    Status s = catalog_manager_->GetTableStates(
        table_identifier,
        CatalogManager::TableInfoMapType::kSoftDeletedTableType,
        &is_soft_deleted_table, &is_expired_table);
    if (s.ok() && (!is_soft_deleted_table || !is_expired_table)) {
      continue;
    }

    // Delete the table.
    DeleteTableRequestPB del_req;
    del_req.mutable_table()->set_table_id(table.id());
    del_req.mutable_table()->set_table_name(table.name());
    del_req.set_reserve_seconds(0);
    DeleteTableResponsePB del_resp;
    LOG(INFO) << "Start to delete soft_deleted table " << table.name();
    WARN_NOT_OK(catalog_manager_->DeleteTableRpc(del_req, &del_resp, nullptr),
                Substitute("Failed to delete soft_deleted table $0 (table_id=$1)",
                           table.name(), table.id()));
  }

  return Status::OK();
}

Status Master::ListMasters(vector<ServerEntryPB>* masters,
                           bool use_external_addr) const {
  auto consensus = catalog_manager_->master_consensus();
  if (!consensus) {
    return Status::IllegalState("consensus not running");
  }
  const auto config = consensus->CommittedConfig();
  masters->clear();
  DCHECK_GE(config.peers_size(), 1);
  // Optimized code path that doesn't involve reaching out to other
  // masters over network for single master configuration.
  if (config.peers_size() == 1) {
    ServerEntryPB local_entry;
    local_entry.mutable_instance_id()->CopyFrom(catalog_manager_->NodeInstance());
    RETURN_NOT_OK(GetMasterRegistration(local_entry.mutable_registration(),
                                        use_external_addr));
    local_entry.set_role(RaftPeerPB::LEADER);
    local_entry.set_cluster_id(catalog_manager_->GetClusterId());
    local_entry.set_member_type(RaftPeerPB::VOTER);
    masters->emplace_back(std::move(local_entry));
    return Status::OK();
  }

  // For distributed master configuration.
  // TODO(aserbin): update this to work with proxied RPCs
  for (const auto& peer : config.peers()) {
    HostPort hp = HostPortFromPB(peer.last_known_addr());
    ServerEntryPB peer_entry;
    Status s = GetMasterEntryForHost(messenger_, hp, &peer_entry);
    if (!s.ok()) {
      s = s.CloneAndPrepend(
          Substitute("Unable to get registration information for peer $0 ($1)",
                     peer.permanent_uuid(),
                     hp.ToString()));
      LOG(WARNING) << s.ToString();
      StatusToPB(s, peer_entry.mutable_error());
    } else if (peer_entry.instance_id().permanent_uuid() != peer.permanent_uuid()) {
      StatusToPB(Status::IllegalState(
          Substitute("mismatched UUIDs: expected UUID $0 from master at $1, but got UUID $2",
                     peer.permanent_uuid(),
                     hp.ToString(),
                     peer_entry.instance_id().permanent_uuid())),
                 peer_entry.mutable_error());
    }
    masters->emplace_back(std::move(peer_entry));
  }
  return Status::OK();
}

Status Master::GetMasterHostPorts(vector<HostPort>* hostports, MasterType type) const {
  auto consensus = catalog_manager_->master_consensus();
  if (!consensus) {
    return Status::IllegalState("consensus not running");
  }

  auto get_raft_member_type = [] (MasterType type) constexpr {
    switch (type) {
      case MasterType::VOTER_ONLY:
        return RaftPeerPB::VOTER;
      default:
        LOG(FATAL) << "No matching Raft member type for master type: " << type;
    }
  };

  hostports->clear();
  consensus::RaftConfigPB config = consensus->CommittedConfig();
  for (const auto& peer : config.peers()) {
    if (type == MasterType::ALL || get_raft_member_type(type) == peer.member_type()) {
      // In non-distributed master configurations, we may not store our own
      // last known address in the Raft config. So, we'll fill it in from
      // the server Registration instead.
      if (!peer.has_last_known_addr()) {
        DCHECK_EQ(config.peers_size(), 1);
        DCHECK(registration_initialized_.load());
        DCHECK_GT(registration_.rpc_addresses_size(), 0);
        hostports->emplace_back(HostPortFromPB(registration_.rpc_addresses(0)));
      } else {
        hostports->emplace_back(HostPortFromPB(peer.last_known_addr()));
      }
    }
  }
  return Status::OK();
}

Status Master::AddMaster(const HostPort& hp, rpc::RpcContext* rpc) {
  // Ensure requested master to be added is not already part of list of masters.
  vector<HostPort> masters;
  // Here the check is made against committed config with all member types.
  RETURN_NOT_OK(GetMasterHostPorts(&masters, MasterType::ALL));
  if (std::find(masters.begin(), masters.end(), hp) != masters.end()) {
    return Status::AlreadyPresent("Master already present");
  }

  // If a new master is being added to a single master configuration, check that
  // the current leader master has the 'last_known_addr' populated. Otherwise
  // Raft will return an error which isn't very actionable.
  if (masters.size() == 1) {
    auto consensus = catalog_manager_->master_consensus();
    if (!consensus) {
      return Status::IllegalState("consensus not running");
    }
    const auto& config = consensus->CommittedConfig();
    DCHECK_EQ(1, config.peers_size());
    if (!config.peers(0).has_last_known_addr()) {
      return Status::InvalidArgument("'last_known_addr' field in single master Raft configuration "
                                     "not set. Please restart master with --master_addresses flag "
                                     "set to the single master which will populate the "
                                     "'last_known_addr' field.");
    }
  }

  // Check whether the master to be added is reachable and fetch its uuid.
  ServerEntryPB peer_entry;
  RETURN_NOT_OK(GetMasterEntryForHost(messenger_, hp, &peer_entry));
  const auto& peer_uuid = peer_entry.instance_id().permanent_uuid();

  // No early validation for whether a config change is in progress.
  // If it's in progress, on initiating config change Raft will return error.
  return catalog_manager()->InitiateMasterChangeConfig(CatalogManager::kAddMaster, hp, peer_uuid,
                                                       rpc);
}

Status Master::RemoveMaster(const HostPort& hp, const string& uuid, rpc::RpcContext* rpc) {
  // Ensure requested master to be removed is part of list of masters.
  auto consensus = catalog_manager_->master_consensus();
  if (!consensus) {
    return Status::IllegalState("consensus not running");
  }
  consensus::RaftConfigPB config = consensus->CommittedConfig();

  // We can't allow removing a master from a single master configuration. Following
  // check ensures a more appropriate error message is returned in case the removal
  // was targeted for a different cluster.
  if (config.peers_size() == 1) {
    bool hp_found;
    if (!config.peers(0).has_last_known_addr()) {
      // In non-distributed master configurations, we may not store our own
      // last known address in the Raft config.
      DCHECK(registration_initialized_.load());
      DCHECK_GT(registration_.rpc_addresses_size(), 0);
      const auto& addresses = registration_.rpc_addresses();
      hp_found = std::find_if(addresses.begin(), addresses.end(),
                              [&hp](const auto &hp_pb) {
                                return HostPortFromPB(hp_pb) == hp;
                              }) != addresses.end();
    } else {
      hp_found = HostPortFromPB(config.peers(0).last_known_addr()) == hp;
    }
    if (hp_found) {
      return Status::InvalidArgument(Substitute("Can't remove master $0 in a single master "
                                                "configuration", hp.ToString()));
    }
    return Status::NotFound(Substitute("Master $0 not found", hp.ToString()));
  }

  // UUIDs of masters matching the supplied HostPort 'hp' to remove.
  vector<string> matching_masters;
  for (const auto& peer : config.peers()) {
    if (peer.has_last_known_addr() && HostPortFromPB(peer.last_known_addr()) == hp) {
      matching_masters.push_back(peer.permanent_uuid());
    }
  }

  string matching_uuid;
  if (PREDICT_TRUE(matching_masters.size() == 1)) {
    if (!uuid.empty() && uuid != matching_masters[0]) {
      return Status::InvalidArgument(Substitute("Mismatch in UUID of the master $0 to be removed. "
                                                "Expected: $1, supplied: $2.", hp.ToString(),
                                                matching_masters[0], uuid));
    }
    matching_uuid = matching_masters[0];
  } else if (matching_masters.empty()) {
    return Status::NotFound(Substitute("Master $0 not found", hp.ToString()));
  } else {
    // We found multiple masters with matching HostPorts. Use the optional uuid to
    // disambiguate, if possible.
    DCHECK_GE(matching_masters.size(), 2);
    if (!uuid.empty()) {
      int matching_uuids_count = std::count(matching_masters.begin(), matching_masters.end(), uuid);
      if (matching_uuids_count == 1) {
        matching_uuid = uuid;
      } else {
        LOG(FATAL) << Substitute("Found multiple masters with same RPC address $0 and UUID $1",
                                 hp.ToString(), uuid);
      }
    } else {
      // Uuid not supplied and we found multiple matching HostPorts.
      return Status::InvalidArgument(Substitute("Found multiple masters with same RPC address $0 "
                                                "and following UUIDs $1. Supply UUID to "
                                                "disambiguate.", hp.ToString(),
                                                JoinStrings(matching_masters, ",")));
    }
  }

  if (matching_uuid == fs_manager_->uuid()) {
    return Status::InvalidArgument(Substitute("Can't remove the leader master $0", hp.ToString()));
  }

  // No early validation for whether a config change is in progress.
  // If it's in progress, on initiating config change Raft will return error.
  return catalog_manager()->InitiateMasterChangeConfig(CatalogManager::kRemoveMaster, hp,
                                                       matching_uuid, rpc);
}

} // namespace master
} // namespace kudu
