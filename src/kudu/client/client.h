// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

/// @mainpage Kudu C++ client API documentation
///
/// Kudu provides C++ and Java client APIs, as well as reference examples
/// to illustrate their use (check the source code for the examples).
/// This is Kudu C++ client API. Use of any APIs other than the client APIs
/// is unsupported.

#ifndef KUDU_CLIENT_CLIENT_H
#define KUDU_CLIENT_CLIENT_H

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

#include "kudu/client/row_result.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#ifdef KUDU_HEADERS_NO_STUBS
#include <gtest/gtest_prod.h>
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class ClientStressTest_TestUniqueClientIds_Test;
class KuduPartialRow;
class MonoDelta;
class PartitionSchema;
class SecurityUnknownTskTest;

namespace tools {
class LeaderMasterProxy;
} // namespace tools

namespace client {

class KuduClient;
class KuduDelete;
class KuduInsert;
class KuduLoggingCallback;
class KuduPartitioner;
class KuduScanBatch;
class KuduSession;
class KuduStatusCallback;
class KuduTable;
class KuduTableAlterer;
class KuduTableCreator;
class KuduTablet;
class KuduTabletServer;
class KuduUpdate;
class KuduUpsert;
class KuduValue;
class KuduWriteOperation;
class ResourceMetrics;

namespace internal {
class Batcher;
class ErrorCollector;
class GetTableSchemaRpc;
class LookupRpc;
class MetaCache;
class RemoteTablet;
class RemoteTabletServer;
class WriteRpc;
} // namespace internal

/// Install a callback for internal client logging.
///
/// The callback can be installed for a log event of any severity,
/// across any KuduClient object.
///
/// Only the first invocation has an effect; subsequent invocations are
/// a no-op. Before a callback is registered, all internal client log events
/// are logged to the stderr.
///
/// @param [in] cb
///   Logging callback. The caller must ensure that @c cb stays alive until
///   UninstallLoggingCallback() is called.
void KUDU_EXPORT InstallLoggingCallback(KuduLoggingCallback* cb);

/// Remove callback installed via InstallLoggingCallback().
///
/// Only the first invocation has an effect; subsequent invocations are
/// a no-op.
///
/// Should be called before unloading the client library.
void KUDU_EXPORT UninstallLoggingCallback();

/// Set the logging verbosity of the client library.
///
/// By default, the logging level is 0. Logs become progressively more verbose
/// as the level is increased. Empirically, the highest verbosity level
/// used in Kudu is 6, which includes very fine-grained tracing information.
/// Most useful logging is enabled at level 1 or 2, with the higher levels
/// used only in rare circumstances.
///
/// Logs are emitted to stderr, or to the configured log callback
/// at @c SEVERITY_INFO.
///
/// This function may be called safely at any point during usage of the library.
///
/// @param [in] level
///   Logging level to set.
void KUDU_EXPORT SetVerboseLogLevel(int level);

/// Set signal number to use internally.
///
/// The Kudu client library uses signals internally in some cases.
/// By default, it uses SIGUSR2. If your application makes use of SIGUSR2,
/// this advanced API can help workaround conflicts.
///
/// @param [in] signum
///   Signal number to use for internal.
/// @return Operation result status.
Status KUDU_EXPORT SetInternalSignalNumber(int signum);

/// Disable initialization of the Cyrus SASL library. Clients should call this
/// method before using the Kudu client if they are manually initializing Cyrus
/// SASL. If this method is not called, Kudu will attempt to auto-detect whether
/// SASL has been externally initialized, but it is recommended to be explicit.
///
/// If this function is called, it must be called prior to the first construction
/// of a KuduClient object.
///
/// NOTE: Kudu makes use of SASL from multiple threads. Thus, it's imperative
/// that embedding applications use sasl_set_mutex(3) to provide a mutex
/// implementation if they are choosing to handle SASL initialization manually.
Status KUDU_EXPORT DisableSaslInitialization();


/// Disable initialization of the OpenSSL library. Clients should call this
/// method and manually initialize OpenSSL before using the Kudu client if
/// they are also using OpenSSL for any other purpose. If this method is not
/// called, Kudu will attempt to initialize OpenSSL, which may trigger a crash
/// if concurrent with another thread's initialization attempt.
///
/// If this function is called, it must be called prior to the first construction
/// of a KuduClient object.
///
/// @note If OpenSSL initialization is disabled, Kudu depends on the embedding
/// application to take care of initialization. When this function is called,
/// Kudu will attempt to verify that the appropriate initialization steps have
/// been taken, and return a bad Status if they have not. Applications may
/// use the following code to initialize OpenSSL:
///
/// @code
///   SSL_load_error_strings();
///   SSL_library_init();
///   OpenSSL_add_all_algorithms();
///   RAND_poll(); // or an equivalent RAND setup.
///   CRYPTO_set_locking_callback(MyAppLockingCallback);
/// @endcode
Status KUDU_EXPORT DisableOpenSSLInitialization();

/// @return Short version info, i.e. a single-line version string
///   identifying the Kudu client.
std::string KUDU_EXPORT GetShortVersionString();

/// @return Detailed version info, i.e. a multi-line version string identifying
///   the client, including build time, etc.
std::string KUDU_EXPORT GetAllVersionInfo();

/// @brief A "factory" for KuduClient objects.
///
/// This class is used to create instances of the KuduClient class
/// with pre-set options/parameters.
class KUDU_EXPORT KuduClientBuilder {
 public:
  KuduClientBuilder();
  ~KuduClientBuilder();

  /// Clear the set of master addresses.
  ///
  /// @return Reference to the updated object.
  KuduClientBuilder& clear_master_server_addrs();

  /// Add RPC addresses of multiple masters.
  ///
  /// @param [in] addrs
  ///   RPC addresses of masters to add.
  /// @return Reference to the updated object.
  KuduClientBuilder& master_server_addrs(const std::vector<std::string>& addrs);

  /// Add an RPC address of a master to work with.
  ///
  /// At least one master is required.
  ///
  /// @param [in] addr
  ///   RPC address of master server to add.
  /// @return Reference to the updated object.
  KuduClientBuilder& add_master_server_addr(const std::string& addr);

  /// Set the default timeout for administrative operations.
  ///
  /// Using this method it is possible to modify the default timeout
  /// for operations like CreateTable, AlterTable, etc.
  /// By default it is 30 seconds.
  ///
  /// @param [in] timeout
  ///   Timeout value to set.
  /// @return Reference to the updated object.
  KuduClientBuilder& default_admin_operation_timeout(const MonoDelta& timeout);

  /// Set the default timeout for individual RPCs.
  ///
  /// If not provided, defaults to 10 seconds.
  ///
  /// @param [in] timeout
  ///   Timeout value to set.
  /// @return Reference to the updated object.
  KuduClientBuilder& default_rpc_timeout(const MonoDelta& timeout);

  /// Import serialized authentication credentials from another client.
  ///
  /// @param [in] authn_creds
  ///   The serialized authentication credentials, provided by a call to
  ///   @c KuduClient.ExportAuthenticationCredentials in the C++ client or
  ///   @c KuduClient#exportAuthenticationCredentials in the Java client.
  /// @return Reference to the updated object.
  KuduClientBuilder& import_authentication_credentials(std::string authn_creds);

  /// Create a client object.
  ///
  /// @note KuduClients objects are shared amongst multiple threads and,
  /// as such, are stored in shared pointers.
  ///
  /// @param [out] client
  ///   The newly created object wrapped in a shared pointer.
  /// @return Operation status. The return value may indicate
  ///   an error in the create operation, or a misuse of the builder;
  ///   in the latter case, only the last error is returned.
  Status Build(sp::shared_ptr<KuduClient>* client);

 private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduClientBuilder);
};

/// @brief A handle for a connection to a cluster.
///
/// The KuduClient class represents a connection to a cluster. From the user
/// perspective, they should only need to create one of these in their
/// application, likely a singleton -- but it is not a singleton in Kudu in any
/// way. Different KuduClient objects do not interact with each other -- no
/// connection pooling, etc. With the exception of common properties
/// managed by free (non-member) functions in the kudu::client namespace,
/// each KuduClient object is sandboxed with no global cross-client state.
///
/// In the implementation, the client holds various pieces of common
/// infrastructure which is not table-specific:
///   @li RPC messenger: reactor threads and RPC connections are pooled here
///   @li Authentication: the client is initialized with some credentials,
///     and all accesses through it share those credentials.
///   @li Caches: caches of table schemas, tablet locations, tablet server IP
///     addresses, etc are shared per-client.
///
/// In order to actually write data to the cluster, callers must first
/// create a KuduSession object using NewSession(). A KuduClient may
/// have several associated sessions.
///
/// @note This class is thread-safe.
///
/// @todo Cluster administration functions are likely to be in this class
///   as well.
class KUDU_EXPORT KuduClient : public sp::enable_shared_from_this<KuduClient> {
 public:
  ~KuduClient();

  /// Create a KuduTableCreator object.
  ///
  /// @return Pointer to newly created object; it is the caller's
  ///   responsibility to free it.
  KuduTableCreator* NewTableCreator();

  /// Check whether a create table operation is in-progress.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] create_in_progress
  ///   The value is set only in case of success; it is @c true iff
  ///   the operation is in progress.
  /// @return Operation status.
  Status IsCreateTableInProgress(const std::string& table_name,
                                 bool* create_in_progress);

  /// Delete/drop a table.
  ///
  /// @param [in] table_name
  ///   Name of the table to drop.
  /// @return Operation status.
  Status DeleteTable(const std::string& table_name);

  /// Create a KuduTableAlterer object.
  ///
  /// @param [in] table_name
  ///   Name of the table to alter.
  /// @return Pointer to newly created object: it is the caller's
  ///   responsibility to free it.
  KuduTableAlterer* NewTableAlterer(const std::string& table_name);

  /// Check if table alteration is in-progress.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] alter_in_progress
  ///   The value is set only in case of success; it is @c true iff
  ///   the operation is in progress.
  /// @return Operation status.
  Status IsAlterTableInProgress(const std::string& table_name,
                                bool* alter_in_progress);
  /// Get table's schema.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] schema
  ///   Raw pointer to the schema object; caller gets ownership.
  /// @return Operation status.
  Status GetTableSchema(const std::string& table_name,
                        KuduSchema* schema);

  /// Get information on current tablet servers.
  ///
  /// @param [out] tablet_servers
  ///   The placeholder for the result. The caller takes ownership
  ///   of the container's elements.
  /// @return Operation status.
  Status ListTabletServers(std::vector<KuduTabletServer*>* tablet_servers);

  /// List only those tables whose names pass a substring match on @c filter.
  ///
  /// @param [out] tables
  ///   The placeholder for the result. Appended only on success.
  /// @param [in] filter
  ///   Substring filter to use; empty sub-string filter matches all tables.
  /// @return Status object for the operation.
  Status ListTables(std::vector<std::string>* tables,
                    const std::string& filter = "");

  /// Check if the table given by 'table_name' exists.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] exists
  ///   Set only on success; set to @c true iff table exists.
  /// @return Status object for the operation.
  Status TableExists(const std::string& table_name, bool* exists);

  /// Open table with the given name.
  ///
  /// This method does an RPC to ensure that the table exists and
  /// looks up its schema.
  ///
  /// @note New range partitions created by other clients will immediately be
  ///   available after opening the table.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] table
  ///   The result table.
  /// @return Operation status.
  ///
  /// @todo Should we offer an async version of this as well?
  /// @todo Probably should have a configurable timeout in KuduClientBuilder?
  Status OpenTable(const std::string& table_name,
                   sp::shared_ptr<KuduTable>* table);

  /// Create a new session for interacting with the cluster.
  ///
  /// This is a fully local operation (no RPCs or blocking).
  ///
  /// @return A new session object; caller is responsible for destroying it.
  sp::shared_ptr<KuduSession> NewSession();

  /// @cond false

  /// Get tablet information for a tablet by ID.
  ///
  /// Private API.
  ///
  /// @todo This operation could benefit from the meta cache if it were
  /// possible to look up using a tablet ID.
  ///
  /// @param [in] tablet_id
  ///   Unique tablet identifier.
  /// @param [out] tablet
  ///   Tablet information. The caller takes ownership of the tablet.
  /// @return Status object for the operation.
  Status KUDU_NO_EXPORT GetTablet(const std::string& tablet_id,
                                  KuduTablet** tablet);

  /// @endcond

  /// Policy with which to choose amongst multiple replicas.
  enum ReplicaSelection {
    LEADER_ONLY,      ///< Select the LEADER replica.

    CLOSEST_REPLICA,  ///< Select the closest replica to the client,
                      ///< or a random one if all replicas are equidistant.

    FIRST_REPLICA     ///< Select the first replica in the list.
  };

  /// @return @c true iff client is configured to talk to multiple
  ///   Kudu master servers.
  bool IsMultiMaster() const;

  /// @return Default timeout for admin operations.
  const MonoDelta& default_admin_operation_timeout() const;

  /// @return Default timeout for RPCs.
  const MonoDelta& default_rpc_timeout() const;

  /// Value for the latest observed timestamp when none has been observed
  /// or set.
  static const uint64_t kNoTimestamp;

  /// Get the highest HybridTime timestamp observed by the client.
  ///
  /// The latest observed timestamp can be used to start a snapshot scan on a
  /// table which is guaranteed to contain all data written or previously read
  /// by this client. See KuduScanner for more details on timestamps.
  ///
  /// How to get Read-Your-Writes consistency:
  /// the code snippet below uses KuduClient::GetLatestObservedTimestamp() along
  /// with KuduScanner::SetSnapshotRaw() to perform READ_AT_SNAPSHOT scan
  /// containing the data which has just been written.  Notice extra 1
  /// added to the timestamp passed to KuduScanner::SetSnapshotRaw():
  /// @code
  ///   shared_ptr<KuduClient> client;
  ///   ... // open/initialize the client
  ///   shared_ptr<KuduSession> session(client->NewSession());
  ///   ... // set Kudu session properties
  ///   shared_ptr<KuduTable> table;
  ///   ... // open the table
  ///   unique_ptr<KuduInsert> insert_op(table->NewInsert());
  ///   ... // populate new insert operation with data
  ///   RETURN_NOT_OK(session->Apply(insert_op.release()));
  ///   RETURN_NOT_OK(session->Flush());
  ///   uint64_t snapshot_timestamp = client->GetLatestObservedTimestamp() + 1;
  ///   KuduScanner scanner(table.get());
  ///   RETURN_NOT_OK(scanner.SetSnapshotRaw(snapshot_timestamp));
  ///   RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  ///   RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  ///   RETURN_NOT_OK(scanner.Open());
  ///   ... // retrieve scanned rows
  /// @endcode
  /// There are currently races in which, in rare occasions, Read-Your-Writes
  /// consistency might not hold even in this case. These are being
  /// taken care of as part of
  /// <a href="https://issues.apache.org/jira/browse/KUDU-430">KUDU-430</a>
  ///
  /// @note This method is experimental and will either disappear or
  ///   change in a future release.
  ///
  /// @return Highest HybridTime timestamp observed by the client.
  uint64_t GetLatestObservedTimestamp() const;

  /// Sets the latest observed HybridTime timestamp.
  ///
  /// This is only useful when forwarding timestamps between clients
  /// to enforce external consistency when using KuduSession::CLIENT_PROPAGATED
  /// external consistency mode.
  ///
  /// The HybridTime encoded timestamp should be obtained from another client's
  /// KuduClient::GetLatestObservedTimestamp() method.
  ///
  /// @note This method is experimental and will either disappear or
  ///   change in a future release.
  ///
  /// @param [in] ht_timestamp
  ///   Timestamp encoded in HybridTime format.
  void SetLatestObservedTimestamp(uint64_t ht_timestamp);

  /// Export the current authentication credentials from this client. This includes
  /// the necessary credentials to authenticate to the cluster, as well as to
  /// authenticate the cluster to the client.
  ///
  /// The resulting binary string may be passed into a new C++ client via the
  /// @c KuduClientBuilder::import_authentication_credentials method, or into a new
  /// Java client via @c KuduClient#importAuthenticationCredentials.
  ///
  /// @param [out] authn_creds
  ///   The resulting binary authentication credentials.
  /// @return Status object for the operation.
  Status ExportAuthenticationCredentials(std::string* authn_creds) const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class internal::Batcher;
  friend class internal::GetTableSchemaRpc;
  friend class internal::LookupRpc;
  friend class internal::MetaCache;
  friend class internal::RemoteTablet;
  friend class internal::RemoteTabletServer;
  friend class internal::WriteRpc;
  friend class ClientTest;
  friend class KuduClientBuilder;
  friend class KuduPartitionerBuilder;
  friend class KuduScanner;
  friend class KuduScanToken;
  friend class KuduScanTokenBuilder;
  friend class KuduSession;
  friend class KuduTable;
  friend class KuduTableAlterer;
  friend class KuduTableCreator;
  friend class ::kudu::SecurityUnknownTskTest;
  friend class tools::LeaderMasterProxy;

  FRIEND_TEST(kudu::ClientStressTest, TestUniqueClientIds);
  FRIEND_TEST(ClientTest, TestGetSecurityInfoFromMaster);
  FRIEND_TEST(ClientTest, TestGetTabletServerBlacklist);
  FRIEND_TEST(ClientTest, TestMasterDown);
  FRIEND_TEST(ClientTest, TestMasterLookupPermits);
  FRIEND_TEST(ClientTest, TestMetaCacheExpiry);
  FRIEND_TEST(ClientTest, TestNonCoveringRangePartitions);
  FRIEND_TEST(ClientTest, TestReplicatedTabletWritesWithLeaderElection);
  FRIEND_TEST(ClientTest, TestScanFaultTolerance);
  FRIEND_TEST(ClientTest, TestScanTimeout);
  FRIEND_TEST(ClientTest, TestWriteWithDeadMaster);
  FRIEND_TEST(MasterFailoverTest, TestPauseAfterCreateTableIssued);

  KuduClient();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduClient);
};

/// @brief In-memory representation of a remote tablet server.
class KUDU_EXPORT KuduTabletServer {
 public:
  ~KuduTabletServer();

  /// @return The UUID which is globally unique and guaranteed not to change
  ///   for the lifetime of the tablet server.
  const std::string& uuid() const;

  /// @return Hostname of the first RPC address that this tablet server
  ///   is listening on.
  const std::string& hostname() const;

  /// @return Port number of the first RPC address that this tablet server
  ///   is listening on.
  uint16_t port() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;
  friend class KuduScanner;
  friend class KuduScanTokenBuilder;

  KuduTabletServer();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTabletServer);
};

/// @brief In-memory representation of a remote tablet's replica.
class KUDU_EXPORT KuduReplica {
 public:
  ~KuduReplica();

  /// @return Whether or not this replica is a Raft leader.
  ///
  /// @note This information may be stale; the role of a replica may change at
  /// any time.
  bool is_leader() const;

  /// @return The tablet server hosting this remote replica.
  const KuduTabletServer& ts() const;

 private:
  friend class KuduClient;
  friend class KuduScanTokenBuilder;

  class KUDU_NO_EXPORT Data;

  KuduReplica();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduReplica);
};

/// @brief In-memory representation of a remote tablet.
class KUDU_EXPORT KuduTablet {
 public:
  ~KuduTablet();

  /// @return The ID which is globally unique and guaranteed not to change
  ///    for the lifetime of the tablet.
  const std::string& id() const;

  /// @return The replicas of this tablet. The KuduTablet retains ownership
  /// over the replicas.
  ///
  /// @note This information may be stale; replicas may be added or removed
  /// from Raft configurations at any time.
  const std::vector<const KuduReplica*>& replicas() const;

 private:
  friend class KuduClient;
  friend class KuduScanTokenBuilder;

  class KUDU_NO_EXPORT Data;

  KuduTablet();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTablet);
};

/// @brief A helper class to create a new table with the desired options.
class KUDU_EXPORT KuduTableCreator {
 public:
  ~KuduTableCreator();

  /// Set name for the table.
  ///
  /// @param [in] name
  ///   Name of the target table.
  /// @return Reference to the modified table creator.
  ///
  /// @remark Calling this method and setting the name for the table-to-be
  ///   is one of the pre-conditions for calling KuduTableCreator::Create()
  ///   method.
  ///
  /// @todo Should name of the table be a constructor's parameter instead?
  KuduTableCreator& table_name(const std::string& name);

  /// Set the schema with which to create the table.
  ///
  /// @param [in] schema
  ///   Schema to use. Must remain valid for the lifetime of the builder.
  ///   Must be non-NULL.
  /// @return Reference to the modified table creator.
  ///
  /// @remark Calling this method and setting schema for the table-to-be
  ///   is one of the pre-conditions for calling KuduTableCreator::Create()
  ///   method.
  KuduTableCreator& schema(const KuduSchema* schema);

  /// Add a set of hash partitions to the table.
  ///
  /// Tables must be created with either range, hash, or range and hash
  /// partitioning.
  ///
  /// For each set of hash partitions added to the table, the total number of
  /// tablets is multiplied by the number of buckets. For example,
  /// if a table is created with 3 split rows, and 2 hash partitions
  /// with 4 and 5 buckets respectively, the total number of tablets
  /// will be 80 (4 range partitions * 4 hash buckets * 5 hash buckets).
  ///
  /// @param [in] columns
  ///   Names of columns to use for partitioning.
  /// @param [in] num_buckets
  ///   Number of buckets for the hashing.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_hash_partitions(const std::vector<std::string>& columns,
                                        int32_t num_buckets);

  /// Add a set of hash partitions to the table (with seed).
  ///
  /// This method is exactly the same as add_hash_partitions() above, with
  /// the exception of additional seed value, which can be used to randomize
  /// the mapping of rows to hash buckets. Setting the seed may provide some
  /// amount of protection against denial of service attacks when the hashed
  /// columns contain user provided values.
  ///
  /// @param [in] columns
  ///   Names of columns to use for partitioning.
  /// @param [in] num_buckets
  ///   Number of buckets for the hashing.
  /// @param [in] seed
  ///   Hash: seed for mapping rows to hash buckets.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_hash_partitions(const std::vector<std::string>& columns,
                                        int32_t num_buckets, int32_t seed);

  /// Set the columns on which the table will be range-partitioned.
  ///
  /// Tables must be created with either range, hash, or range and hash
  /// partitioning. To force the use of a single tablet (not recommended),
  /// call this method with an empty vector and set no split rows and no hash
  /// partitions.
  ///
  /// @param [in] columns
  ///   Names of columns to use for partitioning. Every column must be
  ///   a part of the table's primary key. If not set, or if called with
  ///   an empty vector, the table will be created without range partitioning.
  /// @return Reference to the modified table creator.
  KuduTableCreator& set_range_partition_columns(const std::vector<std::string>& columns);

  /// Range partition bound type.
  enum RangePartitionBound {
    EXCLUSIVE_BOUND, ///< An exclusive bound.
    INCLUSIVE_BOUND, ///< An inclusive bound.
  };

  /// Add a range partition to the table.
  ///
  /// Multiple range partitions may be added, but they must not overlap. All
  /// range splits specified by @c add_range_partition_split must fall in a
  /// range partition. The lower bound must be less than or equal to the upper
  /// bound.
  ///
  /// If this method is not called, the table's range will be unbounded.
  ///
  /// @param [in] lower_bound
  ///   Row to use as a lower bound. The KuduTableCreator instance takes
  ///   ownership of this parameter. If row is empty, no lower bound is imposed
  ///   on the table range. If a column of the @c lower_bound row is missing
  ///   a value, the logical minimum value for that column type is used as the
  ///   default.
  /// @param [in] upper_bound
  ///   Row to use as an upper bound. The KuduTableCreator instance takes
  ///   ownership of this parameter. If row is empty, no upper bound is imposed
  ///   on the table range. If a column of the @c upper_bound row is missing
  ///   a value, the logical maximum value for that column type is used as the
  ///   default.
  /// @param [in] lower_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   inclusive.
  /// @param [in] upper_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   exclusive.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_range_partition(KuduPartialRow* lower_bound,
                                        KuduPartialRow* upper_bound,
                                        RangePartitionBound lower_bound_type = INCLUSIVE_BOUND,
                                        RangePartitionBound upper_bound_type = EXCLUSIVE_BOUND);

  /// Add a range partition split at the provided row.
  ///
  /// @param [in] split_row
  ///   The row to use for partitioning. If the row is missing a value
  ///   for any of the range partition columns, the logical minimum value
  ///   for that column type will be used by default.
  ///   The KuduTableCreator object takes ownership of the parameter.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_range_partition_split(KuduPartialRow* split_row);

  /// @deprecated Use @c add_range_partition_split() instead.
  ///
  /// @param [in] split_rows
  ///   The row to use for partitioning.
  /// @return Reference to the modified table creator.
  KuduTableCreator& split_rows(const std::vector<const KuduPartialRow*>& split_rows)
      ATTRIBUTE_DEPRECATED("use add_range_partition_split() instead");

  /// Set the table replication factor.
  ///
  /// Replicated tables can continue to read and write data while a majority
  /// of replicas are not failed.
  ///
  /// @param [in] n_replicas
  ///   Number of replicas to set. This should be an odd number.
  ///   If not provided (or if <= 0), falls back to the server-side default.
  /// @return Reference to the modified table creator.
  KuduTableCreator& num_replicas(int n_replicas);

  /// Set the timeout for the table creation operation.
  ///
  /// This includes any waiting after the create has been submitted
  /// (i.e. if the create is slow to be performed for a large table,
  ///  it may time out and then later be successful).
  ///
  /// @param [in] timeout
  ///   Timeout to set.
  /// @return Reference to the modified table creator.
  KuduTableCreator& timeout(const MonoDelta& timeout);

  /// Wait for the table to be fully created before returning.
  ///
  /// If not called, defaults to @c true.
  ///
  /// @param [in] wait
  ///   Whether to wait for completion of operations.
  /// @return Reference to the modified table creator.
  KuduTableCreator& wait(bool wait);

  /// Create a table in accordance with parameters currently set for the
  /// KuduTableCreator instance. Once created, the table handle
  /// can be obtained using KuduClient::OpenTable() method.
  ///
  /// @pre The following methods of the KuduTableCreator must be called
  ///   prior to invoking this method:
  ///     @li table_name()
  ///     @li schema()
  ///
  /// @return Result status of the <tt>CREATE TABLE</tt> operation.
  ///   The return value may indicate an error in the create table operation,
  ///   or a misuse of the builder. In the latter case, only the last error
  ///   is returned.
  Status Create();

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;

  explicit KuduTableCreator(KuduClient* client);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTableCreator);
};

/// @brief A representation of a table on a particular cluster.
///
/// A KuduTable holds the current schema of the table. Any given KuduTable
/// object belongs to a specific KuduClient object.
///
/// Upon construction, the table is looked up in the catalog (or catalog cache),
/// and the schema fetched for introspection.
///
/// This class is also a factory for write operation on the table.
/// The provided operations are:
///   @li INSERT
///     Adds a new row. Fails if the row already exists.
///   @li UPSERT
///     Adds a new row. If there's an existing row, updates it.
///   @li UPDATE
///     Updates an existing row. Fails if the row does not exist.
///   @li DELETE
///     Deletes an existing row. Fails if the row does not exist.
///
/// @note This class is thread-safe.
class KUDU_EXPORT KuduTable : public sp::enable_shared_from_this<KuduTable> {
 public:
  ~KuduTable();

  /// @return Name of the table.
  const std::string& name() const;

  /// Get the table's ID.
  ///
  /// This is an internal identifier which uniquely identifies a table.
  /// If the table is deleted and recreated with the same
  /// name, the ID will distinguish the old table from the new.
  ///
  /// @return Identifier string for the table.
  const std::string& id() const;

  /// @return Reference to the table's schema object.
  const KuduSchema& schema() const;

  /// @return Replication factor of the table.
  int num_replicas() const;

  /// @return New @c INSERT operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduInsert* NewInsert();

  /// @return New @c UPSERT operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpsert* NewUpsert();

  /// @return New @c UPDATE operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpdate* NewUpdate();

  /// @return New @c DELETE operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduDelete* NewDelete();

  /// Create a new comparison predicate.
  ///
  /// This method creates new instance of a comparison predicate which
  /// can be used for scanners on this table object.
  ///
  /// @param [in] col_name
  ///   Name of column to use for comparison.
  /// @param [in] op
  ///   Comparison operator to use.
  /// @param [in] value
  ///   The type of the value must correspond to the type of the column
  ///   to which the predicate is to be applied. For example,
  ///   if the given column is any type of integer, the KuduValue should
  ///   also be an integer, with its value in the valid range
  ///   for the column type. No attempt is made to cast between floating point
  ///   and integer values, or numeric and string values.
  /// @return Raw pointer to instance of comparison predicate. The caller owns
  ///   the result until it is passed into KuduScanner::AddConjunctPredicate().
  ///   The returned predicate object takes ownership over the @c value
  ///   Non-NULL is returned both in success and error cases.
  ///   In the case of an error (e.g. invalid column name), a non-NULL value
  ///   is still returned. The error will be returned when attempting
  ///   to add this predicate to a KuduScanner.
  KuduPredicate* NewComparisonPredicate(const Slice& col_name,
                                        KuduPredicate::ComparisonOp op,
                                        KuduValue* value);

  /// Create a new IN list predicate which can be used for scanners on this
  /// table.
  ///
  /// The IN list predicate is used to specify a list of values that a column
  /// must match. A row is filtered from the scan if the value of the column
  /// does not equal any value from the list.
  ///
  /// The type of entries in the list must correspond to the type of the column
  /// to which the predicate is to be applied. For example, if the given column
  /// is any type of integer, the KuduValues should also be integers, with the
  /// values in the valid range for the column type. No attempt is made to cast
  /// between floating point and integer values, or numeric and string values.
  ///
  /// @param [in] col_name
  ///   Name of the column to which the predicate applies.
  /// @param [in] values
  ///   Vector of values which the column will be matched against.
  /// @return Raw pointer to an IN list predicate. The caller owns the predicate
  ///   until it is passed into KuduScanner::AddConjunctPredicate(). The
  ///   returned predicate takes ownership of the values vector and its
  ///   elements. In the case of an error (e.g. an invalid column name), a
  ///   non-NULL value is still returned. The error will be returned when
  ///   attempting to add this predicate to a KuduScanner.
  KuduPredicate* NewInListPredicate(const Slice& col_name,
                                    std::vector<KuduValue*>* values);

  /// Create a new IS NOT NULL predicate which can be used for scanners on this
  /// table.
  ///
  /// @param [in] col_name
  ///   Name of the column to which the predicate applies
  /// @return Raw pointer to an IS NOT NULL predicate. The caller owns the
  ///   predicate until it is passed into KuduScanner::AddConjunctPredicate().
  ///   In the case of an error (e.g. an invalid column name), a non-NULL
  ///   value is still returned. The error will be returned when attempting
  ///   to add this predicate to a KuduScanner.
  KuduPredicate* NewIsNotNullPredicate(const Slice& col_name);

  /// Create a new IS NULL predicate which can be used for scanners on this
  /// table.
  ///
  /// @param [in] col_name
  ///   Name of the column to which the predicate applies
  /// @return Raw pointer to an IS NULL predicate. The caller owns the
  ///   predicate until it is passed into KuduScanner::AddConjunctPredicate().
  ///   In the case of an error (e.g. an invalid column name), a non-NULL
  ///   value is still returned. The error will be returned when attempting
  ///   to add this predicate to a KuduScanner.
  KuduPredicate* NewIsNullPredicate(const Slice& col_name);

  /// @return The KuduClient object associated with the table. The caller
  ///   should not free the returned pointer.
  KuduClient* client() const;

  /// @return The partition schema for the table.
  const PartitionSchema& partition_schema() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;
  friend class KuduPartitioner;

  KuduTable(const sp::shared_ptr<KuduClient>& client,
            const std::string& name,
            const std::string& id,
            int num_replicas,
            const KuduSchema& schema,
            const PartitionSchema& partition_schema);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTable);
};

/// @brief Alters an existing table based on the provided steps.
///
/// Create a new instance of a table alterer using
/// KuduClient::NewTableAlterer(). An example of usage:
/// @code
/// std::unique_ptr<KuduTableAlterer> alterer(
///   client->NewTableAlterer("table-name"));
/// alterer->AddColumn("foo")->Type(KuduColumnSchema::INT32)->NotNull();
/// alterer->AlterColumn("bar")->Compression(KuduColumnStorageAttributes::LZ4);
/// Status s = alterer->Alter();
/// @endcode
class KUDU_EXPORT KuduTableAlterer {
 public:
  ~KuduTableAlterer();

  /// Rename the table.
  ///
  /// @param [in] new_name
  ///   The new name for the table.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* RenameTo(const std::string& new_name);

  /// Add a new column to the table.
  ///
  /// When adding a column, you must specify the default value of the new
  /// column using KuduColumnSpec::DefaultValue(...).
  ///
  /// @param name
  ///   Name of the column do add.
  /// @return Pointer to the result ColumnSpec object. The alterer keeps
  ///   ownership of the newly created object.
  KuduColumnSpec* AddColumn(const std::string& name);

  /// Alter an existing column.
  ///
  /// @note The column may not be in the primary key.
  ///
  /// @param [in] name
  ///   Name of the column to alter.
  /// @return Pointer to the result ColumnSpec object. The alterer keeps
  ///   ownership of the newly created object.
  KuduColumnSpec* AlterColumn(const std::string& name);

  /// Drops an existing column from the table.
  ///
  /// @note The column may not be in the primary key.
  ///
  /// @param [in] name
  ///   Name of the column to alter.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* DropColumn(const std::string& name);

  /// Add a range partition to the table with the specified lower bound and
  /// upper bound.
  ///
  /// @note The table alterer takes ownership of the rows.
  ///
  /// @note Multiple range partitions may be added as part of a single alter
  ///   table transaction by calling this method multiple times on the table
  ///   alterer.
  ///
  /// @note This client may immediately write and scan the new tablets when
  ///   Alter() returns success, however other existing clients may have to wait
  ///   for a timeout period to elapse before the tablets become visible. This
  ///   period is configured by the master's 'table_locations_ttl_ms' flag, and
  ///   defaults to 5 minutes.
  ///
  /// @param [in] lower_bound
  ///   The lower bound of the range partition to add. If the row is empty, then
  ///   the lower bound is unbounded. If any of the columns are unset, the
  ///   logical minimum value for the column's type will be used by default.
  /// @param [in] upper_bound
  ///   The upper bound of the range partition to add. If the row is empty, then
  ///   the upper bound is unbounded. If any of the individual columns are
  ///   unset, the logical minimum value for the column' type will be used by
  ///   default.
  /// @param [in] lower_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   inclusive.
  /// @param [in] upper_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   exclusive.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* AddRangePartition(
      KuduPartialRow* lower_bound,
      KuduPartialRow* upper_bound,
      KuduTableCreator::RangePartitionBound lower_bound_type = KuduTableCreator::INCLUSIVE_BOUND,
      KuduTableCreator::RangePartitionBound upper_bound_type = KuduTableCreator::EXCLUSIVE_BOUND);

  /// Drop the range partition from the table with the specified lower bound and
  /// upper bound. The bounds must match an existing range partition exactly,
  /// and may not span multiple range partitions.
  ///
  /// @note The table alterer takes ownership of the rows.
  ///
  /// @note Multiple range partitions may be dropped as part of a single alter
  ///   table transaction by calling this method multiple times on the table
  ///   alterer.
  ///
  /// @param [in] lower_bound
  ///   The inclusive lower bound of the range partition to drop. If the row is
  ///   empty, then the lower bound is unbounded. If any of the columns are
  ///   unset, the logical minimum value for the column's type will be used by
  ///   default.
  /// @param [in] upper_bound
  ///   The exclusive upper bound of the range partition to add. If the row is
  ///   empty, then the upper bound is unbounded. If any of the individual
  ///   columns are unset, the logical minimum value for the column' type will
  ///   be used by default.
  /// @param [in] lower_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   inclusive.
  /// @param [in] upper_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   exclusive.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* DropRangePartition(
      KuduPartialRow* lower_bound,
      KuduPartialRow* upper_bound,
      KuduTableCreator::RangePartitionBound lower_bound_type = KuduTableCreator::INCLUSIVE_BOUND,
      KuduTableCreator::RangePartitionBound upper_bound_type = KuduTableCreator::EXCLUSIVE_BOUND);

  /// Set a timeout for the alteration operation.
  ///
  /// This includes any waiting after the alter has been submitted
  /// (i.e. if the alter is slow to be performed on a large table,
  ///  it may time out and then later be successful).
  ///
  /// @param [in] timeout
  ///   Timeout to set.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* timeout(const MonoDelta& timeout);

  /// Whether to wait for completion of alteration operations.
  ///
  /// If set to @c true, an alteration operation returns control only after
  /// the operation is complete. Otherwise, every operation returns immediately.
  /// By default (i.e. when an alteration object is created)
  /// it is set to @c true.
  ///
  /// @param [in] wait
  ///   Whether to wait for alteration operation to complete before
  ///   returning control.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* wait(bool wait);

  /// @return Status of the ALTER TABLE operation. The return value
  ///   may indicate an error in the alter operation,
  ///   or a misuse of the builder (e.g. add_column() with default_value=NULL).
  ///   In the latter case, only the last error is returned.
  Status Alter();

 private:
  class KUDU_NO_EXPORT Data;
  friend class KuduClient;

  KuduTableAlterer(KuduClient* client,
                   const std::string& name);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTableAlterer);
};

/// @brief This class represents an error which occurred in a write operation.
///
/// Using an instance of this class, it is possible to track error details
/// such as the operation which caused the error, along with whatever
/// the actual error was.
class KUDU_EXPORT KuduError {
 public:
  ~KuduError();

  /// @return The actual error which occurred.
  const Status& status() const;

  /// @return The operation which failed.
  const KuduWriteOperation& failed_op() const;

  /// Release the operation that failed.
  ///
  /// This method must be called only once on an instance
  /// of the KuduError class.
  ///
  /// @return Raw pointer to write operation object. The caller
  ///   takes ownership of the returned object.
  KuduWriteOperation* release_failed_op();

  /// Check if there is a chance that the requested operation was successful.
  ///
  /// In some cases, it is possible that the server did receive and successfully
  /// perform the requested operation, but the client can't tell whether or not
  /// it was successful. For example, if the call times out, the server may
  /// still succeed in processing at a later time.
  ///
  /// @return This function returns @c true if there is some chance that
  ///   the server did process the operation, and @c false if it can guarantee
  ///   that the operation did not succeed.
  bool was_possibly_successful() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class internal::Batcher;
  friend class internal::ErrorCollector;
  friend class KuduSession;

  KuduError(KuduWriteOperation* failed_op, const Status& error);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduError);
};


/// @brief Representation of a Kudu client session.
///
/// A KuduSession belongs to a specific KuduClient, and represents a context in
/// which all read/write data access should take place. Within a session,
/// multiple operations may be accumulated and batched together for better
/// efficiency. Settings like timeouts, priorities, and trace IDs are also set
/// per session.
///
/// A KuduSession's main purpose is for grouping together multiple data-access
/// operations together into batches or transactions. It is important to note
/// the distinction between these two:
///
/// @li A batch is a set of operations which are grouped together in order to
///   amortize fixed costs such as RPC call overhead and round trip times.
///   A batch DOES NOT imply any ACID-like guarantees. Within a batch, some
///   operations may succeed while others fail, and concurrent readers may see
///   partial results. If the client crashes mid-batch, it is possible that
///   some of the operations will be made durable while others were lost.
/// @li In contrast, a transaction is a set of operations which are treated
///   as an indivisible semantic unit, per the usual definitions of database
///   transactions and isolation levels.
///
/// @note Kudu does not currently support transactions!  They are only mentioned
///   in the above documentation to clarify that batches are not transactional
///   and should only be used for efficiency.
///
/// KuduSession is separate from KuduTable because a given batch or transaction
/// may span multiple tables. This is particularly important in the future when
/// we add ACID support, but even in the context of batching, we may be able to
/// coalesce writes to different tables hosted on the same server into the same
/// RPC.
///
/// KuduSession is separate from KuduClient because, in a multi-threaded
/// application, different threads may need to concurrently execute
/// transactions. Similar to a JDBC "session", transaction boundaries will be
/// delineated on a per-session basis -- in between a "BeginTransaction" and
/// "Commit" call on a given session, all operations will be part of the same
/// transaction. Meanwhile another concurrent Session object can safely run
/// non-transactional work or other transactions without interfering.
///
/// Additionally, there is a guarantee that writes from different sessions
/// do not get batched together into the same RPCs -- this means that
/// latency-sensitive clients can run through the same KuduClient object
/// as throughput-oriented clients, perhaps by setting the latency-sensitive
/// session's timeouts low and priorities high. Without the separation
/// of batches, a latency-sensitive single-row insert might get batched along
/// with 10MB worth of inserts from the batch writer, thus delaying
/// the response significantly.
///
/// Though we currently do not have transactional support, users will be forced
/// to use a KuduSession to instantiate reads as well as writes. This will make
/// it more straight-forward to add RW transactions in the future without
/// significant modifications to the API.
///
/// Users who are familiar with the Hibernate ORM framework should find this
/// concept of a Session familiar.
///
/// @note This class is not thread-safe.
class KUDU_EXPORT KuduSession : public sp::enable_shared_from_this<KuduSession> {
 public:
  ~KuduSession();

  /// Modes of flush operations.
  enum FlushMode {
    /// Every write will be sent to the server in-band with the Apply()
    /// call. No batching will occur. In this mode, the Flush() call never
    /// has any effect, since each Apply() call has already flushed the buffer.
    /// This is the default flush mode.
    AUTO_FLUSH_SYNC,

    /// Apply() calls will return immediately (unless there is not enough
    /// buffer space to accommodate the newly added operations), but
    /// the writes will be sent in the background, potentially batched together
    /// with other writes from the same session. If there is not sufficient
    /// buffer space, Apply() blocks for buffer space to become available.
    ///
    /// Because writes are applied in the background, any errors will be stored
    /// in a session-local buffer. Call CountPendingErrors() or
    /// GetPendingErrors() to retrieve them.
    ///
    /// In this mode, calling the FlushAsync() or Flush() methods causes a flush
    /// that normally would have happened at some point in the near future
    /// to happen right now. The Flush() call can be used to block until
    /// the current batch is sent and the reclaimed space is available
    /// for new operations.
    ///
    /// @attention The @c AUTO_FLUSH_BACKGROUND mode, when used in conjunction
    ///   with a KuduSession::SetMutationBufferMaxNum() of greater than 1
    ///   (the default is 2), may result in out-of-order writes. This
    ///   is because the buffers may flush concurrently, so multiple write
    ///   operations may be sent to the server in parallel.
    ///   See [KUDU-1767](https://issues.apache.org/jira/browse/KUDU-1767) for
    ///   more information.
    ///
    /// @todo Provide an API for the user to specify a callback to do their own
    ///   error reporting.
    AUTO_FLUSH_BACKGROUND,

    /// Apply() calls will return immediately, and the writes will not be
    /// sent until the user calls Flush(). If the buffer runs past the
    /// configured space limit, then Apply() will return an error.
    ///
    /// @attention The @c MANUAL_FLUSH mode, when used in conjunction
    ///   with a KuduSession::SetMutationBufferMaxNum() of greater than 1
    ///   (the default is 2), may result in out-of-order writes if
    ///   KuduSession::FlushAsync() is used. This is because the buffers may
    ///   flush concurrently, so multiple write operations may be sent to the
    ///   server in parallel.
    ///   See [KUDU-1767](https://issues.apache.org/jira/browse/KUDU-1767) for
    ///   more information.
    MANUAL_FLUSH
  };

  /// Set the flush mode.
  ///
  /// @pre There should be no pending writes -- call Flush() first
  ///   to ensure nothing is pending.
  ///
  /// @param [in] m
  ///   Flush mode to set.
  /// @return Operation status.
  Status SetFlushMode(FlushMode m) WARN_UNUSED_RESULT;

  /// The possible external consistency modes on which Kudu operates.
  enum ExternalConsistencyMode {
    /// The response to any write will contain a timestamp. Any further calls
    /// from the same client to other servers will update those servers
    /// with that timestamp. Following write operations from the same client
    /// will be assigned timestamps that are strictly higher, enforcing external
    /// consistency without having to wait or incur any latency penalties.
    ///
    /// In order to maintain external consistency for writes between
    /// two different clients in this mode, the user must forward the timestamp
    /// from the first client to the second by using
    /// KuduClient::GetLatestObservedTimestamp() and
    /// KuduClient::SetLatestObservedTimestamp().
    ///
    /// This is the default external consistency mode.
    ///
    /// @warning
    ///   Failure to propagate timestamp information through back-channels
    ///   between two different clients will negate any external consistency
    ///   guarantee under this mode.
    CLIENT_PROPAGATED,

    /// The server will guarantee that write operations from the same or from
    /// other client are externally consistent, without the need to propagate
    /// timestamps across clients. This is done by making write operations
    /// wait until there is certainty that all follow up write operations
    /// (operations that start after the previous one finishes)
    /// will be assigned a timestamp that is strictly higher, enforcing external
    /// consistency.
    ///
    /// @warning
    ///   Depending on the clock synchronization state of TabletServers this may
    ///   imply considerable latency. Moreover operations in @c COMMIT_WAIT
    ///   external consistency mode will outright fail if TabletServer clocks
    ///   are either unsynchronized or synchronized but with a maximum error
    ///   which surpasses a pre-configured threshold.
    COMMIT_WAIT
  };

  /// Set external consistency mode for the session.
  ///
  /// @param [in] m
  ///   External consistency mode to set.
  /// @return Operation result status.
  Status SetExternalConsistencyMode(ExternalConsistencyMode m)
    WARN_UNUSED_RESULT;

  /// Set the amount of buffer space used by this session for outbound writes.
  ///
  /// The effect of the buffer size varies based on the flush mode of
  /// the session:
  /// @li AUTO_FLUSH_SYNC
  ///   since no buffering is done, this has no effect.
  /// @li AUTO_FLUSH_BACKGROUND
  ///   if the buffer space is exhausted, then write calls will block until
  ///   there is space available in the buffer.
  /// @li MANUAL_FLUSH
  ///   if the buffer space is exhausted, then write calls will return an error
  ///
  /// By default, the buffer space is set to 7 MiB (i.e. 7 * 1024 * 1024 bytes).
  ///
  /// @param [in] size_bytes
  ///   Size of the buffer space to set (number of bytes).
  /// @return Operation result status.
  Status SetMutationBufferSpace(size_t size_bytes) WARN_UNUSED_RESULT;

  /// Set the buffer watermark to trigger flush in AUTO_FLUSH_BACKGROUND mode.
  ///
  /// This method sets the watermark for fresh operations in the buffer
  /// when running in AUTO_FLUSH_BACKGROUND mode: once the specified threshold
  /// is reached, the session starts sending the accumulated write operations
  /// to the appropriate tablet servers. By default, the buffer flush watermark
  /// is to to 50%.
  ///
  /// @note This setting is applicable only for AUTO_FLUSH_BACKGROUND sessions.
  ///   I.e., calling this method in other flush modes is safe, but
  ///   the parameter has no effect until the session is switched into
  ///   AUTO_FLUSH_BACKGROUND mode.
  ///
  /// @note The buffer contains data for fresh (i.e. newly submitted)
  ///   operations and also operations which are scheduled for flush or being
  ///   flushed. The flush watermark determines how much of the buffer space
  ///   is taken by newly submitted operations. Setting this level to 1.0
  ///   (i.e. 100%) results in flushing the buffer only when the newly applied
  ///   operation would overflow the buffer.
  ///
  /// @param [in] watermark_pct
  ///   Watermark level as percentage of the mutation buffer size.
  /// @return Operation result status.
  Status SetMutationBufferFlushWatermark(double watermark_pct)
      WARN_UNUSED_RESULT;

  /// Set the interval for time-based flushing of the mutation buffer.
  ///
  /// In some cases, while running in AUTO_FLUSH_BACKGROUND mode, the size
  /// of the mutation buffer for pending operations and the flush watermark
  /// for fresh operations may be too high for the rate of incoming data:
  /// it would take too long to accumulate enough data in the buffer to trigger
  /// flushing. I.e., it makes sense to flush the accumulated operations
  /// if the prior flush happened long time ago. This method sets the wait
  /// interval for the time-based flushing which takes place along with
  /// the flushing triggered by the over-the-watermark criterion.
  /// By default, the interval is set to 1000 ms (i.e. 1 second).
  ///
  /// @note This setting is applicable only for AUTO_FLUSH_BACKGROUND sessions.
  ///   I.e., calling this method in other flush modes is safe, but
  ///   the parameter has no effect until the session is switched into
  ///   AUTO_FLUSH_BACKGROUND mode.
  ///
  /// @param [in] millis
  ///   The duration of the interval for the time-based flushing,
  ///   in milliseconds.
  /// @return Operation result status.
  Status SetMutationBufferFlushInterval(unsigned int millis) WARN_UNUSED_RESULT;

  /// Set the maximum number of mutation buffers per KuduSession object.
  ///
  /// A KuduSession accumulates write operations submitted via the Apply()
  /// method in mutation buffers. A KuduSession always has at least one
  /// mutation buffer. The mutation buffer which accumulates new incoming
  /// operations is called the <em>current mutation buffer</em>.
  /// The current mutation buffer is flushed either explicitly using
  /// the KuduSession::Flush() and/or KuduSession::FlushAsync() methods
  /// or it's done by the KuduSession automatically if running in
  /// AUTO_FLUSH_BACKGROUND mode. After flushing the current mutation buffer,
  /// a new buffer is created upon calling KuduSession::Apply(),
  /// provided the limit is not exceeded. A call to KuduSession::Apply() blocks
  /// if it's at the maximum number of buffers allowed; the call unblocks
  /// as soon as one of the pending batchers finished flushing and a new batcher
  /// can be created.
  ///
  /// The minimum setting for this parameter is 1 (one).
  /// The default setting for this parameter is 2 (two).
  ///
  /// @param [in] max_num
  ///   The maximum number of mutation buffers per KuduSession object
  ///   to hold the applied operations. Use @c 0 to set the maximum number
  ///   of concurrent mutation buffers to unlimited.
  /// @return Operation result status.
  Status SetMutationBufferMaxNum(unsigned int max_num) WARN_UNUSED_RESULT;

  /// Set the timeout for writes made in this session.
  ///
  /// @param [in] millis
  ///   Timeout to set in milliseconds; should be greater or equal to 0.
  ///   If the parameter value is less than 0, it's implicitly set to 0.
  void SetTimeoutMillis(int millis);

  /// @todo
  ///   Add "doAs" ability here for proxy servers to be able to act on behalf of
  ///   other users, assuming access rights.

  /// Apply the write operation.
  ///
  /// The behavior of this function depends on the current flush mode.
  /// Regardless of flush mode, however, Apply() may begin to perform processing
  /// in the background for the call (e.g. looking up the tablet, etc).
  /// Given that, an error may be queued into the PendingErrors structure prior
  /// to flushing, even in @c MANUAL_FLUSH mode.
  ///
  /// In case of any error, which may occur during flushing or because
  /// the write_op is malformed, the write_op is stored in the session's error
  /// collector which may be retrieved at any time.
  ///
  /// A KuduSession accumulates write operations submitted via the Apply()
  /// method in mutation buffers. A KuduSession always has at least one
  /// mutation buffer. In any flush mode, this call may block if the maximum
  /// number of mutation buffers per session is reached
  /// (use KuduSession::SetMutationBufferMaxNum() to set the limit
  /// on maximum number of batchers).
  ///
  /// @param [in] write_op
  ///   Operation to apply. This method transfers the write_op's ownership
  ///   to the KuduSession.
  /// @return Operation result status.
  Status Apply(KuduWriteOperation* write_op) WARN_UNUSED_RESULT;

  /// Flush any pending writes.
  ///
  /// This method initiates flushing of the current batch of buffered
  /// write operations, if any, and then awaits for completion of all
  /// pending operations of the session. I.e., after successful return
  /// from this method no pending operations should be left in the session.
  ///
  /// In @c AUTO_FLUSH_SYNC mode, calling this method has no effect,
  /// since every Apply() call flushes itself inline.
  ///
  /// @return Operation result status. In particular, returns a non-OK status
  ///   if there are any pending errors after the rows have been flushed.
  ///   Callers should then use GetPendingErrors to determine which specific
  ///   operations failed.
  Status Flush() WARN_UNUSED_RESULT;

  /// Flush any pending writes asynchronously.
  ///
  /// This method schedules a background flush of the latest batch of buffered
  /// write operations. Provided callback is invoked upon the flush completion
  /// of the latest batch of buffered write operations.
  /// If there were errors while flushing the operations, corresponding
  /// 'not OK' status is passed as a parameter for the callback invocation.
  /// Callers should then use GetPendingErrors() to determine which specific
  /// operations failed.
  ///
  /// In the case that the async version of this method is used, then
  /// the callback will be called upon completion of the operations which
  /// were buffered since the last flush. In other words, in the following
  /// sequence:
  /// @code
  ///   session->Insert(a);
  ///   session->FlushAsync(callback_1);
  ///   session->Insert(b);
  ///   session->FlushAsync(callback_2);
  /// @endcode
  /// ... @c callback_2 will be triggered once @c b has been inserted,
  /// regardless of whether @c a has completed or not. That means there might be
  /// pending operations left in prior batches even after the callback
  /// has been invoked to report on the flush status of the latest batch.
  ///
  /// @note This also means that, if FlushAsync is called twice in succession,
  /// with no intervening operations, the second flush will return immediately.
  /// For example:
  /// @code
  ///   session->Insert(a);
  ///   session->FlushAsync(callback_1); // called when 'a' is inserted
  ///   session->FlushAsync(callback_2); // called immediately!
  /// @endcode
  /// Note that, as in all other async functions in Kudu, the callback
  /// may be called either from an IO thread or the same thread which calls
  /// FlushAsync. The callback should not block.
  ///
  /// @param [in] cb
  ///   Callback to call upon flush completion. The @c cb must remain valid
  ///   until it is invoked.
  void FlushAsync(KuduStatusCallback* cb);

  /// @return Status of the session closure. In particular, an error is returned
  ///   if there are non-flushed or in-flight operations.
  Status Close() WARN_UNUSED_RESULT;

  /// Check if there are any pending operations in this session.
  ///
  /// @return @c true if there are operations which have not yet been delivered
  ///   to the cluster. This may include buffered operations (i.e. those
  ///   that have not yet been flushed) as well as in-flight operations
  ///   (i.e. those that are in the process of being sent to the servers).
  ///
  /// @todo Maybe "incomplete" or "undelivered" is clearer?
  bool HasPendingOperations() const;

  /// Get number of buffered operations (not the same as 'pending').
  ///
  /// Note that this is different than HasPendingOperations() above,
  /// which includes operations which have been sent and not yet responded to.
  ///
  /// This method is most relevant in @c MANUAL_FLUSH mode, where
  /// the result count stays valid until next explicit flush or Apply() call.
  /// There is not much sense using this method in other flush modes:
  ///   @li in @c AUTO_FLUSH_SYNC mode, the data is immediately put en-route
  ///     to the destination by Apply() method itself, so this method always
  ///     returns zero.
  ///   @li in @c AUTO_FLUSH_BACKGROUND mode, the result count returned by
  ///     this method expires unpredictably and there isn't any guaranteed
  ///     validity interval for the result: the background flush task can run
  ///     any moment, invalidating the result.
  ///
  /// @deprecated This method is experimental and will disappear
  ///   in a future release.
  ///
  /// @return The number of buffered operations. These are operations that have
  ///   not yet been flushed -- i.e. they are not en-route yet.
  int CountBufferedOperations() const
      ATTRIBUTE_DEPRECATED("this method is experimental and will disappear "
                           "in a future release");

  /// Set limit on maximum buffer (memory) size used by this session's errors.
  /// By default, when a session is created, there is no limit on maximum size.
  ///
  /// The session's error buffer contains information on failed write
  /// operations. In most cases, the error contains the row which would be
  /// applied as is. If the error buffer space limit is set, the number of
  /// errors which fit into the buffer varies depending on error conditions,
  /// write operation types (insert/update/delete), and write operation
  /// row sizes.
  ///
  /// When the limit is set, the session will drop the first error that would
  /// overflow the buffer as well as all subsequent errors. To resume the
  /// accumulation of session errors, it's necessary to flush the current
  /// contents of the error buffer using the GetPendingErrors() method.
  ///
  /// @param [in] size_bytes
  ///   Limit on the maximum memory size consumed by collected session errors,
  ///   where @c 0 means 'unlimited'.
  /// @return Operation result status. An error is returned on an attempt
  ///   to set the limit on the buffer space if:
  ///   @li the session has already dropped at least one error since the last
  ///     call to the GetPendingErrors() method
  ///   @li the new limit is less than the amount of space occupied by already
  ///     accumulated errors.
  Status SetErrorBufferSpace(size_t size_bytes);

  /// Get error count for pending operations.
  ///
  /// Errors may accumulate in session's lifetime; use this method to
  /// see how many errors happened since last call of GetPendingErrors() method.
  /// The error count includes both the accumulated and dropped errors. An error
  /// might be dropped due to the limit on the error buffer size; see the
  /// SetErrorBufferSpace() method for details.
  ///
  /// @return Total count of errors accumulated during the session.
  int CountPendingErrors() const;

  /// Get information on errors from previous session activity.
  ///
  /// The information on errors are reset upon calling this method.
  ///
  /// @param [out] errors
  ///   Pointer to the container to fill with error info objects. Caller takes
  ///   ownership of the returned errors in the container.
  /// @param [out] overflowed
  ///   If there were more errors than could be held in the session's error
  ///   buffer, then @c overflowed is set to @c true.
  void GetPendingErrors(std::vector<KuduError*>* errors, bool* overflowed);

  /// @return Client for the session: pointer to the associated client object.
  KuduClient* client() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;
  friend class internal::Batcher;
  friend class ClientTest;
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundApplyBlocks);
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundAndErrorCollector);

  explicit KuduSession(const sp::shared_ptr<KuduClient>& client);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduSession);
};


/// @brief This class is a representation of a single scan.
///
/// @note This class is not thread-safe, though different scanners on different
///   threads may share a single KuduTable object.
class KUDU_EXPORT KuduScanner {
 public:
  /// The read modes for scanners.
  enum ReadMode {
    /// When @c READ_LATEST is specified the server will always return committed
    /// writes at the time the request was received. This type of read does not
    /// return a snapshot timestamp and is not repeatable.
    ///
    /// In ACID terms this corresponds to Isolation mode: "Read Committed"
    ///
    /// This is the default mode.
    READ_LATEST,

    /// When @c READ_AT_SNAPSHOT is specified the server will attempt to perform
    /// a read at the provided timestamp. If no timestamp is provided
    /// the server will take the current time as the snapshot timestamp.
    /// In this mode reads are repeatable, i.e. all future reads at the same
    /// timestamp will yield the same data. This is performed at the expense
    /// of waiting for in-flight transactions whose timestamp is lower than
    /// the snapshot's timestamp to complete, so it might incur
    /// a latency penalty. See KuduScanner::SetSnapshotMicros() and
    /// KuduScanner::SetSnapshotRaw() for details.
    ///
    /// In ACID terms this, by itself, corresponds to Isolation mode "Repeatable
    /// Read". If all writes to the scanned tablet are made externally
    /// consistent, then this corresponds to Isolation mode
    /// "Strict-Serializable".
    ///
    /// @note There are currently "holes", which happen in rare edge conditions,
    ///   by which writes are sometimes not externally consistent even when
    ///   action was taken to make them so. In these cases Isolation may
    ///   degenerate to mode "Read Committed". See KUDU-430.
    READ_AT_SNAPSHOT
  };

  /// Whether the rows should be returned in order.
  ///
  /// This affects the fault-tolerance properties of a scanner.
  enum OrderMode {
    /// Rows will be returned in an arbitrary order determined by the tablet
    /// server. This is efficient, but unordered scans are not fault-tolerant
    /// and cannot be resumed in the case of tablet server failure.
    ///
    /// This is the default mode.
    UNORDERED,

    /// Rows will be returned ordered by primary key. Sorting the rows imposes
    /// additional overhead on the tablet server, but means that scans are
    /// fault-tolerant and will be resumed at another tablet server
    /// in the case of a failure.
    ORDERED
  };

  /// Default scanner timeout.
  /// This is set to 3x the default RPC timeout returned by
  /// KuduClientBuilder::default_rpc_timeout().
  enum { kScanTimeoutMillis = 30000 };

  /// Constructor for KuduScanner.
  ///
  /// @param [in] table
  ///   The table to perfrom scan. The given object must remain valid
  ///   for the lifetime of this scanner object.
  explicit KuduScanner(KuduTable* table);
  ~KuduScanner();

  /// Set the projection for the scanner using column names.
  ///
  /// Set the projection used for the scanner by passing column names to read.
  /// This overrides any previous call to SetProjectedColumnNames() or
  /// SetProjectedColumnIndexes().
  ///
  /// @param [in] col_names
  ///   Column names to use for the projection.
  /// @return Operation result status.
  Status SetProjectedColumnNames(const std::vector<std::string>& col_names)
    WARN_UNUSED_RESULT;

  /// Set the column projection by passing the column indexes to read.
  ///
  /// Set the column projection used for this scanner by passing the column
  /// indices to read. A call to this method overrides any previous call to
  /// SetProjectedColumnNames() or SetProjectedColumnIndexes().
  ///
  /// @param [in] col_indexes
  ///   Column indices for the projection.
  /// @return Operation result status.
  Status SetProjectedColumnIndexes(const std::vector<int>& col_indexes)
    WARN_UNUSED_RESULT;

  /// @deprecated Use SetProjectedColumnNames() instead.
  ///
  /// @param [in] col_names
  ///   Column names to use for the projection.
  /// @return Operation result status.
  Status SetProjectedColumns(const std::vector<std::string>& col_names)
      WARN_UNUSED_RESULT
      ATTRIBUTE_DEPRECATED("use SetProjectedColumnNames() instead");

  /// Add a predicate for the scan.
  ///
  /// @param [in] pred
  ///   Predicate to set. The KuduScanTokenBuilder instance takes ownership
  ///   of the parameter even if a bad Status is returned. Multiple calls
  ///   of this method make the specified set of predicates work in conjunction,
  ///   i.e. all predicates must be true for a row to be returned.
  /// @return Operation result status.
  Status AddConjunctPredicate(KuduPredicate* pred) WARN_UNUSED_RESULT;

  /// Add a lower bound (inclusive) primary key for the scan.
  ///
  /// If any bound is already added, this bound is intersected with that one.
  ///
  /// @param [in] key
  ///   Lower bound primary key to add. The KuduScanTokenBuilder instance
  ///   does not take ownership of the parameter.
  /// @return Operation result status.
  Status AddLowerBound(const KuduPartialRow& key);

  /// Add lower bound for the scan.
  ///
  /// @deprecated Use AddLowerBound() instead.
  ///
  /// @param [in] key
  ///   The primary key to use as an opaque slice of data.
  /// @return Operation result status.
  Status AddLowerBoundRaw(const Slice& key)
      ATTRIBUTE_DEPRECATED("use AddLowerBound() instead");

  /// Add an upper bound (exclusive) primary key for the scan.
  ///
  /// If any bound is already added, this bound is intersected with that one.
  ///
  /// @param [in] key
  ///   The key to setup the upper bound. The scanner makes a copy of the
  ///   parameter, the caller may free it afterward.
  /// @return Operation result status.
  Status AddExclusiveUpperBound(const KuduPartialRow& key);

  /// Add an upper bound (exclusive) primary key for the scan.
  ///
  /// @deprecated Use AddExclusiveUpperBound() instead.
  ///
  /// @param [in] key
  ///   The encoded primary key is an opaque slice of data.
  /// @return Operation result status.
  Status AddExclusiveUpperBoundRaw(const Slice& key)
      ATTRIBUTE_DEPRECATED("use AddExclusiveUpperBound() instead");

  /// Add a lower bound (inclusive) partition key for the scan.
  ///
  /// @note This method is unstable, and for internal use only.
  ///
  /// @param [in] partition_key
  ///   The scanner makes a copy of the parameter: the caller may invalidate
  ///   it afterward.
  /// @return Operation result status.
  Status AddLowerBoundPartitionKeyRaw(const Slice& partition_key);

  /// Add an upper bound (exclusive) partition key for the scan.
  ///
  /// @note This method is unstable, and for internal use only.
  ///
  /// @param [in] partition_key
  ///   The scanner makes a copy of the parameter, the caller may invalidate
  ///   it afterward.
  /// @return Operation result status.
  Status AddExclusiveUpperBoundPartitionKeyRaw(const Slice& partition_key);

  /// Set the block caching policy.
  ///
  /// @param [in] cache_blocks
  ///   If @c true, scanned data blocks will be cached in memory and
  ///   made available for future scans. Default is @c true.
  /// @return Operation result status.
  Status SetCacheBlocks(bool cache_blocks);

  /// @return Result status of the operation (begin scanning).
  Status Open();

  /// Keep the current remote scanner alive.
  ///
  /// Keep the current remote scanner alive on the Tablet server
  /// for an additional time-to-live (set by a configuration flag on
  /// the tablet server). This is useful if the interval in between
  /// NextBatch() calls is big enough that the remote scanner might be garbage
  /// collected (default TTL is set to 60 secs.).
  /// This does not invalidate any previously fetched results.
  ///
  /// @return Operation result status. In particular, this method returns
  ///   a non-OK status if the scanner was already garbage collected or if the
  ///   TabletServer was unreachable, for any reason. Note that a non-OK
  ///   status returned by this method should not be taken as indication
  ///   that the scan has failed. Subsequent calls to NextBatch() might
  ///   still be successful, particularly if SetFaultTolerant() has been called.
  Status KeepAlive();

  /// Close the scanner.
  ///
  /// Closing the scanner releases resources on the server. This call does not
  /// block, and will not ever fail, even if the server cannot be contacted.
  ///
  /// @note The scanner is reset to its initial state by this function.
  ///   You'll have to re-add any projection, predicates, etc if you want
  ///   to reuse this object.
  void Close();

  /// Check if there may be rows to be fetched from this scanner.
  ///
  /// @return @c true if there may be rows to be fetched from this scanner.
  ///   The method returns @c true provided there's at least one more tablet
  ///   left to scan, even if that tablet has no data
  ///   (we'll only know once we scan it).
  ///   It will also be @c true after the initially opening the scanner before
  ///   NextBatch is called for the first time.
  bool HasMoreRows() const;

  /// Get next batch of rows.
  ///
  /// Clears 'rows' and populates it with the next batch of rows
  /// from the tablet server. A call to NextBatch() invalidates all previously
  /// fetched results which might now be pointing to garbage memory.
  ///
  /// @deprecated Use NextBatch(KuduScanBatch*) instead.
  ///
  /// @param [out] rows
  ///   Placeholder for the result.
  /// @return Operation result status.
  Status NextBatch(std::vector<KuduRowResult>* rows)
      ATTRIBUTE_DEPRECATED("use NextBatch(KuduScanBatch*) instead");

  /// Fetch the next batch of results for this scanner.
  ///
  /// A single KuduScanBatch object may be reused. Each subsequent call
  /// replaces the data from the previous call, and invalidates any
  /// KuduScanBatch::RowPtr objects previously obtained from the batch.
  /// @param [out] batch
  ///   Placeholder for the result.
  /// @return Operation result status.
  Status NextBatch(KuduScanBatch* batch);

  /// Get the KuduTabletServer that is currently handling the scan.
  ///
  /// More concretely, this is the server that handled the most recent
  /// Open() or NextBatch() RPC made by the server.
  ///
  /// @param [out] server
  ///   Placeholder for the result.
  /// @return Operation result status.
  Status GetCurrentServer(KuduTabletServer** server);

  /// @return Cumulative resource metrics since the scan was started.
  const ResourceMetrics& GetResourceMetrics() const;

  /// Set the hint for the size of the next batch in bytes.
  ///
  /// @param [in] batch_size
  ///   The hint of batch size to set. If setting to 0 before calling Open(),
  ///   it means that the first call to the tablet server won't return data.
  /// @return Operation result status.
  Status SetBatchSizeBytes(uint32_t batch_size);

  /// Set the replica selection policy while scanning.
  ///
  /// @param [in] selection
  ///   The policy to set.
  /// @return Operation result status.
  ///
  /// @todo Kill this method in favor of a consistency-level-based API.
  Status SetSelection(KuduClient::ReplicaSelection selection)
    WARN_UNUSED_RESULT;

  /// Set the ReadMode. Default is @c READ_LATEST.
  ///
  /// @param [in] read_mode
  ///   Read mode to set.
  /// @return Operation result status.
  Status SetReadMode(ReadMode read_mode) WARN_UNUSED_RESULT;

  /// @deprecated Use SetFaultTolerant() instead.
  ///
  /// @param [in] order_mode
  ///   Result record ordering mode to set.
  /// @return Operation result status.
  Status SetOrderMode(OrderMode order_mode) WARN_UNUSED_RESULT
      ATTRIBUTE_DEPRECATED("use SetFaultTolerant() instead");

  /// Make scans resumable at another tablet server if current server fails.
  ///
  /// Scans are by default non fault-tolerant, and scans will fail
  /// if scanning an individual tablet fails (for example, if a tablet server
  /// crashes in the middle of a tablet scan). If this method is called,
  /// scans will be resumed at another tablet server in the case of failure.
  ///
  /// Fault-tolerant scans typically have lower throughput than non
  /// fault-tolerant scans. Fault tolerant scans use @c READ_AT_SNAPSHOT mode:
  /// if no snapshot timestamp is provided, the server will pick one.
  ///
  /// @return Operation result status.
  Status SetFaultTolerant() WARN_UNUSED_RESULT;

  /// Set snapshot timestamp for scans in @c READ_AT_SNAPSHOT mode.
  ///
  /// @param [in] snapshot_timestamp_micros
  ///   Timestamp to set in in microseconds since the Epoch.
  /// @return Operation result status.
  Status SetSnapshotMicros(uint64_t snapshot_timestamp_micros) WARN_UNUSED_RESULT;

  /// Set snapshot timestamp for scans in @c READ_AT_SNAPSHOT mode (raw).
  ///
  /// See KuduClient::GetLatestObservedTimestamp() for details on how to
  /// use this method to achieve Read-Your-Writes behavior.
  ///
  /// @note This method is experimental and will either disappear or
  ///   change in a future release.
  ///
  /// @param [in] snapshot_timestamp
  ///   Timestamp to set in raw encoded form
  ///   (i.e. as returned by a previous call to a server).
  /// @return Operation result status.
  Status SetSnapshotRaw(uint64_t snapshot_timestamp) WARN_UNUSED_RESULT;

  /// Set the maximum time that Open() and NextBatch() are allowed to take.
  ///
  /// @param [in] millis
  ///   Timeout to set (in milliseconds). Must be greater than 0.
  /// @return Operation result status.
  Status SetTimeoutMillis(int millis);

  /// @return Schema of the projection being scanned.
  KuduSchema GetProjectionSchema() const;

  /// @name Advanced/Unstable API
  //
  ///@{
  /// Modifier flags for the row format returned from the server.
  ///
  /// @note Each flag corresponds to a bit that gets set on a bitset that is sent
  ///   to the server. See SetRowFormatFlags() for example usage.
  static const uint64_t NO_FLAGS = 0;
  /// Makes the server pad UNIXTIME_MICROS slots to 16 bytes.
  /// @note This flag actually wastes throughput by making messages larger than they need to
  ///   be. It exists merely for compatibility reasons and requires the user to know the row
  ///   format in order to decode the data. That is, if this flag is enabled, the user _must_
  ///   use KuduScanBatch::direct_data() and KuduScanBatch::indirect_data() to obtain the row
  ///   data for further decoding. Using KuduScanBatch::Row() might yield incorrect/corrupt
  ///   results and might even cause the client to crash.
  static const uint64_t PAD_UNIXTIME_MICROS_TO_16_BYTES = 1 << 0;
  /// Optionally set row format modifier flags.
  ///
  /// If flags is RowFormatFlags::NO_FLAGS, then no modifications will be made to the row
  /// format and the default will be used.
  ///
  /// Some flags require server-side server-side support, thus the caller should be prepared to
  /// handle a NotSupported status in Open() and NextBatch().
  ///
  /// Example usage (without error handling, for brevity):
  /// @code
  ///   KuduScanner scanner(...);
  ///   uint64_t row_format_flags = KuduScanner::NO_FLAGS;
  ///   row_format_flags |= KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES;
  ///   scanner.SetRowFormatFlags(row_format_flags);
  ///   scanner.Open();
  ///   while (scanner.HasMoreRows()) {
  ///     KuduScanBatch batch;
  ///     scanner.NextBatch(&batch);
  ///     Slice direct_data = batch.direct_data();
  ///     Slice indirect_data = batch.indirect_data();
  ///     ... // Row data decoding and handling.
  ///   }
  /// @endcode
  Status SetRowFormatFlags(uint64_t flags);
  ///@}

  /// @return String representation of this scan.
  ///
  /// @internal
  /// @note This method must not be used in log messages because it contains
  ///   sensitive predicate values. Use Scanner::Data::DebugString instead.
  std::string ToString() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduScanToken;
  FRIEND_TEST(ClientTest, TestScanCloseProxy);
  FRIEND_TEST(ClientTest, TestScanFaultTolerance);
  FRIEND_TEST(ClientTest, TestScanNoBlockCaching);
  FRIEND_TEST(ClientTest, TestScanTimeout);
  FRIEND_TEST(ClientTest, TestReadAtSnapshotNoTimestampSet);
  FRIEND_TEST(ConsistencyITest, TestSnapshotScanTimestampReuse);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduScanner);
};

/// @brief A scan descriptor limited to a single physical contiguous location.
///
/// A KuduScanToken describes a partial scan of a Kudu table limited to a single
/// contiguous physical location. Using the KuduScanTokenBuilder, clients can
/// describe the desired scan, including predicates, bounds, timestamps, and
/// caching, and receive back a collection of scan tokens.
///
/// Each scan token may be separately turned into a scanner using
/// KuduScanToken::IntoKuduScanner, with each scanner responsible for a disjoint
/// section of the table.
///
/// Scan tokens may be serialized using the KuduScanToken::Serialize method and
/// deserialized back into a scanner using the
/// KuduScanToken::DeserializeIntoScanner method. This allows use cases such as
/// generating scan tokens in the planner component of a query engine, then
/// sending the tokens to execution nodes based on locality, and then
/// instantiating the scanners on those nodes.
///
/// Scan token locality information can be inspected using the
/// KuduScanToken::tablet() function.
class KUDU_EXPORT KuduScanToken {
 public:

  ~KuduScanToken();

  /// Create a new scanner.
  ///
  /// This method creates a new scanner, setting the result scanner's options
  /// according to the scan token.
  ///
  /// @param [out] scanner
  ///   The result scanner. The caller owns the new scanner. The scanner
  ///   must be opened before use. The output parameter will not be set
  ///   if the returned status is an error.
  /// @return Operation result status.
  Status IntoKuduScanner(KuduScanner** scanner) const WARN_UNUSED_RESULT;

  /// @return Tablet that this scan will retrieve rows from.
  const KuduTablet& tablet() const;

  /// Serialize the token into a string.
  ///
  /// Deserialize with KuduScanToken::DeserializeIntoScanner().
  ///
  /// @param [out] buf
  ///   Result string to output the serialized token.
  /// @return Operation result status.
  Status Serialize(std::string* buf) const WARN_UNUSED_RESULT;

  /// Create a new scanner and set the scanner options.
  ///
  /// @param [in] client
  ///   Client to bound to the scanner.
  /// @param [in] serialized_token
  ///   Token containing serialized scanner parameters.
  /// @param [out] scanner
  ///   The result scanner. The caller owns the new scanner. The scanner
  ///   must be opened before use. The scanner will not be set if
  ///   the returned status is an error.
  /// @return Operation result status.
  static Status DeserializeIntoScanner(KuduClient* client,
                                       const std::string& serialized_token,
                                       KuduScanner** scanner) WARN_UNUSED_RESULT;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduScanTokenBuilder;

  KuduScanToken();

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduScanToken);
};

/// @brief Builds scan tokens for a table.
///
/// @note This class is not thread-safe.
class KUDU_EXPORT KuduScanTokenBuilder {
 public:

  /// Construct an instance of the class.
  ///
  /// @param [in] table
  ///   The table the tokens should scan. The given object must remain valid
  ///   for the lifetime of the builder, and the tokens which it builds.
  explicit KuduScanTokenBuilder(KuduTable* table);
  ~KuduScanTokenBuilder();

  /// Set the column projection by passing the column names to read.
  ///
  /// Set the column projection used for this scanner by passing the column
  /// names to read. A call of this method overrides any previous call to
  /// SetProjectedColumnNames() or SetProjectedColumnIndexes().
  ///
  /// @param [in] col_names
  ///   Column names for the projection.
  /// @return Operation result status.
  Status SetProjectedColumnNames(const std::vector<std::string>& col_names)
    WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetProjectedColumnIndexes()
  Status SetProjectedColumnIndexes(const std::vector<int>& col_indexes)
    WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::AddConjunctPredicate()
  Status AddConjunctPredicate(KuduPredicate* pred) WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::AddLowerBound()
  Status AddLowerBound(const KuduPartialRow& key) WARN_UNUSED_RESULT;

  /// Add an upper bound (exclusive) primary key.
  ///
  /// If any bound is already added, this bound is intersected with that one.
  ///
  /// @param [in] key
  ///   Upper bound primary key to add. The KuduScanTokenBuilder instance
  ///   does not take ownership of the parameter.
  /// @return Operation result status.
  Status AddUpperBound(const KuduPartialRow& key) WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetCacheBlocks
  Status SetCacheBlocks(bool cache_blocks) WARN_UNUSED_RESULT;

  /// Set the hint for the size of the next batch in bytes.
  ///
  /// @param [in] batch_size
  ///   Batch size to set (in bytes). If set to 0, the first call
  ///   to the tablet server won't return data.
  /// @return Operation result status.
  Status SetBatchSizeBytes(uint32_t batch_size) WARN_UNUSED_RESULT;

  /// Set the replica selection policy while scanning.
  ///
  /// @param [in] selection
  ///   Selection policy to set.
  /// @return Operation result status.
  ///
  /// @todo Kill this in favor of a consistency-level-based API.
  Status SetSelection(KuduClient::ReplicaSelection selection)
    WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetReadMode()
  Status SetReadMode(KuduScanner::ReadMode read_mode) WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetFaultTolerant
  Status SetFaultTolerant() WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetSnapshotMicros
  Status SetSnapshotMicros(uint64_t snapshot_timestamp_micros)
    WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetSnapshotRaw
  Status SetSnapshotRaw(uint64_t snapshot_timestamp) WARN_UNUSED_RESULT;

  /// @copydoc KuduScanner::SetTimeoutMillis
  Status SetTimeoutMillis(int millis) WARN_UNUSED_RESULT;

  /// Build the set of scan tokens.
  ///
  /// The builder may be reused after this call.
  ///
  /// @param [out] tokens
  ///   Result set of tokens. The caller takes ownership of the container
  ///   elements.
  /// @return Operation result status.
  Status Build(std::vector<KuduScanToken*>* tokens) WARN_UNUSED_RESULT;

 private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduScanTokenBuilder);
};

/// @brief Builder for Partitioner instances.
class KUDU_EXPORT KuduPartitionerBuilder {
 public:
  /// Construct an instance of the class.
  ///
  /// @param [in] table
  ///   The table whose rows should be partitioned.
  explicit KuduPartitionerBuilder(sp::shared_ptr<KuduTable> table);
  ~KuduPartitionerBuilder();

  /// Set the timeout used for building the Partitioner object.
  KuduPartitionerBuilder* SetBuildTimeout(MonoDelta timeout);

  /// Create a KuduPartitioner object for the specified table.
  ///
  /// This fetches all of the partitioning information up front if it
  /// is not already cached by the associated KuduClient object. Thus,
  /// it may time out or have an error if the Kudu master is not accessible.
  ///
  /// @param [out] partitioner
  ///   The resulting KuduPartitioner instance; caller gets ownership.
  ///
  /// @note If the KuduClient object associated with the table already has
  /// some partition information cached (e.g. due to the construction of
  /// other Partitioners, or due to normal read/write activity), the
  /// resulting Partitioner will make use of that cached information.
  /// This means that the resulting partitioner is not guaranteed to have
  /// up-to-date partition information in the case that there has been
  /// a recent change to the partitioning of the target table.
  Status Build(KuduPartitioner** partitioner);
 private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduPartitionerBuilder);
};

/// A KuduPartitioner allows clients to determine the target partition of a
/// row without actually performing a write. The set of partitions is eagerly
/// fetched when the KuduPartitioner is constructed so that the actual partitioning
/// step can be performed synchronously without any network trips.
///
/// @note Because this operates on a metadata snapshot retrieved at construction
/// time, it will not reflect any metadata changes to the table that have occurred
/// since its creation.
///
/// @warning This class is not thread-safe.
class KUDU_EXPORT KuduPartitioner {
 public:
  ~KuduPartitioner();

  /// Return the number of partitions known by this partitioner.
  /// The partition indices returned by @c PartitionRow are guaranteed
  /// to be less than this value.
  int NumPartitions() const;

  /// Determine the partition index that the given row falls into.
  ///
  /// @param [in] row
  ///   The row to be partitioned.
  /// @param [out] partition
  ///   The resulting partition index, or -1 if the row falls into a
  ///   non-covered range. The result will be less than @c NumPartitioons().
  ///
  /// @return Status OK if successful. May return a bad Status if the
  /// provided row does not have all columns of the partition key
  /// set.
  Status PartitionRow(const KuduPartialRow& row, int* partition);
 private:
  class KUDU_NO_EXPORT Data;
  friend class KuduPartitionerBuilder;

  explicit KuduPartitioner(Data* data);
  Data* data_; // Owned.
};


} // namespace client
} // namespace kudu
#endif
