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
#include <map>
// Not safe to include <memory>; this header must remain compatible with C++98.
//
// IWYU pragma: no_include <memory>
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

class AlterTableTest;
class AuthzTokenTest;
class ClientStressTest_TestUniqueClientIds_Test;
class DisableWriteWhenExceedingQuotaTest;
class KuduPartialRow;
class MetaCacheLookupStressTest_PerfSynthetic_Test;
class MonoDelta;
class Partition;
class PartitionSchema;
class SecurityUnknownTskTest;
class TxnId;

namespace client {
class KuduClient;
class KuduTable;
} // namespace client

namespace tablet {
class FuzzTest;
} // namespace tablet

namespace transactions {
class CoordinatorRpc;
class ParticipantRpc;
class TxnSystemClient;
} // namespace transactions

namespace tools {
class LeaderMasterProxy;
class RemoteKsckCluster;
class TableAlter;
class TableLister;
} // namespace tools

namespace client {

class KuduColumnarScanBatch;
class KuduDelete;
class KuduDeleteIgnore;
class KuduInsert;
class KuduInsertIgnore;
class KuduLoggingCallback;
class KuduPartitioner;
class KuduScanBatch;
class KuduSession;
class KuduStatusCallback;
class KuduTableAlterer;
class KuduTableCreator;
class KuduTableStatistics;
class KuduTablet;
class KuduTabletServer;
class KuduUpdate;
class KuduUpdateIgnore;
class KuduUpsert;
class KuduUpsertIgnore;
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
class ReplicaController;
class RetrieveAuthzTokenRpc;
class ScanBatchDataInterface;
class TabletInfoProvider;
class WriteRpc;
template <class ReqClass, class RespClass>
class AsyncLeaderMasterRpc; // IWYU pragma: keep
template <class ReqClass, class RespClass>
class AsyncRandomTxnManagerRpc; // IWYU pragma: keep
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
Status KUDU_EXPORT SetInternalSignalNumber(int signum) WARN_UNUSED_RESULT;

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
Status KUDU_EXPORT DisableSaslInitialization() WARN_UNUSED_RESULT;


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
Status KUDU_EXPORT DisableOpenSSLInitialization() WARN_UNUSED_RESULT;

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

  /// Policy for on-the-wire encryption
  enum EncryptionPolicy {
    OPTIONAL,        ///< Optional, it uses encrypted connection if the server supports
                     ///< it, but it can connect to insecure servers too.

    REQUIRED_REMOTE, ///< Only connects to remote servers that support encryption, fails
                     ///< otherwise. It can connect to insecure servers only locally.

    REQUIRED         ///< Only connects to any server, including on the loopback interface,
                     ///< that support encryption, fails otherwise.
  };

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

  /// Set the timeout for negotiating a connection to a remote server.
  ///
  /// If not provided, the underlying messenger is created with reasonable
  /// default. The result value could be retrieved using
  /// @c KuduClient.connection_negotiation_timeout() after an instance of
  /// @c KuduClient is created. Sometimes it makes sense to customize the
  /// timeout for connection negotiation, e.g. when running on a cluster with
  /// heavily loaded tablet servers. For details on the connection negotiation,
  /// see ../../../docs/design-docs/rpc.md#negotiation.
  ///
  /// @param [in] timeout
  ///   Timeout value to set.
  /// @return Reference to the updated object.
  KuduClientBuilder& connection_negotiation_timeout(const MonoDelta& timeout);


  /// Import serialized authentication credentials from another client.
  ///
  /// @param [in] authn_creds
  ///   The serialized authentication credentials, provided by a call to
  ///   @c KuduClient.ExportAuthenticationCredentials in the C++ client or
  ///   @c KuduClient#exportAuthenticationCredentials in the Java client.
  /// @return Reference to the updated object.
  KuduClientBuilder& import_authentication_credentials(std::string authn_creds);

  /// @brief Set the number of reactors for the RPC messenger.
  ///
  /// The reactor threads are used for sending and receiving. If not provided,
  /// the underlying messenger is created with the default number of reactor
  /// threads.
  ///
  /// @param [in] num_reactors
  ///   Number of reactors to set.
  /// @return Reference to the updated object.
  KuduClientBuilder& num_reactors(int num_reactors);

  /// Set the SASL protocol name for the connection to a remote server.
  ///
  /// If the servers use a non-default Kerberos service principal name (other
  /// than "kudu" or "kudu/<hostname>", this needs to be set for the client to
  /// be able to connect to the servers. If unset, the client will assume the
  /// server is using the default service principal.
  ///
  /// @param [in] sasl_protocol_name
  ///   SASL protocol name.
  /// @return Reference to the updated object.
  KuduClientBuilder& sasl_protocol_name(const std::string& sasl_protocol_name);

  /// Require authentication for the connection to a remote server.
  ///
  /// If it's set to true, the client will require mutual authentication between
  /// the server and the client. If the server doesn't support authentication,
  /// or it's disabled, the client will fail to connect.
  ///
  /// @param [in] require_authentication
  ///   Whether to require authentication.
  /// @return Reference to the updated object.
  KuduClientBuilder& require_authentication(bool require_authentication);

  /// Require encryption for the connection to a remote server.
  ///
  /// If it's set to REQUIRED_REMOTE or REQUIRED, the client will
  /// require encrypting the traffic between the server and the client.
  /// If the server doesn't support encryption, or if it's disabled, the
  /// client will fail to connect.
  ///
  /// Loopback connections are encrypted only if 'encryption_policy' is
  /// set to REQUIRED, or if it's required by the server.
  ///
  /// The default value is OPTIONAL, which allows connecting to servers without
  /// encryption as well, but it will still attempt to use it if the server
  /// supports it.
  ///
  /// @param [in] encryption_policy
  ///   Which encryption policy to use.
  /// @return Reference to the updated object.
  KuduClientBuilder& encryption_policy(EncryptionPolicy encryption_policy);

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

  friend class internal::ReplicaController;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduClientBuilder);
};

/// A class representing a multi-row transaction in Kudu. Once created using
/// @c KuduClient::BeginTransaction() or @c KuduTransaction::Deserialize method,
/// @c KuduTransaction instance can be used to commit or rollback the underlying
/// multi-row transaction and create a transactional session.
///
/// @note The @c KuduTransaction should be kept in scope to maintain automatic
///   keep-alive heartbeating for the corresponding transaction. Once this
///   object goes out of scope, the heartbeating stops and the transaction may
///   automatically be aborted soon if no other clients do the heartbeating.
///
/// @note There isn't any automation to rollback or commit the underlying
///   transaction upon destruction of an instance of this class.
///
/// @warning The set of methods in this class, their behavior, and signatures
///          are experimental and may change or disappear in future. The class
///          itself is experimental and may change its lineage, API status,
///          or disappear in future.
class KUDU_EXPORT KuduTransaction :
    public sp::enable_shared_from_this<KuduTransaction> {
 public:
  ~KuduTransaction();

  /// Create a new @c KuduSession with "transactional" semantics.
  ///
  /// Every write operation performed in the context of the newly created
  /// "transactional" session becomes a part of the corresponding multi-row
  /// transaction represented by an instance of this class. Multiple sessions
  /// can be created in the context of the same multi-row distributed
  /// transaction by the same or different Kudu clients residing on a single
  /// or multiple nodes.
  ///
  /// @param [out] session
  ///   The result session object.
  /// @return Operation result status.
  Status CreateSession(sp::shared_ptr<KuduSession>* session) WARN_UNUSED_RESULT;

  /// Commit the transaction.
  ///
  /// This method automatically flushes all transactional sessions created off
  /// this transaction handle via @c KuduTransaction::CreateSession(), initiates
  /// committing the transaction, and then waits for the commit phase to
  /// finalize. The flushing of all the derivative transactional sessions helps
  /// avoiding unintentional data loss when those sessions are not flushed
  /// explicitly before committing. No new operations should be pushed into the
  /// derivative transactional sessions created off this handle
  /// once the method has been called.
  ///
  /// @return Returns @c Status::OK() if all the stages of the transaction's
  ///   commit sequence were successful, i.e. the status of various pre-commit
  ///   work, the status of starting the commit phase, the status of the commit
  ///   phase itself once it's completed. Returns non-OK status of the very
  ///   first failed stage of the transaction's commit sequence.
  Status Commit() WARN_UNUSED_RESULT;

  /// Start committing this transaction, but don't wait for the commit phase
  /// to finalize.
  ///
  /// This method initiates the commit phase for this transaction, not waiting
  /// for the commit phase to finalize. It requires all the transactional
  /// sessions created off this handle via @c KuduTransaction::CreateSession()
  /// to be flushed already. No new operations should be pushed into the
  /// derivative transactional sessions created off this handle once the method
  /// has been called. To check for the transaction's commit status, use the
  /// @c KuduTransaction::IsCommitComplete() method.
  ///
  /// @return Status of starting the commit phase for this transaction if all
  ///   the transactional sessions created off this handle are flushed,
  ///   otherwise returns @c Status::IllegalState().
  Status StartCommit() WARN_UNUSED_RESULT;

  /// Whether the commit has completed i.e. no longer in progress of finalizing.
  ///
  /// This method checks for the transaction's commit status, setting the
  /// @c is_complete out parameter to @c true and the @c completion_status
  /// parameter to the finalization status of the commit process,
  /// assuming the method returning @c Status::OK(). The happy case is when
  /// the method returns @c Status::OK(), @c is_complete is set to @c true and
  /// @c completion_status is set to @c Status::OK() -- that means the
  /// transaction has successfully finalized its commit phase.
  ///
  /// @param [out] is_complete
  ///   Whether the process of finalizing the commit of the transaction has
  ///   ended, including both success and failure outcomes. In other words,
  ///   the value of this out parameter indicates whether the finalization
  ///   of the transaction's commit phase is no longer in progress: it already
  ///   succeeded or failed by the time of processing the request.
  ///   This parameter is assigned a meaningful value iff the method returns
  ///   @c Status::OK().
  /// @param [out] completion_status
  ///   The status of finalization of the transaction's commit phase:
  ///     @li Status::OK() if the commit phase successfully finalized
  ///     @li non-OK status if the commit phase failed to finalize
  ///   This parameter is assigned a meaningful value iff the method returns
  ///   @c Status::OK().
  /// @return The result status of querying the transaction's commit status.
  ///   Both @c is_complete and @c completion_status are set iff the method
  ///   returns @c Status::OK().
  Status IsCommitComplete(bool* is_complete,
                          Status* completion_status) WARN_UNUSED_RESULT;

  /// Rollback/abort the transaction.
  ///
  /// @return Operation result status.
  Status Rollback() WARN_UNUSED_RESULT;

  /// This class controls serialization-related parameters for a Kudu
  /// transaction handle (i.e. @c KuduTransaction).
  ///
  /// One of the parameters is whether to enable sending keepalive messages for
  /// the resulting @c KuduTransaction handle upon deserialization.
  /// In future, the list of configurable parameters might be extended (e.g.,
  /// by adding commit and abort permissions, i.e. whether a handle obtained by
  /// deserializing a handle from the string representation can be used
  /// to commit and/or abort the transaction).
  class KUDU_EXPORT SerializationOptions {
   public:
    SerializationOptions();
    ~SerializationOptions();

    /// This method returns the current setting keepalive behavior, i.e. whether
    /// to send keepalive messages for Kudu transaction handles.
    ///
    /// No keepalive heartbeat messages are sent from a transaction handle if
    /// its token was created with the default "keepalive disabled" setting.
    /// The idea here is that the most common use case for using transaction
    /// tokens is of the "start topology" (see below), so it's enough to have
    /// just one top-level handle sending keepalive messages. Overall, having
    /// more than one actor sending keepalive messages for a transaction is
    /// acceptable but it puts needless load on a cluster.
    ///
    /// The most common use case for a transaction's handle
    /// serialization/deserialization is of the "star topology": a transaction
    /// is started by a top-level application which sends the transaction token
    /// produced by serializing the original transaction handle to other worker
    /// applications running concurrently, where the latter write their data
    /// in the context of the same transaction and report back to the top-level
    /// application, which in its turn initiates committing the transaction
    /// as needed. The important point is that the top-level application keeps
    /// the transaction handle around all the time from the start of the
    /// transaction to the very point when transaction is committed. Under the
    /// hood, the original transaction handle sends keepalive messages as
    /// required until commit phase is initiated, so the deserialized
    /// transaction handles which are used by the worker applications don't
    /// need to send keepalive messages.
    ///
    /// The other (less common) use case is of the "ring topology": a chain of
    /// applications work sequentially as a part of the same transaction, where
    /// the very first application starts the transaction, writes its data, and
    /// hands over the responsibility of managing the lifecycle of the
    /// transaction to other application down the chain. After doing so it may
    /// exit, so now only the next application has the active transaction
    /// handle, and so on it goes until the transaction is committed by the
    /// application in the end of the chain. In this scenario, every
    /// deserialized handle have to send keepalive messages to avoid automatic
    /// rollback of the transaction, and every application in the chain should
    /// set @c SerializationOptions::enable_keepalive to true when serializing
    /// its transaction handle into a transaction token to pass to the
    /// application next in the chain.
    ///
    /// @return whether to send keepalive messages for Kudu transaction handles
    bool keepalive() const;

    /// Enable/disable keepalive for a handle which is the result of
    /// deserializing a previously serialized @c KuduTransaction handle.
    ///
    /// Sending keepalive messages for a transaction handle deserialized
    /// from a string is disabled by default.
    ///
    /// @param [in] enable
    ///   Whether to enable sending keepalive messages for @c KuduTransaction
    ///   handle once it's deserialized from the string representation of a
    ///   Kudu transaction handle.
    /// @return Reference to the updated object.
    SerializationOptions& enable_keepalive(bool enable);

   private:
    friend class KuduTransaction;
    class KUDU_NO_EXPORT Data;

    Data* data_; // Owned.

    DISALLOW_COPY_AND_ASSIGN(SerializationOptions);
  };

  /// Export the information on this transaction in a serialized form.
  ///
  /// The serialized information on a Kudu transaction can be passed among
  /// different Kudu clients running at multiple nodes, so those separate
  /// Kudu clients can perform operations to be a part of the same distributed
  /// transaction. The resulting string is referred as "transaction token" and
  /// can be deserialized into a transaction handle (i.e. an object of the @c
  /// KuduTransaction class) via the @c KuduTransaction::Deserialize() method.
  ///
  /// This method doesn't perform any RPC under the hood. The behavior of this
  /// method is controlled by @c SerializationOptions set for this transaction
  /// handle.
  ///
  /// @note The representation of the data in the serialized form
  ///   (i.e. the format of a Kudu transaction token) is an implementation
  ///   detail, not a part of the public API.
  ///
  /// @param [out] serialized_txn
  ///   Result string to output the serialized transaction information.
  /// @param [in] options
  ///   Options to use when serializing the handle (optional). If omitted,
  ///   the default serialization parameters are used -- the same as it would
  ///   be for a default-constructed instance of SerializationOptions used
  ///   for this parameter.
  /// @return Operation result status.
  Status Serialize(
      std::string* serialized_txn,
      const SerializationOptions& options = SerializationOptions()) const WARN_UNUSED_RESULT;

  /// Re-create KuduTransaction object given its serialized representation.
  ///
  /// This method doesn't perform any RPC under the hood. The newly created
  /// object automatically does or does not send keep-alive messages depending
  /// on the @c KuduTransaction::SerializationOptions::enable_keepalive()
  /// setting when the original @c KuduTransaction object was serialized using
  /// @c KuduTransaction::Serialize().
  ///
  /// @param [in] client
  ///   Client instance to bound the result object to.
  /// @param [in] serialized_txn
  ///   String containing serialized representation of KuduTransaction object.
  /// @param [out] txn
  ///   The result KuduTransaction object, wrapped into a smart pointer.
  /// @return Operation result status.
  static Status Deserialize(const sp::shared_ptr<KuduClient>& client,
                            const std::string& serialized_txn,
                            sp::shared_ptr<KuduTransaction>* txn) WARN_UNUSED_RESULT;
 private:
  DISALLOW_COPY_AND_ASSIGN(KuduTransaction);

  friend class KuduClient;
  friend class KuduSession;
  FRIEND_TEST(ClientTest, TxnIdOfTransactionalSession);
  FRIEND_TEST(ClientTest, TxnToken);

  class KUDU_NO_EXPORT Data;

  explicit KuduTransaction(const sp::shared_ptr<KuduClient>& client);
  Data* data_; // Owned.
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

  /// Delete/drop a table without reserving.
  /// The deleted table may turn to soft-deleted status with the flag
  /// --default_deleted_table_reserve_seconds set to nonzero on the master side.
  ///
  /// The delete operation or drop operation means that the service will directly
  /// delete the table after receiving the instruction. Which means that once we
  /// delete the table by mistake, we have no way to recall the deleted data.
  /// We have added a new API @SoftDeleteTable to allow the deleted data to be
  /// reserved for a period of time, which means that the wrongly deleted data may
  /// be recalled. In order to be compatible with the previous versions, this interface
  /// will continue to directly delete tables without reserving the table.
  ///
  /// Refer to SoftDeleteTable for detailed usage examples.
  ///
  /// @param [in] table_name
  ///   Name of the table to drop.
  /// @return Operation status.
  Status DeleteTable(const std::string& table_name);

  /// Soft delete/drop a table.
  ///
  /// Usage Example1:
  /// Equal to DeleteTable(table_name) and the table will not be reserved.
  /// @code
  /// client->SoftDeleteTable(table_name);
  /// @endcode
  ///
  /// Usage Example2:
  /// The table will be reserved for 600s after delete operation.
  /// We can recall the table in time after the delete.
  /// @code
  /// client->SoftDeleteTable(table_name, false, 600);
  /// client->RecallTable(table_id);
  /// @endcode

  /// @param [in] table_name
  ///   Name of the table to drop.
  /// @param [in] reserve_seconds
  ///   Reserve seconds after being deleted.
  /// @return Operation status.
  Status SoftDeleteTable(const std::string& table_name,
                         uint32_t reserve_seconds = 0);

  /// @cond PRIVATE_API

  /// Delete/drop a table in internal catalogs and possibly external catalogs.
  ///
  /// Private API.
  ///
  /// @param [in] table_name
  ///   Name of the table to drop.
  /// @param [in] modify_external_catalogs
  ///   Whether to apply the deletion to external catalogs, such as the Hive Metastore,
  ///   which the Kudu master has been configured to integrate with.
  /// @param [in] reserve_seconds
  ///   Reserve seconds after being deleted.
  ///   Default value '-1' means not specified and the server will use master side config.
  ///   '0' means purge immediately and other values means reserve some seconds.
  /// @return Operation status.
  Status DeleteTableInCatalogs(const std::string& table_name,
                               bool modify_external_catalogs,
                               int32_t reserve_seconds = -1) KUDU_NO_EXPORT;

  /// Recall a deleted but still reserved table.
  ///
  /// @param [in] table_id
  ///   ID of the table to recall.
  /// @param [in] new_table_name
  ///   New table name for the recalled table. The recalled table will use the original
  ///   table name if the parameter is empty string (i.e. "").
  /// @return Operation status.
  Status RecallTable(const std::string& table_id, const std::string& new_table_name = "");

  /// @endcond

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

  /// List non-soft-deleted tables whose names pass a substring
  /// match on @c filter.
  ///
  /// @param [out] tables
  ///   The placeholder for the result. Appended only on success.
  /// @param [in] filter
  ///   Substring filter to use; empty sub-string filter matches all tables.
  /// @return Status object for the operation.
  Status ListTables(std::vector<std::string>* tables,
                    const std::string& filter = "");

  /// List soft-deleted tables only those names pass a substring
  /// with names matching the specified @c filter.
  ///
  /// @param [out] tables
  ///   The placeholder for the result. Appended only on success.
  /// @param [in] filter
  ///   Substring filter to use; empty sub-string filter matches all tables.
  /// @return Status object for the operation.
  Status ListSoftDeletedTables(std::vector<std::string>* tables,
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

  /// Start a multi-row transaction.
  ///
  /// This method results in an RPC sent to a Kudu cluster to begin a multi-row
  /// distributed transaction. In case of success, the resulting transaction
  /// handle is output into the 'txn' parameter. That handle can be used
  /// to create a new @c KuduSession using the
  /// @c NewSession(const sp::shared_ptr<KuduSession>&) method. To commit or
  /// rollback all single-row write operations performed in the context of
  /// the newly created transaction, use @c KuduTransaction::Commit() and
  /// @c KuduTransaction::Rollback() methods correspondingly.
  ///
  /// @note The newly created object starts sending keep-alive messages for
  ///   the newly opened transaction as required by the keep-alive interval
  ///   assigned to the transaction by the system. To keep the heartbeating,
  ///   the newly created @c KuduTransaction should be kept in scope.
  ///
  /// @warning This method is experimental and may change or disappear in future.
  ///
  /// @param txn [out]
  ///   The resulting @c KuduTransaction object wrapped into a smart pointer.
  ///   This 'out' parameter is populated iff the operation to begin
  ///   a transaction was successful.
  /// @return The status of underlying "begin transaction" operation.
  Status NewTransaction(sp::shared_ptr<KuduTransaction>* txn) WARN_UNUSED_RESULT;

  /// @cond PRIVATE_API

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
  Status GetTablet(const std::string& tablet_id,
                   KuduTablet** tablet) KUDU_NO_EXPORT;

  /// Get the table statistics by table name.
  ///
  /// @param [in] table_name
  ///   Name of the table.
  /// @param [out] statistics
  ///   Table statistics. The caller takes ownership of the statistics.
  /// @return Operation status.
  Status GetTableStatistics(const std::string& table_name,
                            KuduTableStatistics** statistics);

  /// Get the master RPC addresses as configured on the last leader master this
  /// client connected to, as a CSV. If the client has not connected to a leader
  /// master, an empty string is returned.
  ///
  /// Private API.
  ///
  /// @return The master addresses as a CSV.
  std::string GetMasterAddresses() const KUDU_NO_EXPORT;

  /// @endcond

  /// Policy with which to choose amongst multiple replicas.
  enum ReplicaSelection {
    LEADER_ONLY,      ///< Select the LEADER replica.

    CLOSEST_REPLICA,  ///< Select the closest replica to the client.
                      ///< Local replicas are considered the closest,
                      ///< followed by replicas in the same location as the
                      ///< client, followed by all other replicas. If there are
                      ///< multiple closest replicas, one is chosen randomly.

    FIRST_REPLICA     ///< Select the first replica in the list.
  };

  /// @return @c true iff client is configured to talk to multiple
  ///   Kudu master servers.
  bool IsMultiMaster() const;

  /// @return Default timeout for admin operations.
  const MonoDelta& default_admin_operation_timeout() const;

  /// @return Default timeout for RPCs.
  const MonoDelta& default_rpc_timeout() const;

  /// @return Timeout for connection negotiation to a remote server.
  MonoDelta connection_negotiation_timeout() const;

  /// Value for the latest observed timestamp when none has been observed
  /// or set.
  static const uint64_t kNoTimestamp;

  /// Get the highest HybridTime timestamp observed by the client.
  ///
  /// This is useful when retrieving timestamp from one client and
  /// forwarding it to another to enforce external consistency when
  /// using KuduSession::CLIENT_PROPAGATED external consistency mode.
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

  /// @cond PRIVATE_API

  /// Private API.
  ///
  /// @return the configured Hive Metastore URIs on the most recently connected to
  ///   leader master, or an empty string if the Hive Metastore integration is not
  ///   enabled.
  std::string GetHiveMetastoreUris() const KUDU_NO_EXPORT;

  /// Private API.
  ///
  /// @return the configured Hive Metastore SASL (Kerberos) configuration on the most
  ///   recently connected to leader master, or an arbitrary value if the Hive
  ///   Metastore integration is not enabled.
  bool GetHiveMetastoreSaslEnabled() const KUDU_NO_EXPORT;

  /// Private API.
  ///
  /// @note this is provided on a best-effort basis, as not all Hive Metastore
  ///   versions which Kudu is compatible with include the necessary APIs. See
  ///   HIVE-16452 for more info.
  ///
  /// @return a unique ID which identifies the Hive Metastore instance, if the
  ///   cluster is configured with the Hive Metastore integration, or an
  ///   arbitrary value if the Hive Metastore integration is not enabled.
  std::string GetHiveMetastoreUuid() const KUDU_NO_EXPORT;

  /// Private API.
  ///
  /// @return The location of the client, assigned when it first connects to
  ///   a cluster. An empty string will be returned if no location has been
  ///   assigned yet, or if the leader master did not assign a location to
  ///   the client.
  std::string location() const KUDU_NO_EXPORT;

  /// Private API.
  ///
  /// @return The ID of the cluster that this client is connected to.
  std::string cluster_id() const KUDU_NO_EXPORT;
  /// @endcond

 private:
  class KUDU_NO_EXPORT Data;

  template <class ReqClass, class RespClass>
  friend class internal::AsyncLeaderMasterRpc;
  template <class ReqClass, class RespClass>
  friend class internal::AsyncRandomTxnManagerRpc;

  friend class ClientTest;
  friend class ConnectToClusterBaseTest;
  friend class KuduClientBuilder;
  friend class KuduPartitionerBuilder;
  friend class KuduTransaction;
  friend class KuduScanToken;
  friend class KuduScanTokenBuilder;
  friend class KuduScanner;
  friend class KuduSession;
  friend class KuduTable;
  friend class KuduTableAlterer;
  friend class KuduTableCreator;
  friend class internal::Batcher;
  friend class internal::GetTableSchemaRpc;
  friend class internal::LookupRpc;
  friend class internal::MetaCache;
  friend class internal::RemoteTablet;
  friend class internal::RemoteTabletServer;
  friend class internal::RetrieveAuthzTokenRpc;
  friend class internal::TabletInfoProvider;
  friend class internal::WriteRpc;
  friend class kudu::AuthzTokenTest;
  friend class kudu::DisableWriteWhenExceedingQuotaTest;
  friend class kudu::SecurityUnknownTskTest;
  friend class transactions::CoordinatorRpc;
  friend class transactions::ParticipantRpc;
  friend class transactions::TxnSystemClient;
  friend class tools::LeaderMasterProxy;
  friend class tools::RemoteKsckCluster;
  friend class tools::TableLister;
  friend class ScanTokenTest;

  FRIEND_TEST(kudu::ClientStressTest, TestUniqueClientIds);
  FRIEND_TEST(kudu::MetaCacheLookupStressTest, PerfSynthetic);
  FRIEND_TEST(ClientTest, ClearCacheAndConcurrentWorkload);
  FRIEND_TEST(ClientTest, ConnectionNegotiationTimeout);
  FRIEND_TEST(ClientTest, TestBasicIdBasedLookup);
  FRIEND_TEST(ClientTest, TestCacheAuthzTokens);
  FRIEND_TEST(ClientTest, TestGetSecurityInfoFromMaster);
  FRIEND_TEST(ClientTest, TestGetTabletServerBlacklist);
  FRIEND_TEST(ClientTest, TestGetTabletServerDeterministic);
  FRIEND_TEST(ClientTest, TestMasterDown);
  FRIEND_TEST(ClientTest, TestMasterLookupPermits);
  FRIEND_TEST(ClientTest, TestMetaCacheExpiry);
  FRIEND_TEST(ClientTest, TestMetaCacheExpiryById);
  FRIEND_TEST(ClientTest, TestMetaCacheExpiryWithKeysAndIds);
  FRIEND_TEST(ClientTest, TestMetaCacheLookupNoLeaders);
  FRIEND_TEST(ClientTest, TestMetaCacheWithKeysAndIds);
  FRIEND_TEST(ClientTest, TestNonCoveringRangePartitions);
  FRIEND_TEST(ClientTest, TestRetrieveAuthzTokenInParallel);
  FRIEND_TEST(ClientTest, TestReplicatedTabletWritesWithLeaderElection);
  FRIEND_TEST(ClientTest, TestScanFaultTolerance);
  FRIEND_TEST(ClientTest, TestScanTimeout);
  FRIEND_TEST(ClientTest, TestWriteWithDeadMaster);
  FRIEND_TEST(MasterFailoverTest, TestPauseAfterCreateTableIssued);
  FRIEND_TEST(MultiTServerClientTest, TestSetReplicationFactor);

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

  /// @cond PRIVATE_API

  /// Private API.
  ///
  /// @return The location of the tablet server.
  ///   An empty string will be returned if the location is not assigned.
  const std::string& location() const KUDU_NO_EXPORT;
  /// @endcond

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
  friend class internal::ReplicaController;

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

  const std::string& table_id() const;
  const std::string& table_name() const;

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
                                        int32_t num_buckets,
                                        int32_t seed);

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

  /// Add a range partition with table-wide hash bucket schema.
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

  /// Add a range partition with a custom hash schema.
  ///
  /// This method allows adding a range partition which has hash partitioning
  /// schema different from the table-wide one.
  ///
  /// @li When called with a @c KuduRangePartition for which
  ///   @c KuduRangePartition::add_hash_partitions() hasn't been called,
  ///   a range with no hash sub-partitioning is created.
  /// @li To create a range with the table-wide hash schema, use
  ///   @c KuduTableCreator::add_range_partition() instead.
  ///
  /// @param [in] partition
  ///   Range partition with range-specific hash schema.
  ///   The KuduTableCreator object takes ownership of the partition object.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_custom_range_partition(
      class KuduRangePartition* partition);

  /// Add a range partition split at the provided row.
  ///
  /// @param [in] split_row
  ///   The row to use for partitioning. If the row is missing a value
  ///   for any of the range partition columns, the logical minimum value
  ///   for that column type will be used by default.
  ///   The KuduTableCreator object takes ownership of the parameter.
  /// @return Reference to the modified table creator.
  KuduTableCreator& add_range_partition_split(KuduPartialRow* split_row);

  /// Set the table owner.
  ///
  /// If unspecified, the owner will be the user creating the table.
  ///
  /// @param [in] owner
  ///   The username of the table owner.
  /// @return Reference to the modified table creator.
  KuduTableCreator& set_owner(const std::string& owner);

  /// Set the table comment.
  ///
  /// @param [in] comment
  ///   The comment on the table.
  /// @return Reference to the modified table creator.
  KuduTableCreator& set_comment(const std::string& comment);

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

  /// Set the dimension label for all tablets created at table creation time.
  ///
  /// @note By default, the master will try to place newly created tablet replicas on tablet
  /// servers with a small number of tablet replicas. If the dimension label is provided,
  /// newly created replicas will be evenly distributed in the cluster based on the dimension
  /// label. In other words, the master will try to place newly created tablet replicas on
  /// tablet servers with a small number of tablet replicas belonging to this dimension label.
  ///
  /// @param [in] dimension_label
  ///   The dimension label for the tablet to be created.
  /// @return Reference to the modified table creator.
  KuduTableCreator& dimension_label(const std::string& dimension_label);

  /// Sets the table's extra configuration properties.
  ///
  /// If the value of the kv pair is empty, the property will be ignored.
  ///
  /// @param [in] extra_configs
  ///   The table's extra configuration properties.
  /// @return Reference to the modified table creator.
  KuduTableCreator& extra_configs(const std::map<std::string, std::string>& extra_configs);

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
  friend class transactions::TxnSystemClient;

  explicit KuduTableCreator(KuduClient* client);

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTableCreator);
};

/// A helper class to represent a Kudu range partition with a custom hash
/// bucket schema. The hash sub-partitioning for a range partition might be
/// different from the default table-wide hash bucket schema specified during
/// the creation of a table (see KuduTableCreator::add_hash_partitions()).
/// Correspondingly, this class provides a means to specify a custom hash
/// bucket structure for the data in a range partition.
class KUDU_EXPORT KuduRangePartition {
 public:
  /// Create an object representing the range defined by the given parameters.
  ///
  /// @param [in] lower_bound
  ///   The lower bound for the range.
  ///   The KuduRangePartition object takes ownership of the parameter.
  /// @param [in] upper_bound
  ///   The upper bound for the range.
  ///   The KuduRangePartition object takes ownership of the parameter.
  /// @param [in] lower_bound_type
  ///   The type of the lower_bound: inclusive or exclusive; inclusive if the
  ///   parameter is omitted.
  /// @param [in] upper_bound_type
  ///   The type of the upper_bound: inclusive or exclusive; exclusive if the
  ///   parameter is omitted.
  KuduRangePartition(KuduPartialRow* lower_bound,
                     KuduPartialRow* upper_bound,
                     KuduTableCreator::RangePartitionBound lower_bound_type =
      KuduTableCreator::INCLUSIVE_BOUND,
                     KuduTableCreator::RangePartitionBound upper_bound_type =
      KuduTableCreator::EXCLUSIVE_BOUND);

  ~KuduRangePartition();

  /// Add a level of hash sub-partitioning for this range partition.
  ///
  /// The hash schema for the range partition is defined by the whole set of
  /// its hash sub-partitioning levels. A range partition can have multiple
  /// levels of hash sub-partitioning: this method can be called multiple
  /// times to define a multi-dimensional hash bucketing structure for the
  /// range. Alternatively, a range partition can have zero levels of hash
  /// sub-partitioning: simply don't call this method on a newly created
  /// @c KuduRangePartition object to have no hash sub-partitioning for the
  /// range represented by the object.
  ///
  /// @param [in] columns
  ///   Names of columns to use for partitioning.
  /// @param [in] num_buckets
  ///   Number of buckets for the hashing.
  /// @param [in] seed
  ///   Hash seed for mapping rows to hash buckets.
  /// @return Operation result status.
  Status add_hash_partitions(const std::vector<std::string>& columns,
                             int32_t num_buckets,
                             int32_t seed = 0);
 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduTableCreator;
  friend class KuduTableAlterer;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduRangePartition);
};

/// @brief In-memory statistics of table.
class KUDU_EXPORT KuduTableStatistics {
 public:
  KuduTableStatistics();
  ~KuduTableStatistics();

  /// @return The table's on disk size.
  ///  -1 is returned if the table doesn't support on_disk_size.
  ///
  /// @note This statistic is pre-replication.
  int64_t on_disk_size() const;

  /// @return The table's live row count.
  ///  -1 is returned if the table doesn't support live_row_count.
  ///
  /// @note This statistic is pre-replication.
  int64_t live_row_count() const;

  /// @return The table's on disk size limit.
  ///  -1 is returned if there is no disk size limit on this table.
  ///
  /// @note It is experimental and may change or disappear in future.
  /// This feature currently applies size limit on a single table, but
  /// it should also support database level size limit.
  int64_t on_disk_size_limit() const;

  /// @return The table's live row count limit.
  ///  -1 is returned if there is no row count limit on this table.
  ///
  /// @note It is experimental and may change or disappear in future.
  /// This feature currently applies row count limit on a single table,
  /// but it should also support database level row count limit.
  int64_t live_row_count_limit() const;

  /// Stringify this Statistics.
  ///
  /// @return A string describing this statistics
  std::string ToString() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduTableStatistics);
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

  /// @return Comment string for the table.
  const std::string& comment() const;

  /// @return Replication factor of the table.
  int num_replicas() const;

  /// @return Owner of the table (empty string if unset).
  const std::string& owner() const;

  /// @return New @c INSERT operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduInsert* NewInsert();

  /// @return New @c INSERT_IGNORE operation for this table. It is the
  ///   caller's responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduInsertIgnore* NewInsertIgnore();

  /// @return New @c UPSERT operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpsert* NewUpsert();

  /// @return New @c UPSERT_IGNORE operation for this table. It is the
  ///   caller's responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpsertIgnore* NewUpsertIgnore();

  /// @return New @c UPDATE operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpdate* NewUpdate();

  /// @return New @c UPDATE_IGNORE operation for this table. It is the
  ///   caller's responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduUpdateIgnore* NewUpdateIgnore();

  /// @return New @c DELETE operation for this table. It is the caller's
  ///   responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduDelete* NewDelete();

  /// @return New @c DELETE_IGNORE operation for this table. It is the
  ///   caller's responsibility to free the result, unless it is passed to
  ///   KuduSession::Apply().
  KuduDeleteIgnore* NewDeleteIgnore();

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

  /// Create a new IN Bloom filter predicate which can be used for scanners on
  /// this table.
  ///
  /// A Bloom filter is a space-efficient probabilistic data structure used to
  /// test set membership with a possibility of false positive matches.
  /// See @c KuduBloomFilter for creating Bloom filters.
  ///
  /// IN list predicate can be used with small number of values; on the other
  /// hand with IN Bloom filter predicate large number of values can be tested
  /// for membership in a space-efficient manner.
  ///
  /// IN Bloom filter predicate may be automatically disabled if determined to
  /// be ineffective in filtering rows during scan requests.
  ///
  /// Users are expected to perform further filtering to guard against false
  /// positives and automatic disablement of an ineffective Bloom filter
  /// predicate to get precise set membership information.
  ///
  /// @param [in] col_name
  ///   Name of the column to which the predicate applies.
  /// @param [in] bloom_filters
  ///   Vector of Bloom filters that contain the values inserted to match
  ///   against the column. The column value must match against all the
  ///   supplied Bloom filters to be returned by the scanner. On return,
  ///   regardless of success or error, the returned predicate will take
  ///   ownership of the pointers contained in @c bloom_filters.
  /// @return Raw pointer to an IN Bloom filter predicate. The caller owns the
  ///   predicate until it is passed into KuduScanner::AddConjunctPredicate().
  ///   In the case of an error (e.g. an invalid column name),
  ///   a non-NULL value is still returned. The error will be returned when
  ///   attempting to add this predicate to a KuduScanner.
  KuduPredicate* NewInBloomFilterPredicate(const Slice& col_name,
                                           std::vector<KuduBloomFilter*>* bloom_filters);

  /// @name Advanced/Unstable API
  ///
  /// There are no guarantees on the stability of this client API.
  ///@{

  /// Create a new IN Bloom filter predicate using direct BlockBloomFilter
  /// pointers which can be used for scanners on this table.
  ///
  /// A Bloom filter is a space-efficient probabilistic data structure used to
  /// test set membership with a possibility of false positive matches.
  ///
  /// IN list predicate can be used with small number of values; on the other
  /// hand with IN Bloom filter predicate large number of values can be tested
  /// for membership in a space-efficient manner.
  ///
  /// IN Bloom filter predicate may be automatically disabled if determined to
  /// be ineffective in filtering rows during scan requests.
  ///
  /// Users are expected to perform further filtering to guard against false
  /// positives and automatic disablement of an ineffective Bloom filter
  /// predicate to get precise set membership information.
  ///
  /// @warning This method is experimental and may change or disappear in future.
  ///
  /// @param [in] col_name
  ///   Name of the column to which the predicate applies.
  /// @param bloom_filters
  ///   Vector of BlockBloomFilter pointers that contain the values inserted to
  ///   match against the column. The column value must match against all the
  ///   supplied Bloom filters to be returned by the scanner. On return,
  ///   regardless of success or error, the returned predicate will NOT take
  ///   ownership of the pointers contained in @c bloom_filters and caller is
  ///   responsible for the lifecycle management of the Bloom filters.
  ///   The supplied Bloom filters are expected to remain valid for the
  ///   lifetime of the KuduScanner.
  /// @return Raw pointer to an IN Bloom filter predicate. The caller owns the
  ///   predicate until it is passed into KuduScanner::AddConjunctPredicate().
  ///   In the case of an error (e.g. an invalid column name),
  ///   a non-NULL value is still returned. The error will be returned when
  ///   attempting to add this predicate to a KuduScanner.
  KuduPredicate* NewInBloomFilterPredicate(const Slice& col_name,
                                           const std::vector<Slice>& bloom_filters);
  ///@}

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

  /// @return The table's extra configuration properties.
  const std::map<std::string, std::string>& extra_configs() const;

  /// @cond PRIVATE_API

  /// List the partitions of this table in 'partitions'. This operation may
  /// involve RPC roundtrips to the leader master, and has a timeout equal
  /// to the table's client instance's default admin operation timeout.
  ///
  /// Private API.
  ///
  /// @param [out] partitions
  ///   The list of partitions of the table.
  /// @return Status object for the operation.
  Status ListPartitions(std::vector<Partition>* partitions) KUDU_NO_EXPORT;

  /// @endcond

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;
  friend class KuduPartitioner;
  friend class KuduScanToken;
  friend class KuduScanner;

  KuduTable(const sp::shared_ptr<KuduClient>& client,
            const std::string& name,
            const std::string& id,
            int num_replicas,
            const std::string& owner,
            const std::string& comment,
            const KuduSchema& schema,
            const PartitionSchema& partition_schema,
            const std::map<std::string, std::string>& extra_configs);

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

  /// Set the owner of the table.
  ///
  /// @param [in] new_owner
  ///   The new owner for the table.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* SetOwner(const std::string& new_owner);

  /// Set the comment on the table.
  ///
  /// @param [in] new_comment
  ///   The new comment on the table.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* SetComment(const std::string& new_comment);

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

  /// Add the specified range partition with custom hash schema to the table.
  ///
  /// @note The table alterer takes ownership of the partition object.
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
  /// @param [in] partition
  ///   The range partition to be created: it can have a custom hash schema.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* AddRangePartition(KuduRangePartition* partition);

  /// Add a range partition to the table with dimension label.
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
  /// @note See KuduTableCreator::dimension_label() for details on dimension label.
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
  /// @param [in] dimension_label
  ///   The dimension label for the tablet to be created.
  /// @param [in] lower_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   inclusive.
  /// @param [in] upper_bound_type
  ///   The type of the lower bound, either inclusive or exclusive. Defaults to
  ///   exclusive.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* AddRangePartitionWithDimension(
      KuduPartialRow* lower_bound,
      KuduPartialRow* upper_bound,
      const std::string& dimension_label,
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

  /// Change the table's extra configuration properties.
  ///
  /// @note These configuration properties will be merged into existing configuration properties.
  ///
  /// @note If the value of the kv pair is empty, the property will be unset.
  ///
  /// @param [in] extra_configs
  ///   The table's extra configuration properties.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* AlterExtraConfig(const std::map<std::string, std::string>& extra_configs);

  /// Set the disk size limit of the table by the super user.
  ///
  /// @note The table limit alterations, including disk_size_limit and row_count_limit,
  /// cannot be changed in the same alteration request with other alterations, because the
  /// table 'limit' alteration needs the super user permission.
  ///
  /// @note It is experimental and may change or disappear in future.
  /// This feature currently applies size limit on a single table.
  ///
  /// @param [in] disk_size_limit
  ///   The max table disk size, -1 is for no limit
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* SetTableDiskSizeLimit(int64_t disk_size_limit);

  /// Set the row count limit of the table by the super user.
  ///
  /// @note The table limit alterations, including disk_size_limit and row_count_limit,
  /// cannot be changed in the same alteration request with other alterations, because the
  /// table 'limit' alteration needs the super user permission.
  ///
  /// @note It is experimental and may change or disappear in future.
  /// This feature currently applies row count limit on a single table.
  ///
  /// @param [in] row_count_limit
  ///   The max row count of the table, -1 is for no limit
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* SetTableRowCountLimit(int64_t row_count_limit);

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

  /// @cond PRIVATE_API

  /// Whether to apply the alteration to external catalogs, such as the Hive
  /// Metastore, which the Kudu master has been configured to integrate with.
  ///
  /// Private API.
  ///
  /// @param [in] modify_external_catalogs
  ///   Whether to apply the alteration to external catalogs.
  /// @return Raw pointer to this alterer object.
  KuduTableAlterer* modify_external_catalogs(bool modify_external_catalogs) KUDU_NO_EXPORT;

  /// @endcond

  /// @return Status of the ALTER TABLE operation. The return value
  ///   may indicate an error in the alter operation,
  ///   or a misuse of the builder (e.g. add_column() with default_value=NULL).
  ///   In the latter case, only the last error is returned.
  Status Alter();

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduClient;
  friend class tools::TableAlter;
  friend class kudu::AlterTableTest;

  FRIEND_TEST(MultiTServerClientTest, TestSetReplicationFactor);

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
  /// write operations, if any, and then waits for the completion of all
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

  /// @return Cumulative write operation metrics since the beginning of the session.
  const ResourceMetrics& GetWriteOpMetrics() const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class ClientTest;
  friend class KuduClient;
  friend class KuduTransaction;
  friend class internal::Batcher;
  friend class tablet::FuzzTest;
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundAndErrorCollector);
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundApplyBlocks);
  FRIEND_TEST(ClientTest, TxnIdOfTransactionalSession);

  explicit KuduSession(const sp::shared_ptr<KuduClient>& client);
  KuduSession(const sp::shared_ptr<KuduClient>& client, const TxnId& txn_id);

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
    /// of waiting for in-flight ops whose timestamp is lower than
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
    READ_AT_SNAPSHOT,

    /// When @c READ_YOUR_WRITES is specified, the client will perform a read
    /// such that it follows all previously known writes and reads from this client.
    /// Specifically this mode:
    ///  (1) ensures read-your-writes and read-your-reads session guarantees,
    ///  (2) minimizes latency caused by waiting for outstanding write
    ///      ops to complete.
    ///
    /// Reads in this mode are not repeatable: two READ_YOUR_WRITES reads, even if
    /// they provide the same propagated timestamp bound, can execute at different
    /// timestamps and thus return different results.
    READ_YOUR_WRITES
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
  /// Keep the current remote scanner alive on the Tablet server for an
  /// additional time-to-live. This is useful if the interval in between
  /// NextBatch() calls is big enough that the remote scanner might be garbage
  /// collected. The scanner time-to-live can be configured on the tablet
  /// server via the --scanner_ttl_ms configuration flag and has a default
  /// of 60 seconds.
  ///
  /// This does not invalidate any previously fetched results.
  ///
  /// @return Operation result status. In particular, this method returns
  ///   a non-OK status if the scanner was already garbage collected or if the
  ///   TabletServer was unreachable, for any reason. Note that a non-OK
  ///   status returned by this method should not be taken as indication
  ///   that the scan has failed. Subsequent calls to NextBatch() might
  ///   still be successful, particularly if the scanner is configured to be
  ///   fault tolerant.
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
  /// This variant may not be used when the scan is configured with the
  /// COLUMNAR_LAYOUT RowFormatFlag.
  ///
  /// A single KuduScanBatch object may be reused. Each subsequent call
  /// replaces the data from the previous call, and invalidates any
  /// KuduScanBatch::RowPtr objects previously obtained from the batch.
  /// @param [out] batch
  ///   Placeholder for the result.
  /// @return Operation result status.
  Status NextBatch(KuduScanBatch* batch);

  /// Fetch the next batch of columnar results for this scanner.
  ///
  /// This variant may only be used when the scan is configured with the
  /// COLUMNAR_LAYOUT RowFormatFlag.
  ///
  /// A single KuduColumnarScanBatch object may be reused. Each subsequent call
  /// replaces the data from the previous call, and invalidates any
  /// Slice objects previously obtained from the batch.
  /// @param [out] batch
  ///   Placeholder for the result.
  /// @return Operation result status.
  Status NextBatch(KuduColumnarScanBatch* batch);

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
  /// @note This method is experimental and will either disappear or
  ///   change in a future release.
  ///
  /// @param [in] snapshot_timestamp
  ///   Timestamp to set in raw encoded form
  ///   (i.e. as returned by a previous call to a server).
  /// @return Operation result status.
  Status SetSnapshotRaw(uint64_t snapshot_timestamp) WARN_UNUSED_RESULT;

  /// @cond PRIVATE_API

  /// Set the start and end timestamp for a diff scan. The timestamps should be
  /// encoded HT timestamps.
  ///
  /// Additionally sets any other scan properties required by diff scans.
  ///
  /// Private API.
  ///
  /// @param [in] start_timestamp
  ///   Start timestamp to set in raw encoded form
  ///   (i.e. as returned by a previous call to a server).
  /// @param [in] end_timestamp
  ///   End timestamp to set in raw encoded form
  ///   (i.e. as returned by a previous call to a server).
  /// @return Operation result status.
  Status SetDiffScan(uint64_t start_timestamp, uint64_t end_timestamp)
      WARN_UNUSED_RESULT KUDU_NO_EXPORT;

  /// @endcond

  /// Set the maximum time that Open() and NextBatch() are allowed to take.
  ///
  /// @param [in] millis
  ///   Timeout to set (in milliseconds). Must be greater than 0.
  /// @return Operation result status.
  Status SetTimeoutMillis(int millis);

  /// @return Schema of the projection being scanned.
  KuduSchema GetProjectionSchema() const;

  /// @return KuduTable being scanned.
  sp::shared_ptr<KuduTable> GetKuduTable();

  /// @name Advanced/Unstable API
  ///
  /// Modifier flags for the row format returned from the server.
  ///
  /// @note Each flag corresponds to a bit that gets set on a bitset that is sent
  ///   to the server. See SetRowFormatFlags() for example usage.
  ///@{

  /// No flags set.
  static const uint64_t NO_FLAGS = 0;
  /// @note This flag actually wastes throughput by making messages larger than they need to
  ///   be. It exists merely for compatibility reasons and requires the user to know the row
  ///   format in order to decode the data. That is, if this flag is enabled, the user _must_
  ///   use KuduScanBatch::direct_data() and KuduScanBatch::indirect_data() to obtain the row
  ///   data for further decoding. Using KuduScanBatch::Row() might yield incorrect/corrupt
  ///   results and might even cause the client to crash.
  static const uint64_t PAD_UNIXTIME_MICROS_TO_16_BYTES = 1 << 0;

  /// Enable column-oriented data transfer. The server will transfer data to the client
  /// in a columnar format rather than a row-wise format. The KuduColumnarScanBatch API
  /// must be used to fetch results from this scan.
  ///
  /// NOTE: older versions of the Kudu server do not support this feature. Clients
  /// aiming to support compatibility with previous versions should have a fallback
  /// code path.
  static const uint64_t COLUMNAR_LAYOUT = 1 << 1;

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
  ///
  /// @param [in] flags
  ///   Row format modifier flags to set.
  /// @return Operation result status.
  Status SetRowFormatFlags(uint64_t flags);
  ///@}

  /// Set the maximum number of rows the scanner should return.
  ///
  /// @param [in] limit
  ///   Limit on the number of rows to return.
  /// @return Operation result status.
  Status SetLimit(int64_t limit) WARN_UNUSED_RESULT;

  /// @return String representation of this scan.
  ///
  /// @internal
  /// @note This method must not be used in log messages because it contains
  ///   sensitive predicate values. Use Scanner::Data::DebugString instead.
  std::string ToString() const;

 private:
  class KUDU_NO_EXPORT Data;

  Status NextBatch(internal::ScanBatchDataInterface* batch);

  friend class KuduScanToken;
  friend class FlexPartitioningTest;
  FRIEND_TEST(ClientTest, TestBlockScannerHijackingAttempts);
  FRIEND_TEST(ClientTest, TestScanCloseProxy);
  FRIEND_TEST(ClientTest, TestScanFaultTolerance);
  FRIEND_TEST(ClientTest, TestScanNoBlockCaching);
  FRIEND_TEST(ClientTest, TestScanTimeout);
  FRIEND_TEST(ClientTest, TestReadAtSnapshotNoTimestampSet);
  FRIEND_TEST(ConsistencyITest, TestSnapshotScanTimestampReuse);
  FRIEND_TEST(ScanTokenTest, TestScanTokens);
  FRIEND_TEST(ScanTokenTest, TestScanTokens_NonUniquePrimaryKey);

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
  /// The resulting string can be deserialized with
  /// @c KuduScanToken::Deserialize() to
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

  /// @cond PRIVATE_API

  /// @copydoc KuduScanner::SetDiffScan
  Status SetDiffScan(uint64_t start_timestamp, uint64_t end_timestamp)
      WARN_UNUSED_RESULT KUDU_NO_EXPORT;
  /// @endcond

  /// @copydoc KuduScanner::SetTimeoutMillis
  Status SetTimeoutMillis(int millis) WARN_UNUSED_RESULT;

  /// If the table metadata is included on the scan token a GetTableSchema
  /// RPC call to the master can be avoided when deserializing each scan token
  /// into a scanner.
  ///
  /// @param [in] include_metadata
  ///   true, if table metadata should be included.
  /// @return Operation result status.
  Status IncludeTableMetadata(bool include_metadata) WARN_UNUSED_RESULT;

  /// If the tablet metadata is included on the scan token a GetTableLocations
  /// RPC call to the master can be avoided when scanning with a scanner constructed
  /// from a scan token.
  ///
  /// @param [in] include_metadata
  ///   true, if table metadata should be included.
  /// @return Operation result status.
  Status IncludeTabletMetadata(bool include_metadata) WARN_UNUSED_RESULT;

  /// Build the set of scan tokens.
  ///
  /// The builder may be reused after this call.
  ///
  /// @param [out] tokens
  ///   Result set of tokens. The caller takes ownership of the container
  ///   elements.
  /// @return Operation result status.
  Status Build(std::vector<KuduScanToken*>* tokens) WARN_UNUSED_RESULT;

  /// Set the size of the data in each key range.
  /// The default value is 0 without set and tokens build by meta cache.
  /// It's corresponding to 'setSplitSizeBytes' in Java client.
  void SetSplitSizeBytes(uint64_t split_size_bytes);

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
  ///
  /// @param [in] timeout
  ///   The timeout to set.
  /// @return Pointer to the result object.
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
  ///
  /// @return Operation result status.
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

  /// @return the number of partitions known by this partitioner.
  ///   The partition indices returned by @c PartitionRow are guaranteed
  ///   to be less than this value.
  int NumPartitions() const;

  /// Determine the partition index that the given row falls into.
  ///
  /// @param [in] row
  ///   The row to be partitioned.
  /// @param [out] partition
  ///   The resulting partition index, or -1 if the row falls into a
  ///   non-covered range. The result will be less than @c NumPartitions().
  ///
  /// @return Status::OK if successful. May return a bad Status if the
  ///   provided row does not have all columns of the partition key
  ///   set.
  Status PartitionRow(const KuduPartialRow& row, int* partition);
 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduPartitionerBuilder;

  explicit KuduPartitioner(Data* data);
  Data* data_; // Owned.

  DISALLOW_COPY_AND_ASSIGN(KuduPartitioner);
};


} // namespace client
} // namespace kudu
#endif
