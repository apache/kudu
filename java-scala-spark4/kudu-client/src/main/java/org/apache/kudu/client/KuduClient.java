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

package org.apache.kudu.client;

import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.master.Master.TableIdentifierPB;

/**
 * A synchronous and thread-safe client for Kudu.
 * <p>
 * This class acts as a wrapper around {@link AsyncKuduClient} which contains all the relevant
 * documentation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KuduClient.class);
  public static final long NO_TIMESTAMP = -1;

  @InterfaceAudience.LimitedPrivate("Test")
  final AsyncKuduClient asyncClient;

  KuduClient(AsyncKuduClient asyncClient) {
    this.asyncClient = asyncClient;
  }

  /**
   * Updates the last timestamp received from a server. Used for CLIENT_PROPAGATED
   * external consistency.
   *
   * @param lastPropagatedTimestamp the last timestamp received from a server.
   */
  public void updateLastPropagatedTimestamp(long lastPropagatedTimestamp) {
    asyncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp);
  }

  /**
   * Returns the last timestamp received from a server. Used for CLIENT_PROPAGATED
   * external consistency. Note that the returned timestamp is encoded and cannot be
   * interpreted as a raw timestamp.
   *
   * @return a long indicating the specially-encoded last timestamp received from a server
   */
  public long getLastPropagatedTimestamp() {
    return asyncClient.getLastPropagatedTimestamp();
  }

  /**
   * Checks if the client received any timestamps from a server. Used for
   * CLIENT_PROPAGATED external consistency.
   *
   * @return true if last propagated timestamp has been set
   */
  public boolean hasLastPropagatedTimestamp() {
    return asyncClient.hasLastPropagatedTimestamp();
  }

  /**
   * Returns a string representation of this client's location. If this
   * client was not assigned a location, returns the empty string.
   *
   * @return a string representation of this client's location
   */
  public String getLocationString() {
    return asyncClient.getLocationString();
  }

  /**
   * Returns the ID of the cluster that this client is connected to.
   * It will be an empty string if the client is not connected or
   * the client is connected to a cluster that doesn't support
   * cluster IDs.
   *
   * @return the ID of the cluster that this client is connected to
   */
  public String getClusterId() {
    return asyncClient.getClusterId();
  }

  /**
   * Returns the unique client id assigned to this client.
   * @return the unique client id assigned to this client.
   */
  String getClientId() {
    return asyncClient.getClientId();
  }

  /**
   * Returns the Hive Metastore configuration of the cluster.
   *
   * @return the Hive Metastore configuration of the cluster
   * @throws KuduException if the configuration can not be retrieved
   */
  @InterfaceAudience.LimitedPrivate("Impala")
  @InterfaceStability.Unstable
  public HiveMetastoreConfig getHiveMetastoreConfig() throws KuduException {
    return joinAndHandleException(asyncClient.getHiveMetastoreConfig());
  }

  /**
   * Create a table on the cluster with the specified name, schema, and table configurations.
   * @param name the table's name
   * @param schema the table's schema
   * @param builder a builder containing the table's configurations
   * @return an object to communicate with the created table
   * @throws KuduException if anything went wrong
   */
  public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
      throws KuduException {
    Deferred<KuduTable> d = asyncClient.createTable(name, schema, builder);
    return joinAndHandleException(d);
  }

  /**
   * Waits for all of the tablets in a table to be created, or until the
   * default admin operation timeout is reached.
   * @param name the table's name
   * @return true if the table is done being created, or false if the default
   *         admin operation timeout was reached.
   * @throws KuduException for any error returned by sending RPCs to the master
   *         (e.g. the table does not exist)
   */
  public boolean isCreateTableDone(String name) throws KuduException {
    TableIdentifierPB.Builder table = TableIdentifierPB.newBuilder().setTableName(name);
    Deferred<KuduTable> d = asyncClient.getDelayedIsCreateTableDoneDeferred(table, null, null);
    try {
      joinAndHandleException(d);
    } catch (KuduException e) {
      if (e.getStatus().isTimedOut()) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Delete a table on the cluster with the specified name.
   * The deleted table may turn to soft-deleted status with the flag
   * default_deleted_table_reserve_seconds set to nonzero on the master side.
   *
   * @param name the table's name
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public DeleteTableResponse deleteTable(String name) throws KuduException {
    Deferred<DeleteTableResponse> d = asyncClient.deleteTable(name);
    return joinAndHandleException(d);
  }

  /**
   * SoftDelete a table on the cluster with the specified name, the table will be
   * reserved for reserveSeconds before being purged.
   * @param name the table's name
   * @param reserveSeconds the soft deleted table to be alive time
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public DeleteTableResponse deleteTable(String name,
                                         int reserveSeconds) throws KuduException {
    Deferred<DeleteTableResponse> d = asyncClient.deleteTable(name, reserveSeconds);
    return joinAndHandleException(d);
  }

  /**
   * Recall a deleted table on the cluster with the specified table id
   * @param id the table's id
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public RecallDeletedTableResponse recallDeletedTable(String id) throws KuduException {
    Deferred<RecallDeletedTableResponse> d = asyncClient.recallDeletedTable(id);
    return joinAndHandleException(d);
  }

  /**
   * Recall a deleted table on the cluster with the specified table id
   * and give the recalled table the new table name
   * @param id the table's id
   * @param newTableName the recalled table's new name
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public RecallDeletedTableResponse recallDeletedTable(String id,
                                                       String newTableName) throws KuduException {
    Deferred<RecallDeletedTableResponse> d = asyncClient.recallDeletedTable(id,
                                                         newTableName);
    return joinAndHandleException(d);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   * @param name the table's name (old name if the table is being renamed)
   * @param ato the alter table options
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public AlterTableResponse alterTable(String name, AlterTableOptions ato) throws KuduException {
    Deferred<AlterTableResponse> d = asyncClient.alterTable(name, ato);
    return joinAndHandleException(d);
  }

  /**
   * Waits for all of the tablets in a table to be altered, or until the
   * default admin operation timeout is reached.
   * @param name the table's name
   * @return true if the table is done being altered, or false if the default
   *         admin operation timeout was reached.
   * @throws KuduException for any error returned by sending RPCs to the master
   *         (e.g. the table does not exist)
   */
  public boolean isAlterTableDone(String name) throws KuduException {
    TableIdentifierPB.Builder table = TableIdentifierPB.newBuilder().setTableName(name);
    Deferred<AlterTableResponse> d =
        asyncClient.getDelayedIsAlterTableDoneDeferred(table, null, null);
    try {
      joinAndHandleException(d);
    } catch (KuduException e) {
      if (e.getStatus().isTimedOut()) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Get the list of running tablet servers.
   * @return a list of tablet servers
   * @throws KuduException if anything went wrong
   */
  public ListTabletServersResponse listTabletServers() throws KuduException {
    Deferred<ListTabletServersResponse> d = asyncClient.listTabletServers();
    return joinAndHandleException(d);
  }

  /**
   * Get the list of all the regular tables.
   * @return a list of all the regular tables
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getTablesList() throws KuduException {
    return getTablesList(null);
  }

  /**
   * Get a list of regular table names. Passing a null filter returns all the tables.
   * When a filter is specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that contains the list of table names
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getTablesList(String nameFilter) throws KuduException {
    Deferred<ListTablesResponse> d = asyncClient.getTablesList(nameFilter, false);
    return joinAndHandleException(d);
  }

  /**
   * Get the list of all the soft deleted tables.
   * @return a list of all the soft deleted tables
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getSoftDeletedTablesList() throws KuduException {
    return getSoftDeletedTablesList(null);
  }

  /**
   * Get list of soft deleted table names. Passing a null filter returns all the tables.
   * When a filter is specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that contains the list of table names
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getSoftDeletedTablesList(String nameFilter) throws KuduException {
    Deferred<ListTablesResponse> d = asyncClient.getTablesList(nameFilter, true);
    return joinAndHandleException(d);
  }

  /**
   * Get table's statistics from master.
   * @param name the table's name
   * @return the statistics of table
   * @throws KuduException if anything went wrong
   */
  public KuduTableStatistics getTableStatistics(String name) throws KuduException {
    Deferred<KuduTableStatistics> d = asyncClient.getTableStatistics(name);
    return joinAndHandleException(d);
  }

  /**
   * Test if a table exists.
   * @param name a non-null table name
   * @return true if the table exists, else false
   * @throws KuduException if anything went wrong
   */
  public boolean tableExists(String name) throws KuduException {
    Deferred<Boolean> d = asyncClient.tableExists(name);
    return joinAndHandleException(d);
  }

  /**
   * Open the table with the given id.
   *
   * @param id the id of the table to open
   * @return a KuduTable if the table exists
   * @throws KuduException if anything went wrong
   */
  KuduTable openTableById(final String id) throws KuduException {
    Deferred<KuduTable> d = asyncClient.openTableById(id);
    return joinAndHandleException(d);
  }

  /**
   * Open the table with the given name.
   *
   * New range partitions created by other clients will immediately be available
   * after opening the table.
   *
   * @param name table to open
   * @return a KuduTable if the table exists
   * @throws KuduException if anything went wrong
   */
  public KuduTable openTable(final String name) throws KuduException {
    Deferred<KuduTable> d = asyncClient.openTable(name);
    return joinAndHandleException(d);
  }

  /**
   * Create a new session for interacting with the cluster.
   * User is responsible for destroying the session object.
   * This is a fully local operation (no RPCs or blocking).
   * @return a synchronous wrapper around KuduSession.
   */
  public KuduSession newSession() {
    AsyncKuduSession session = asyncClient.newSession();
    return new KuduSession(session);
  }

  /**
   * Start a new multi-row distributed transaction.
   * <p>
   * Start a new multi-row transaction and return a handle for the transactional
   * object to manage the newly started transaction. Under the hood, this makes
   * an RPC call to the Kudu cluster and registers a newly created transaction
   * in the system. This call is blocking.
   *
   * @return a handle to the newly started transaction in case of success
   */
  public KuduTransaction newTransaction() throws KuduException {
    KuduTransaction txn = new KuduTransaction(asyncClient);
    txn.begin();
    return txn;
  }

  /**
   * Check if statistics collection is enabled for this client.
   * @return true if it is enabled, else false
   */
  public boolean isStatisticsEnabled() {
    return asyncClient.isStatisticsEnabled();
  }

  /**
   * Get the statistics object of this client.
   *
   * @return this client's Statistics object
   * @throws IllegalStateException thrown if statistics collection has been disabled
   */
  public Statistics getStatistics() {
    return asyncClient.getStatistics();
  }

  /**
   * Creates a new {@link KuduScanner.KuduScannerBuilder} for a particular table.
   * @param table the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return a new scanner builder for the table
   */
  public KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table) {
    return new KuduScanner.KuduScannerBuilder(asyncClient, table);
  }

  /**
   * Creates a new {@link KuduScanToken.KuduScanTokenBuilder} for a particular table.
   * Used for integrations with compute frameworks.
   * @param table the table you intend to scan
   * @return a new scan token builder for the table
   */
  public KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduTable table) {
    return new KuduScanToken.KuduScanTokenBuilder(asyncClient, table);
  }

  /**
   * Analogous to {@link #shutdown()}.
   * @throws KuduException if an error happens while closing the connections
   */
  @Override
  public void close() throws KuduException {
    try {
      asyncClient.close();
    } catch (Exception e) {
      throw KuduException.transformException(e);
    }
  }

  /**
   * Performs a graceful shutdown of this instance.
   * @throws KuduException if anything went wrong
   */
  public void shutdown() throws KuduException {
    Deferred<ArrayList<Void>> d = asyncClient.shutdown();
    joinAndHandleException(d);
  }

  /**
   * Export serialized authentication data that may be passed to a different
   * client instance and imported to provide that client the ability to connect
   * to the cluster.
   */
  @InterfaceStability.Unstable
  public byte[] exportAuthenticationCredentials() throws KuduException {
    return joinAndHandleException(asyncClient.exportAuthenticationCredentials());
  }

  /**
   * Import data allowing this client to authenticate to the cluster.
   * This will typically be used before making any connections to servers
   * in the cluster.
   *
   * Note that, if this client has already been used by one user, this
   * method cannot be used to switch authenticated users. Attempts to
   * do so have undefined results, and may throw an exception.
   *
   * @param authnData then authentication data provided by a prior call to
   * {@link #exportAuthenticationCredentials()}
   */
  @InterfaceStability.Unstable
  public void importAuthenticationCredentials(byte[] authnData) {
    asyncClient.importAuthenticationCredentials(authnData);
  }

  /**
   * Mark the given CA certificates (in DER format) as the trusted ones for the
   * client. The provided list of certificates replaces any previously set ones.
   *
   * @param certificates list of certificates to trust (in DER format)
   * @throws CertificateException if any of the specified certificates were invalid
   */
  @InterfaceStability.Unstable
  public void trustedCertificates(List<ByteString> certificates) throws CertificateException {
    asyncClient.trustedCertificates(certificates);
  }

  /**
   * Set JWT (JSON Web Token) to authenticate the client to a server.
   * <p>
   * @note If {@link #importAuthenticationCredentials(byte[] authnData)} and
   * this method are called on the same object, the JWT provided with this call
   * overrides the corresponding JWT that comes as a part of the imported
   * authentication credentials (if present).
   *
   * @param jwt The JSON web token to set.
   */
  @InterfaceStability.Unstable
  public void jwt(String jwt) {
    asyncClient.jwt(jwt);
  }

  /**
   * Get the timeout used for operations on sessions and scanners.
   * @return a timeout in milliseconds
   */
  public long getDefaultOperationTimeoutMs() {
    return asyncClient.getDefaultOperationTimeoutMs();
  }

  /**
   * Get the timeout used for admin operations.
   * @return a timeout in milliseconds
   */
  public long getDefaultAdminOperationTimeoutMs() {
    return asyncClient.getDefaultAdminOperationTimeoutMs();
  }

  /**
   * @return the list of master addresses, stringified using commas to separate
   * them
   */
  public String getMasterAddressesAsString() {
    return asyncClient.getMasterAddressesAsString();
  }

  /**
   * Sends a request to the master to check if the cluster supports ignore operations, including
   * InsertIgnore, UpdateIgnore and DeleteIgnore operations.
   * @return true if the cluster supports ignore operations
   */
  @InterfaceAudience.Private
  public boolean supportsIgnoreOperations() throws KuduException {
    return joinAndHandleException(asyncClient.supportsIgnoreOperations());
  }

  /**
   * @return a HostAndPort describing the current leader master
   * @throws KuduException if a leader master could not be found in time
   */
  @InterfaceAudience.LimitedPrivate("Test")
  public HostAndPort findLeaderMasterServer() throws KuduException {
    // Consult the cache to determine the current leader master.
    //
    // If one isn't found, issue an RPC that retries until the leader master
    // is discovered. We don't need the RPC's results; it's just a simple way to
    // wait until a leader master is elected.
    TableLocationsCache.Entry entry = asyncClient.getTableLocationEntry(
        AsyncKuduClient.MASTER_TABLE_NAME_PLACEHOLDER, null);
    if (entry == null) {
      // If there's no leader master, this will time out and throw an exception.
      listTabletServers();

      entry = asyncClient.getTableLocationEntry(
          AsyncKuduClient.MASTER_TABLE_NAME_PLACEHOLDER, null);
    }
    Preconditions.checkNotNull(entry);
    Preconditions.checkState(!entry.isNonCoveredRange());
    ServerInfo info = entry.getTablet().getLeaderServerInfo();
    Preconditions.checkNotNull(info);
    return info.getHostAndPort();
  }

  // Helper method to handle joining and transforming the Exception we receive.
  static <R> R joinAndHandleException(Deferred<R> deferred) throws KuduException {
    try {
      return deferred.join();
    } catch (Exception e) {
      throw KuduException.transformException(e);
    }
  }

  /**
   * Builder class to use in order to connect to Kudu.
   * All the parameters beyond those in the constructors are optional.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static final class KuduClientBuilder {
    private AsyncKuduClient.AsyncKuduClientBuilder clientBuilder;

    /**
     * Creates a new builder for a client that will connect to the specified masters.
     * @param masterAddresses comma-separated list of "host:port" pairs of the masters
     */
    public KuduClientBuilder(String masterAddresses) {
      clientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses);
    }

    /**
     * Creates a new builder for a client that will connect to the specified masters.
     *
     * <p>Here are some examples of recognized formats:
     * <ul>
     *   <li>example.com
     *   <li>example.com:80
     *   <li>192.0.2.1
     *   <li>192.0.2.1:80
     *   <li>[2001:db8::1]
     *   <li>[2001:db8::1]:80
     *   <li>2001:db8::1
     * </ul>
     *
     * @param masterAddresses list of master addresses
     */
    public KuduClientBuilder(List<String> masterAddresses) {
      clientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses);
    }

    /**
     * Sets the default timeout used for administrative operations (e.g. createTable, deleteTable,
     * etc).
     * Optional.
     * If not provided, defaults to 30s.
     * A value of 0 disables the timeout.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     */
    public KuduClientBuilder defaultAdminOperationTimeoutMs(long timeoutMs) {
      clientBuilder.defaultAdminOperationTimeoutMs(timeoutMs);
      return this;
    }

    /**
     * Sets the default timeout used for user operations (using sessions and scanners).
     * Optional.
     * If not provided, defaults to 30s.
     * A value of 0 disables the timeout.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     */
    public KuduClientBuilder defaultOperationTimeoutMs(long timeoutMs) {
      clientBuilder.defaultOperationTimeoutMs(timeoutMs);
      return this;
    }

    /**
     * Sets the default timeout used for connection negotiation.
     * Optional.
     * If not provided, defaults to 10s.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     */
    public KuduClientBuilder connectionNegotiationTimeoutMs(long timeoutMs) {
      clientBuilder.connectionNegotiationTimeoutMs(timeoutMs);
      return this;
    }

    /**
     * Socket read timeouts are no longer used in the Java client and have no effect.
     * Setting this has no effect.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     * @deprecated socket read timeouts are no longer used
     */
    @Deprecated
    public KuduClientBuilder defaultSocketReadTimeoutMs(long timeoutMs) {
      LOG.info("defaultSocketReadTimeoutMs is deprecated");
      return this;
    }

    /**
     * Disable this client's collection of statistics.
     * Statistics are enabled by default.
     * @return this builder
     */
    public KuduClientBuilder disableStatistics() {
      clientBuilder.disableStatistics();
      return this;
    }

    /**
     * @deprecated the bossExecutor is no longer used and will have no effect if provided
     */
    @Deprecated
    public KuduClientBuilder nioExecutors(Executor bossExecutor, Executor workerExecutor) {
      clientBuilder.nioExecutors(bossExecutor, workerExecutor);
      return this;
    }

    /**
     * Set the executor which will be used for the embedded Netty workers.
     *
     * Optional.
     * If not provided, uses a simple cached threadpool. If workerExecutor is null,
     * then such a thread pool will be used.
     * Note: executor's max thread number must be greater or equal to corresponding
     * worker count, or netty cannot start enough threads, and client will get stuck.
     * If not sure, please just use CachedThreadPool.
     */
    public KuduClientBuilder nioExecutor(Executor workerExecutor) {
      clientBuilder.nioExecutor(workerExecutor);
      return this;
    }

    /**
     * @deprecated the bossExecutor is no longer used and will have no effect if provided
     */
    @Deprecated
    public KuduClientBuilder bossCount(int bossCount) {
      LOG.info("bossCount is deprecated");
      return this;
    }

    /**
     * Set the maximum number of worker threads.
     * A worker thread performs non-blocking read and write for one or more
     * Netty Channels in a non-blocking mode.
     *
     * Optional.
     * If not provided, (2 * the number of available processors) is used. If
     * this client instance will be used on a machine running many client
     * instances, it may be wise to lower this count, for example to avoid
     * resource limits, at the possible cost of some performance of this client
     * instance.
     */
    public KuduClientBuilder workerCount(int workerCount) {
      clientBuilder.workerCount(workerCount);
      return this;
    }

    /**
     * Set the SASL protocol name.
     * SASL protocol name is used when connecting to a secure (Kerberos-enabled)
     * cluster. It must match the servers' service principal name (SPN).
     *
     * Optional.
     * If not provided, it will use the default SASL protocol name ("kudu").
     * @return this builder
     */
    public KuduClientBuilder saslProtocolName(String saslProtocolName) {
      clientBuilder.saslProtocolName(saslProtocolName);
      return this;
    }

    /**
     * Require authentication for the connection to a remote server.
     *
     * If it's set to true, the client will require mutual authentication between
     * the server and the client. If the server doesn't support authentication,
     * or it's disabled, the client will fail to connect.
     */
    public KuduClientBuilder requireAuthentication(boolean requireAuthentication) {
      clientBuilder.requireAuthentication(requireAuthentication);
      return this;
    }

    /**
     * Require encryption for the connection to a remote server.
     *
     * If it's set to REQUIRED or REQUIRED_LOOPBACK, the client will
     * require encrypting the traffic between the server and the client.
     * If the server doesn't support encryption, or if it's disabled, the
     * client will fail to connect.
     *
     * Loopback connections are encrypted only if 'encryption_policy' is
     * set to REQUIRE_LOOPBACK, or if it's required by the server.
     *
     * The default value is OPTIONAL, which allows connecting to servers without
     * encryption as well, but it will still attempt to use it if the server
     * supports it.
     */
    public KuduClientBuilder encryptionPolicy(AsyncKuduClient.EncryptionPolicy encryptionPolicy) {
      clientBuilder.encryptionPolicy(encryptionPolicy);
      return this;
    }

    /**
     * Creates a new client that connects to the masters.
     * Doesn't block and won't throw an exception if the masters don't exist.
     * @return a new asynchronous Kudu client
     */
    public KuduClient build() {
      AsyncKuduClient client = clientBuilder.build();
      return new KuduClient(client);
    }
  }
}
