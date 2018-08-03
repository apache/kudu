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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import com.stumbleupon.async.Callback;
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

  private static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);
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
   * @param name the table's name
   * @return an rpc response object
   * @throws KuduException if anything went wrong
   */
  public DeleteTableResponse deleteTable(String name) throws KuduException {
    Deferred<DeleteTableResponse> d = asyncClient.deleteTable(name);
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
    Deferred<AlterTableResponse> d = asyncClient.getDelayedIsAlterTableDoneDeferred(table, null, null);
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
   * Get the list of all the tables.
   * @return a list of all the tables
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getTablesList() throws KuduException {
    return getTablesList(null);
  }

  /**
   * Get a list of table names. Passing a null filter returns all the tables. When a filter is
   * specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that contains the list of table names
   * @throws KuduException if anything went wrong
   */
  public ListTablesResponse getTablesList(String nameFilter) throws KuduException {
    Deferred<ListTablesResponse> d = asyncClient.getTablesList(nameFilter);
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
     * Sets the default timeout to use when waiting on data from a socket.
     * Optional.
     * If not provided, defaults to 10s.
     * A value of 0 disables the timeout.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     */
    public KuduClientBuilder defaultSocketReadTimeoutMs(long timeoutMs) {
      clientBuilder.defaultSocketReadTimeoutMs(timeoutMs);
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
     * Set the executors which will be used for the embedded Netty boss and workers.
     * Optional.
     * If not provided, uses a simple cached threadpool. If either argument is null,
     * then such a thread pool will be used in place of that argument.
     * Note: executor's max thread number must be greater or equal to corresponding
     * worker count, or netty cannot start enough threads, and client will get stuck.
     * If not sure, please just use CachedThreadPool.
     */
    public KuduClientBuilder nioExecutors(Executor bossExecutor, Executor workerExecutor) {
      clientBuilder.nioExecutors(bossExecutor, workerExecutor);
      return this;
    }

    /**
     * Set the maximum number of boss threads.
     * Optional.
     * If not provided, 1 is used.
     */
    public KuduClientBuilder bossCount(int bossCount) {
      clientBuilder.bossCount(bossCount);
      return this;
    }

    /**
     * Set the maximum number of worker threads.
     * Optional.
     * If not provided, (2 * the number of available processors) is used.
     */
    public KuduClientBuilder workerCount(int workerCount) {
      clientBuilder.workerCount(workerCount);
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
