// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;
import org.kududb.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A synchronous and thread-safe client for Kudu.
 * <p>
 * This class acts as a wrapper around {@link AsyncKuduClient}. The {@link Deferred} objects are
 * joined against using the configured timeout (see {@link KuduClient#setTimeoutMillis(long)}).
 */
public class KuduClient {

  public static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);

  private final AsyncKuduClient asyncClient;
  private long currentTimeoutMs = 5000;

  /**
   * Create a new client that connects to masters specified by a comma-separated
   * list.
   * Doesn't block and won't throw an exception if the masters don't exist.
   * @param masterAddresses Comma-separated list of "host:port" pairs of the masters
   */
  public KuduClient(String masterAddresses) {
    this.asyncClient = new AsyncKuduClient(masterAddresses);
  }

  /**
   * Create a new client that connects to the masters.
   * Doesn't block and won't throw an exception if the masters don't exist.
   * @param masterAddresses list of master addresses
   */
  public KuduClient(List<HostAndPort> masterAddresses) {
    this.asyncClient = new AsyncKuduClient(masterAddresses);
  }

  /**
   * Package-private way of creating a client directly from an AsyncKuduClient. The passed client
   * must _not_ be shutdown before this client.
   * @param asyncClient
   */
  KuduClient(AsyncKuduClient asyncClient) {
    this.asyncClient = asyncClient;
  }

  /**
   * Create a table on the cluster with the specified name and schema. Default table
   * configurations are used, mainly the table will have one tablet.
   * @param name Table's name
   * @param schema Table's schema
   * @return an rpc response object
   */
  public CreateTableResponse createTable(String name, Schema schema) throws Exception {
    return createTable(name, schema, new CreateTableBuilder());
  }

  /**
   * Create a table on the cluster with the specified name, schema, and table configurations.
   * @param name the table's name
   * @param schema the table's schema
   * @param builder a builder containing the table's configurations
   * @return an rpc response object
   */
  public CreateTableResponse createTable(String name, Schema schema, CreateTableBuilder builder)
      throws Exception {
    Deferred<CreateTableResponse> d = asyncClient.createTable(name, schema, builder);
    return d.join(currentTimeoutMs);
  }

  /**
   * Delete a table on the cluster with the specified name.
   * @param name the table's name
   * @return an rpc response object
   */
  public DeleteTableResponse deleteTable(String name) throws Exception {
    Deferred<DeleteTableResponse> d = asyncClient.deleteTable(name);
    return d.join(currentTimeoutMs);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   *
   * When the method returns it only indicates that the master accepted the alter
   * command, use {@link KuduClient#isAlterTableDone(String)} to know when the alter finishes.
   * @param name the table's name, if this is a table rename then the old table name must be passed
   * @param atb the alter table builder
   * @return an rpc response object
   */
  public AlterTableResponse alterTable(String name, AlterTableBuilder atb) throws Exception {
    Deferred<AlterTableResponse> d = asyncClient.alterTable(name, atb);
    return d.join(currentTimeoutMs);
  }

  /**
   * Helper method that checks and waits until the completion of an alter command.
   * It will block until the alter command is done or the timeout is reached.
   * @param name Table's name, if the table was renamed then that name must be checked against
   * @return a boolean indicating if the table is done being altered
   */
  public boolean isAlterTableDone(String name) throws Exception {
    long totalSleepTime = 0;
    while (totalSleepTime < currentTimeoutMs) {
      long start = System.currentTimeMillis();

      Deferred<IsAlterTableDoneResponse> d = asyncClient.isAlterTableDone(name);
      IsAlterTableDoneResponse response;
      try {
        response = d.join(AsyncKuduClient.SLEEP_TIME);
      } catch (Exception ex) {
        throw ex;
      }

      if (response.isDone()) {
        return true;
      }

      // Count time that was slept and see if we need to wait a little more.
      long elapsed = System.currentTimeMillis() - start;
      // Don't oversleep the deadline.
      if (totalSleepTime + AsyncKuduClient.SLEEP_TIME > currentTimeoutMs) {
        return false;
      }
      // elapsed can be bigger if we slept about 500ms
      if (elapsed <= AsyncKuduClient.SLEEP_TIME) {
        LOG.debug("Alter not done, sleep " + (AsyncKuduClient.SLEEP_TIME - elapsed) +
            " and slept " + totalSleepTime);
        Thread.sleep(AsyncKuduClient.SLEEP_TIME - elapsed);
        totalSleepTime += AsyncKuduClient.SLEEP_TIME;
      } else {
        totalSleepTime += elapsed;
      }
    }
    return false;
  }

  /**
   * Get the list of running tablet servers.
   * @return a list of tablet servers
   */
  public ListTabletServersResponse listTabletServers() throws Exception {
    Deferred<ListTabletServersResponse> d = asyncClient.listTabletServers();
    return d.join(currentTimeoutMs);
  }

  /**
   * Get the list of all the tables.
   * @return a list of all the tables
   */
  public ListTablesResponse getTablesList() throws Exception {
    return getTablesList(null);
  }

  /**
   * Get a list of table names. Passing a null filter returns all the tables. When a filter is
   * specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that contains the list of table names
   */
  public ListTablesResponse getTablesList(String nameFilter) throws Exception {
    Deferred<ListTablesResponse> d = asyncClient.getTablesList(nameFilter);
    return d.join(currentTimeoutMs);
  }

  /**
   * Test if a table exists.
   * @param name a non-null table name
   * @return true if the table exists, else false
   */
  public boolean tableExists(String name) throws Exception {
    Deferred<Boolean> d = asyncClient.tableExists(name);
    return d.join(currentTimeoutMs);
  }

  /**
   * Open the table with the given name.
   * @param name table to open
   * @return a KuduTable if the table exists, else a MasterErrorException
   */
  public KuduTable openTable(final String name) throws Exception {
    Deferred<KuduTable> d = asyncClient.openTable(name);
    return d.join(currentTimeoutMs);
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
   * Creates a new {@link KuduScanner.KuduScannerBuilder} for a particular table.
   * @param table the name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return a new scanner builder for this table
   */
  public KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table, Schema schema) {
    return new KuduScanner.KuduScannerBuilder(asyncClient, table, schema);
  }

  /**
   * Performs a graceful shutdown of this instance.
   * @throws Exception
   */
  public void shutdown() throws Exception {
    Deferred<ArrayList<Void>> d = asyncClient.shutdown();
    d.join(currentTimeoutMs);
  }

  /**
   * Sets the timeout for the next operations.
   * The default timeout is 5 seconds.
   * A value of 0 disables the timeout functionality.
   * @param timeoutMs timeout in milliseconds
   */
  public void setTimeoutMillis(long timeoutMs) {
    this.currentTimeoutMs = timeoutMs;
  }
}
