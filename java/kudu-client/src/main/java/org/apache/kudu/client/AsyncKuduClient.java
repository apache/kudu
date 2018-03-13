/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kudu.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.kudu.client.ExternalConsistencyMode.CLIENT_PROPAGATED;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.master.Master;
import org.apache.kudu.master.Master.GetTableLocationsResponsePB;
import org.apache.kudu.util.AsyncUtil;
import org.apache.kudu.util.NetUtil;
import org.apache.kudu.util.Pair;
import org.apache.kudu.util.SecurityUtil;

/**
 * A fully asynchronous and thread-safe client for Kudu.
 * <p>
 * This client should be
 * instantiated only once. You can use it with any number of tables at the
 * same time. The only case where you should have multiple instances is when
 * you want to use multiple different clusters at the same time.
 * <p>
 * If you play by the rules, this client is completely
 * thread-safe. Read the documentation carefully to know what the requirements
 * are for this guarantee to apply.
 * <p>
 * This client is fully non-blocking, any blocking operation will return a
 * {@link Deferred} instance to which you can attach a {@link Callback} chain
 * that will execute when the asynchronous operation completes.
 *
 * <h1>Note regarding {@code KuduRpc} instances passed to this class</h1>
 * Every {@link KuduRpc} passed to a method of this class should not be
 * changed or re-used until the {@code Deferred} returned by that method
 * calls you back.  <strong>Changing or re-using any {@link KuduRpc} for
 * an RPC in flight will lead to <em>unpredictable</em> results and voids
 * your warranty</strong>.
 *
 * <h1>{@code throws} clauses</h1>
 * None of the asynchronous methods in this API are expected to throw an
 * exception.  But the {@link Deferred} object they return to you can carry an
 * exception that you should handle (using "errbacks", see the javadoc of
 * {@link Deferred}).  In order to be able to do proper asynchronous error
 * handling, you need to know what types of exceptions you're expected to face
 * in your errbacks.  In order to document that, the methods of this API use
 * javadoc's {@code @throws} to spell out the exception types you should
 * handle in your errback.  Asynchronous exceptions will be indicated as such
 * in the javadoc with "(deferred)".
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class AsyncKuduClient implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);
  public static final int SLEEP_TIME = 500;
  public static final byte[] EMPTY_ARRAY = new byte[0];
  public static final long NO_TIMESTAMP = -1;
  public static final long DEFAULT_OPERATION_TIMEOUT_MS = 30000;
  public static final long DEFAULT_SOCKET_READ_TIMEOUT_MS = 10000;
  private static final long MAX_RPC_ATTEMPTS = 100;

  /**
   * The number of tablets to fetch from the master in a round trip when performing
   * a lookup of a single partition (e.g. for a write), or re-looking-up a tablet with
   * stale information.
   */
  private static final int FETCH_TABLETS_PER_POINT_LOOKUP = 10;

  /**
   * The number of tablets to fetch from the master when looking up a range of
   * tablets.
   */
  static int FETCH_TABLETS_PER_RANGE_LOOKUP = 1000;

  private final ClientSocketChannelFactory channelFactory;

  /**
   * This map contains data cached from calls to the master's
   * GetTableLocations RPC. This map is keyed by table ID.
   */
  private final ConcurrentHashMap<String, TableLocationsCache> tableLocations =
      new ConcurrentHashMap<>();

  /** A cache to keep track of already opened connections to Kudu servers. */
  private final ConnectionCache connectionCache;

  @GuardedBy("sessions")
  private final Set<AsyncKuduSession> sessions = new HashSet<>();

  // Since RPCs to the masters also go through RpcProxy, we need to treat them as if they were a
  // normal table. We'll use the following fake table name to identify places where we need special
  // handling.
  // TODO(aserbin) clean this up
  static final String MASTER_TABLE_NAME_PLACEHOLDER =  "Kudu Master";
  private final KuduTable masterTable;
  private final List<HostAndPort> masterAddresses;

  private final HashedWheelTimer timer;

  /**
   * Timestamp required for HybridTime external consistency through timestamp
   * propagation.
   * @see src/kudu/common/common.proto
   */
  private long lastPropagatedTimestamp = NO_TIMESTAMP;

  /**
   * A table is considered not served when we get a TABLET_NOT_RUNNING error from the master
   * after calling GetTableLocations (it means that some tablets aren't ready to serve yet).
   * We cache this information so that concurrent RPCs sent just after creating a table don't
   * all try to hit the master for no good reason.
   */
  private final Set<String> tablesNotServed = Collections.newSetFromMap(new
      ConcurrentHashMap<String, Boolean>());

  /**
   * Semaphore used to rate-limit master lookups
   * Once we have more than this number of concurrent master lookups, we'll
   * start to throttle ourselves slightly.
   * @see #acquireMasterLookupPermit
   */
  private final Semaphore masterLookups = new Semaphore(50);

  private final Random sleepRandomizer = new Random();

  private final long defaultOperationTimeoutMs;

  private final long defaultAdminOperationTimeoutMs;

  private final long defaultSocketReadTimeoutMs;

  private final Statistics statistics;

  private final boolean statisticsDisabled;

  private final RequestTracker requestTracker;

  private final SecurityContext securityContext;

  /** A helper to facilitate re-acquiring of authentication token if current one expires. */
  private final AuthnTokenReacquirer tokenReacquirer;

  private volatile boolean closed;

  private AsyncKuduClient(AsyncKuduClientBuilder b) {
    this.channelFactory = b.createChannelFactory();
    this.masterAddresses = b.masterAddresses;
    this.masterTable = new KuduTable(this, MASTER_TABLE_NAME_PLACEHOLDER,
        MASTER_TABLE_NAME_PLACEHOLDER, null, null);
    this.defaultOperationTimeoutMs = b.defaultOperationTimeoutMs;
    this.defaultAdminOperationTimeoutMs = b.defaultAdminOperationTimeoutMs;
    this.defaultSocketReadTimeoutMs = b.defaultSocketReadTimeoutMs;
    this.statisticsDisabled = b.statisticsDisabled;
    this.statistics = statisticsDisabled ? null : new Statistics();
    this.timer = b.timer;
    this.requestTracker = new RequestTracker(UUID.randomUUID().toString().replace("-", ""));

    this.securityContext = new SecurityContext(b.subject);
    this.connectionCache = new ConnectionCache(
        securityContext, defaultSocketReadTimeoutMs, timer, channelFactory);
    this.tokenReacquirer = new AuthnTokenReacquirer(this);
  }

  /**
   * Get a proxy to send RPC calls to the specified server. The result proxy object does not
   * restrict the type of credentials that may be used to connect to the server: it will use the
   * secondary credentials if available, otherwise SASL credentials are used to authenticate
   * the client when negotiating the connection to the server.
   *
   * @param serverInfo server's information
   * @return the proxy object bound to the target server
   */
  @Nonnull
  RpcProxy newRpcProxy(final ServerInfo serverInfo) {
    return newRpcProxy(serverInfo, Connection.CredentialsPolicy.ANY_CREDENTIALS);
  }

  /**
   * Get a proxy to send RPC calls to the specified server. The result proxy object should use
   * a connection to the server negotiated with the specified credentials policy.
   *
   * @param serverInfo target server information
   * @param credentialsPolicy authentication credentials policy to use for the connection
   *                          negotiation
   * @return the proxy object bound to the target server
   */
  @Nonnull
  private RpcProxy newRpcProxy(final ServerInfo serverInfo,
                               Connection.CredentialsPolicy credentialsPolicy) {
    final Connection connection = connectionCache.getConnection(serverInfo, credentialsPolicy);
    return new RpcProxy(this, connection);
  }

  /**
   * Get a proxy to send RPC calls to Kudu master at the specified end-point.
   *
   * @param hostPort master end-point
   * @param credentialsPolicy credentials policy to use for the connection negotiation to the target
   *                          master server
   * @return the proxy object bound to the target master
   */
  @Nullable
  RpcProxy newMasterRpcProxy(HostAndPort hostPort,
                             Connection.CredentialsPolicy credentialsPolicy) {
    // We should have a UUID to construct ServerInfo for the master, but we have a chicken
    // and egg problem, we first need to communicate with the masters to find out about them,
    // and that's what we're trying to do. The UUID is just used for logging and cache key,
    // so instead we just use concatenation of master host and port, prefixed with "master-".
    final InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
    if (inetAddress == null) {
      // TODO(todd): should we log the resolution failure? throw an exception?
      return null;
    }
    return newRpcProxy(
        new ServerInfo(getFakeMasterUuid(hostPort), hostPort, inetAddress), credentialsPolicy);
  }

  static String getFakeMasterUuid(HostAndPort hostPort) {
    return "master-" + hostPort.toString();
  }

  void reconnectToCluster(Callback<Void, Boolean> cb,
                          Callback<Void, Exception> eb) {

    final class ReconnectToClusterCB implements Callback<Void, ConnectToClusterResponse> {
      private final Callback<Void, Boolean> cb;

      ReconnectToClusterCB(Callback<Void, Boolean> cb) {
        this.cb = Preconditions.checkNotNull(cb);
      }

      /**
       * Report on the token re-acqusition results. The result authn token might be null: in that
       * case the SASL credentials will be used to negotiate future connections.
       */
      @Override
      public Void call(ConnectToClusterResponse resp) throws Exception {
        final Master.ConnectToMasterResponsePB masterResponsePB = resp.getConnectResponse();
        if (masterResponsePB.hasAuthnToken()) {
          LOG.info("connect to master: received a new authn token");
          securityContext.setAuthenticationToken(masterResponsePB.getAuthnToken());
          cb.call(true);
        } else {
          LOG.warn("connect to master: received no authn token");
          securityContext.setAuthenticationToken(null);
          cb.call(false);
        }
        return null;
      }
    }

    ConnectToCluster.run(masterTable, masterAddresses, null, defaultAdminOperationTimeoutMs,
        Connection.CredentialsPolicy.PRIMARY_CREDENTIALS).addCallbacks(
            new ReconnectToClusterCB(cb), eb);
  }

  /**
   * Updates the last timestamp received from a server. Used for CLIENT_PROPAGATED
   * external consistency.
   *
   * @param lastPropagatedTimestamp the last timestamp received from a server
   */
  public synchronized void updateLastPropagatedTimestamp(long lastPropagatedTimestamp) {
    if (this.lastPropagatedTimestamp == NO_TIMESTAMP ||
        this.lastPropagatedTimestamp < lastPropagatedTimestamp) {
      this.lastPropagatedTimestamp = lastPropagatedTimestamp;
    }
  }

  /**
   * Returns the last timestamp received from a server. Used for CLIENT_PROPAGATED
   * external consistency. Note that the returned timestamp is encoded and cannot be
   * interpreted as a raw timestamp.
   *
   * @return a long indicating the specially-encoded last timestamp received from a server
   */
  public synchronized long getLastPropagatedTimestamp() {
    return lastPropagatedTimestamp;
  }

  /**
   * Returns a synchronous {@link KuduClient} which wraps this asynchronous client.
   * Calling {@link KuduClient#close} on the returned client will close this client.
   * If this asynchronous client should outlive the returned synchronous client,
   * then do not close the synchronous client.
   * @return a new synchronous {@code KuduClient}
   */
  public KuduClient syncClient() {
    return new KuduClient(this);
  }

  /**
   * Create a table on the cluster with the specified name, schema, and table configurations.
   * If the primary key columns of the table schema aren't specified first, the deferred result
   * will be a {@link NonRecoverableException}
   *
   * @param name the table's name
   * @param schema the table's schema
   * @param builder a builder containing the table's configurations
   * @return a deferred object to track the progress of the createTable command that gives
   * an object to communicate with the created table
   */
  public Deferred<KuduTable> createTable(final String name, Schema schema,
                                         CreateTableOptions builder) {
    checkIsClosed();
    if (builder == null) {
      throw new IllegalArgumentException("CreateTableOptions may not be null");
    }
    if (!builder.getBuilder().getPartitionSchema().hasRangeSchema() &&
        builder.getBuilder().getPartitionSchema().getHashBucketSchemasCount() == 0) {
      throw new IllegalArgumentException("Table partitioning must be specified using " +
                                         "setRangePartitionColumns or addHashPartitions");

    }
    CreateTableRequest create = new CreateTableRequest(this.masterTable, name, schema, builder);
    create.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(create).addCallbackDeferring(
        new Callback<Deferred<KuduTable>, CreateTableResponse>() {
          @Override
          public Deferred<KuduTable> call(CreateTableResponse createTableResponse)
              throws Exception {
            return openTable(name);
          }
        });
  }

  /**
   * Delete a table on the cluster with the specified name.
   * @param name the table's name
   * @return a deferred object to track the progress of the deleteTable command
   */
  public Deferred<DeleteTableResponse> deleteTable(String name) {
    checkIsClosed();
    DeleteTableRequest delete = new DeleteTableRequest(this.masterTable, name);
    delete.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(delete);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   *
   * When the returned deferred completes it only indicates that the master accepted the alter
   * command, use {@link AsyncKuduClient#isAlterTableDone(String)} to know when the alter finishes.
   * @param name the table's name, if this is a table rename then the old table name must be passed
   * @param ato the alter table builder
   * @return a deferred object to track the progress of the alter command
   */
  public Deferred<AlterTableResponse> alterTable(String name, AlterTableOptions ato) {
    checkIsClosed();
    AlterTableRequest alter = new AlterTableRequest(this.masterTable, name, ato);
    alter.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    Deferred<AlterTableResponse> response = sendRpcToTablet(alter);

    if (ato.hasAddDropRangePartitions()) {
      // Clear the table locations cache so the new partition is immediately visible.
      return response.addCallback(new Callback<AlterTableResponse, AlterTableResponse>() {
        @Override
        public AlterTableResponse call(AlterTableResponse resp) {
          // If the master is of a recent enough version to return the table ID,
          // we can selectively clear the cache only for the altered table.
          // Otherwise, we clear the caches for all tables.
          if (resp.getTableId() != null) {
            tableLocations.remove(resp.getTableId());
          } else {
            tableLocations.clear();
          }
          return resp;
        }

        @Override
        public String toString() {
          return "ClearTableLocationsCacheCB";
        }
      }).addErrback(new Callback<Exception, Exception>() {
        @Override
        public Exception call(Exception e) {
          // We clear the cache even on failure, just in
          // case the alter table operation actually succeeded.
          tableLocations.clear();
          return e;
        }

        @Override
        public String toString() {
          return "ClearTableLocationsCacheEB";
        }
      });
    }
    return response;
  }

  /**
   * Helper method that checks and waits until the completion of an alter command.
   * It will block until the alter command is done or the deadline is reached.
   * @param name the table's name, if the table was renamed then that name must be checked against
   * @return a deferred object to track the progress of the isAlterTableDone command
   */
  public Deferred<IsAlterTableDoneResponse> isAlterTableDone(String name) {
    checkIsClosed();
    IsAlterTableDoneRequest request = new IsAlterTableDoneRequest(this.masterTable, name);
    request.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(request);
  }

  /**
   * Get the list of running tablet servers.
   * @return a deferred object that yields a list of tablet servers
   */
  public Deferred<ListTabletServersResponse> listTabletServers() {
    checkIsClosed();
    ListTabletServersRequest rpc = new ListTabletServersRequest(this.masterTable);
    rpc.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(rpc);
  }

  private Deferred<GetTableSchemaResponse> getTableSchema(String name) {
    GetTableSchemaRequest rpc = new GetTableSchemaRequest(this.masterTable, name);
    rpc.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(rpc);
  }

  /**
   * Get the list of all the tables.
   * @return a deferred object that yields a list of all the tables
   */
  public Deferred<ListTablesResponse> getTablesList() {
    return getTablesList(null);
  }

  /**
   * Get a list of table names. Passing a null filter returns all the tables. When a filter is
   * specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that yields the list of table names
   */
  public Deferred<ListTablesResponse> getTablesList(String nameFilter) {
    ListTablesRequest rpc = new ListTablesRequest(this.masterTable, nameFilter);
    rpc.setTimeoutMillis(defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(rpc);
  }

  /**
   * Test if a table exists.
   * @param name a non-null table name
   * @return true if the table exists, else false
   */
  public Deferred<Boolean> tableExists(final String name) {
    if (name == null) {
      throw new IllegalArgumentException("The table name cannot be null");
    }
    return getTablesList().addCallbackDeferring(new Callback<Deferred<Boolean>,
        ListTablesResponse>() {
      @Override
      public Deferred<Boolean> call(ListTablesResponse listTablesResponse) throws Exception {
        for (String tableName : listTablesResponse.getTablesList()) {
          if (name.equals(tableName)) {
            return Deferred.fromResult(true);
          }
        }
        return Deferred.fromResult(false);
      }
    });
  }

  /**
   * Open the table with the given name. If the table was just created, the
   * Deferred will only get called back when all the tablets have been
   * successfully created.
   *
   * New range partitions created by other clients will immediately be available
   * after opening the table.
   *
   * @param name table to open
   * @return a KuduTable if the table exists, else a MasterErrorException
   */
  public Deferred<KuduTable> openTable(final String name) {
    checkIsClosed();

    // We create an RPC that we're never going to send, and will instead use it to keep track of
    // timeouts and use its Deferred.
    final KuduRpc<KuduTable> fakeRpc = new KuduRpc<KuduTable>(null) {
      @Override
      Message createRequestPB() {
        return null;
      }

      @Override
      String serviceName() {
        return null;
      }

      @Override
      String method() {
        return "IsCreateTableDone";
      }

      @Override
      Pair<KuduTable, Object> deserialize(CallResponse callResponse, String tsUUID)
          throws KuduException {
        return null;
      }
    };
    fakeRpc.setTimeoutMillis(defaultAdminOperationTimeoutMs);

    return getTableSchema(name).addCallbackDeferring(new Callback<Deferred<KuduTable>,
        GetTableSchemaResponse>() {
      @Override
      public Deferred<KuduTable> call(GetTableSchemaResponse response) throws Exception {
        KuduTable table = new KuduTable(AsyncKuduClient.this,
            name,
            response.getTableId(),
            response.getSchema(),
            response.getPartitionSchema());
        // We grab the Deferred first because calling callback on the RPC will reset it and we'd
        // return a different, non-triggered Deferred.
        Deferred<KuduTable> d = fakeRpc.getDeferred();
        if (response.isCreateTableDone()) {
          // When opening a table, clear the existing cached non-covered range entries.
          // This avoids surprises where a new table instance won't be able to see the
          // current range partitions of a table for up to the ttl.
          TableLocationsCache cache = tableLocations.get(response.getTableId());
          if (cache != null) {
            cache.clearNonCoveredRangeEntries();
          }

          LOG.debug("Opened table {}", name);
          fakeRpc.callback(table);
        } else {
          LOG.debug("Delaying opening table {}, its tablets aren't fully created", name);
          fakeRpc.attempt++;
          delayedIsCreateTableDone(
              table,
              fakeRpc,
              getOpenTableCB(fakeRpc, table),
              getDelayedIsCreateTableDoneErrback(fakeRpc));
        }
        return d;
      }
    });
  }

  /**
   * This callback will be repeatedly used when opening a table until it is done being created.
   */
  private Callback<Deferred<KuduTable>, Master.IsCreateTableDoneResponsePB> getOpenTableCB(
      final KuduRpc<KuduTable> rpc, final KuduTable table) {
    return new Callback<Deferred<KuduTable>, Master.IsCreateTableDoneResponsePB>() {
      @Override
      public Deferred<KuduTable> call(
          Master.IsCreateTableDoneResponsePB isCreateTableDoneResponsePB) throws Exception {
        String tableName = table.getName();
        Deferred<KuduTable> d = rpc.getDeferred();
        if (isCreateTableDoneResponsePB.getDone()) {
          // When opening a table, clear the existing cached non-covered range entries.
          TableLocationsCache cache = tableLocations.get(table.getTableId());
          if (cache != null) {
            cache.clearNonCoveredRangeEntries();
          }
          LOG.debug("Table {}'s tablets are now created", tableName);
          rpc.callback(table);
        } else {
          rpc.attempt++;
          LOG.debug("Table {}'s tablets are still not created, further delaying opening it",
              tableName);

          delayedIsCreateTableDone(
              table,
              rpc,
              getOpenTableCB(rpc, table),
              getDelayedIsCreateTableDoneErrback(rpc));
        }
        return d;
      }
    };
  }

  /**
   * Export serialized authentication data that may be passed to a different
   * client instance and imported to provide that client the ability to connect
   * to the cluster.
   */
  @InterfaceStability.Unstable
  public Deferred<byte[]> exportAuthenticationCredentials() {
    byte[] authnData = securityContext.exportAuthenticationCredentials();
    if (authnData != null) {
      return Deferred.fromResult(authnData);
    }
    // We have no authn data -- connect to the master, which will fetch
    // new info.
    return getMasterTableLocationsPB(null)
        .addCallback(new MasterLookupCB(masterTable, null, 1))
        .addCallback(new Callback<byte[], Object>() {
          @Override
          public byte[] call(Object ignored) {
            // Connecting to the cluster should have also fetched the
            // authn data.
            return securityContext.exportAuthenticationCredentials();
          }
        });
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
    securityContext.importAuthenticationCredentials(authnData);
  }

  /**
   * Get the timeout used for operations on sessions and scanners.
   * @return a timeout in milliseconds
   */
  public long getDefaultOperationTimeoutMs() {
    return defaultOperationTimeoutMs;
  }

  /**
   * Get the timeout used for admin operations.
   * @return a timeout in milliseconds
   */
  public long getDefaultAdminOperationTimeoutMs() {
    return defaultAdminOperationTimeoutMs;
  }

  /**
   * Get the timeout used when waiting to read data from a socket. Will be triggered when nothing
   * has been read on a socket connected to a tablet server for {@code timeout} milliseconds.
   * @return a timeout in milliseconds
   */
  public long getDefaultSocketReadTimeoutMs() {
    return defaultSocketReadTimeoutMs;
  }

  /**
   * @return the list of master addresses, stringified using commas to separate
   * them
   */
  public String getMasterAddressesAsString() {
    return Joiner.on(",").join(masterAddresses);
  }

  /**
   * Check if statistics collection is enabled for this client.
   * @return true if it is enabled, else false
   */
  public boolean isStatisticsEnabled() {
    return !statisticsDisabled;
  }

  /**
   * Get the statistics object of this client.
   *
   * @return this client's Statistics object
   * @throws IllegalStateException thrown if statistics collection has been disabled
   */
  public Statistics getStatistics() {
    if (statisticsDisabled) {
      throw new IllegalStateException("This client's statistics is disabled");
    }
    return this.statistics;
  }

  RequestTracker getRequestTracker() {
    return requestTracker;
  }

  /**
   * Creates a new {@link AsyncKuduScanner.AsyncKuduScannerBuilder} for a particular table.
   * @param table the name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return a new scanner builder for this table
   */
  public AsyncKuduScanner.AsyncKuduScannerBuilder newScannerBuilder(KuduTable table) {
    checkIsClosed();
    return new AsyncKuduScanner.AsyncKuduScannerBuilder(this, table);
  }

  /**
   * Create a new session for interacting with the cluster.
   * User is responsible for destroying the session object.
   * This is a fully local operation (no RPCs or blocking).
   * @return a new AsyncKuduSession
   */
  public AsyncKuduSession newSession() {
    checkIsClosed();
    AsyncKuduSession session = new AsyncKuduSession(this);
    synchronized (sessions) {
      sessions.add(session);
    }
    return session;
  }

  /**
   * This method is for KuduSessions so that they can remove themselves as part of closing down.
   * @param session Session to remove
   */
  void removeSession(AsyncKuduSession session) {
    synchronized (sessions) {
      boolean removed = sessions.remove(session);
      assert removed;
    }
  }

  /**
   * Package-private access point for {@link AsyncKuduScanner}s to scan more rows.
   * @param scanner The scanner to use.
   * @return A deferred row.
   */
  Deferred<AsyncKuduScanner.Response> scanNextRows(final AsyncKuduScanner scanner) {
    RemoteTablet tablet = Preconditions.checkNotNull(scanner.currentTablet());
    KuduRpc<AsyncKuduScanner.Response> nextRequest = scanner.getNextRowsRequest();
    // Important to increment the attempts before the next if statement since
    // getSleepTimeForRpc() relies on it if the client is null or dead.
    nextRequest.attempt++;
    final ServerInfo info = tablet.getReplicaSelectedServerInfo(nextRequest.getReplicaSelection());
    if (info == null) {
      return delayedSendRpcToTablet(nextRequest, new RecoverableException(Status.RemoteError(
          String.format("No information on servers hosting tablet %s, will retry later",
              tablet.getTabletId()))));
    }

    Deferred<AsyncKuduScanner.Response> d = nextRequest.getDeferred();
    RpcProxy.sendRpc(this, connectionCache.getConnection(
        info, Connection.CredentialsPolicy.ANY_CREDENTIALS), nextRequest);
    return d;
  }

  /**
   * Package-private access point for {@link AsyncKuduScanner}s to close themselves.
   * @param scanner the scanner to close
   * @return a deferred object that indicates the completion of the request.
   * The {@link AsyncKuduScanner.Response} can contain rows that were left to scan.
   */
  Deferred<AsyncKuduScanner.Response> closeScanner(final AsyncKuduScanner scanner) {
    final RemoteTablet tablet = scanner.currentTablet();
    // Getting a null tablet here without being in a closed state means we were in between tablets.
    if (tablet == null) {
      return Deferred.fromResult(null);
    }
    final KuduRpc<AsyncKuduScanner.Response> closeRequest = scanner.getCloseRequest();
    final ServerInfo info = tablet.getReplicaSelectedServerInfo(closeRequest.getReplicaSelection());
    if (info == null) {
      return Deferred.fromResult(null);
    }

    final Deferred<AsyncKuduScanner.Response> d = closeRequest.getDeferred();
    closeRequest.attempt++;
    RpcProxy.sendRpc(this, connectionCache.getConnection(
        info, Connection.CredentialsPolicy.ANY_CREDENTIALS), closeRequest);
    return d;
  }

  /**
   * Sends the provided {@link KuduRpc} to the tablet server hosting the leader
   * of the tablet identified by the RPC's table and partition key.
   *
   * Note: despite the name, this method is also used for routing master
   * requests to the leader master instance since it's also handled like a tablet.
   *
   * @param request the RPC to send
   * @param <R> the expected return type of the RPC
   * @return a {@code Deferred} which will contain the response
   */
  <R> Deferred<R> sendRpcToTablet(final KuduRpc<R> request) {
    if (cannotRetryRequest(request)) {
      return tooManyAttemptsOrTimeout(request, null);
    }
    request.attempt++;
    final String tableId = request.getTable().getTableId();
    byte[] partitionKey = request.partitionKey();
    TableLocationsCache.Entry entry = getTableLocationEntry(tableId, partitionKey);

    if (entry != null && entry.isNonCoveredRange()) {
      Exception e = new NonCoveredRangeException(entry.getLowerBoundPartitionKey(),
                                                 entry.getUpperBoundPartitionKey());
      // Sending both as an errback and returning fromError because sendRpcToTablet might be
      // called via a callback that won't care about the returned Deferred.
      Deferred<R> d = request.getDeferred();
      request.errback(e);
      return d;
    }

    // Set the propagated timestamp so that the next time we send a message to
    // the server the message includes the last propagated timestamp.
    long lastPropagatedTs = getLastPropagatedTimestamp();
    if (request.getExternalConsistencyMode() == CLIENT_PROPAGATED &&
        lastPropagatedTs != NO_TIMESTAMP) {
      request.setPropagatedTimestamp(lastPropagatedTs);
    }

    // If we found a tablet, we'll try to find the TS to talk to.
    if (entry != null) {
      RemoteTablet tablet = entry.getTablet();
      ServerInfo info = tablet.getReplicaSelectedServerInfo(request.getReplicaSelection());
      if (info != null) {
        Deferred<R> d = request.getDeferred();
        request.setTablet(tablet);
        RpcProxy.sendRpc(this, connectionCache.getConnection(
            info, Connection.CredentialsPolicy.ANY_CREDENTIALS), request);
        return d;
      }
    }

    request.addTrace(
        new RpcTraceFrame.RpcTraceFrameBuilder(
            request.method(),
            RpcTraceFrame.Action.QUERY_MASTER)
            .build());

    // We fall through to here in two cases:
    //
    // 1) This client has not yet discovered the tablet which is responsible for
    //    the RPC's table and partition key. This can happen when the client's
    //    tablet location cache is cold because the client is new, or the table
    //    is new.
    //
    // 2) The tablet is known, but we do not have an active client for the
    //    leader replica.
    if (tablesNotServed.contains(tableId)) {
      return delayedIsCreateTableDone(request.getTable(), request,
          new RetryRpcCB<R, Master.IsCreateTableDoneResponsePB>(request),
          getDelayedIsCreateTableDoneErrback(request));
    }
    Callback<Deferred<R>, Master.GetTableLocationsResponsePB> cb = new RetryRpcCB<>(request);
    Callback<Deferred<R>, Exception> eb = new RetryRpcErrback<>(request);
    Deferred<Master.GetTableLocationsResponsePB> returnedD =
        locateTablet(request.getTable(), partitionKey, FETCH_TABLETS_PER_POINT_LOOKUP, request);
    return AsyncUtil.addCallbacksDeferring(returnedD, cb, eb);
  }

  /**
   * Callback used to retry a RPC after another query finished, like looking up where that RPC
   * should go.
   * <p>
   * Use {@code AsyncUtil.addCallbacksDeferring} to add this as the callback and
   * {@link AsyncKuduClient.RetryRpcErrback} as the "errback" to the {@code Deferred}
   * returned by {@link #locateTablet(KuduTable, byte[], int, KuduRpc)}.
   * @param <R> RPC's return type.
   * @param <D> Previous query's return type, which we don't use, but need to specify in order to
   *           tie it all together.
   */
  final class RetryRpcCB<R, D> implements Callback<Deferred<R>, D> {
    private final KuduRpc<R> request;

    RetryRpcCB(KuduRpc<R> request) {
      this.request = request;
    }

    public Deferred<R> call(final D arg) {
      LOG.debug("Retrying sending RPC {} after lookup", request);
      return sendRpcToTablet(request);  // Retry the RPC.
    }

    public String toString() {
      return "retry RPC";
    }
  }

  /**
   * "Errback" used to delayed-retry a RPC if a recoverable exception is thrown in the callback
   * chain.
   * Other exceptions are used to notify request RPC error, and passed through to be handled
   * by the caller.
   * <p>
   * Use {@code AsyncUtil.addCallbacksDeferring} to add this as the "errback" and
   * {@link RetryRpcCB} as the callback to the {@code Deferred} returned by
   * {@link #locateTablet(KuduTable, byte[], int, KuduRpc)}.
   * @see #delayedSendRpcToTablet(KuduRpc, KuduException)
   * @param <R> The type of the original RPC.
   */
  final class RetryRpcErrback<R> implements Callback<Deferred<R>, Exception> {
    private final KuduRpc<R> request;

    public RetryRpcErrback(KuduRpc<R> request) {
      this.request = request;
    }

    @Override
    public Deferred<R> call(Exception arg) {
      if (arg instanceof RecoverableException) {
        return delayedSendRpcToTablet(request, (KuduException) arg);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Notify RPC %s after lookup exception", request), arg);
      }
      request.errback(arg);
      return Deferred.fromError(arg);
    }

    @Override
    public String toString() {
      return "retry RPC after error";
    }
  }

  /**
   * This errback ensures that if the delayed call to IsCreateTableDone throws an Exception that
   * it will be propagated back to the user.
   * @param request Request to errback if there's a problem with the delayed call.
   * @param <R> Request's return type.
   * @return An errback.
   */
  private <R> Callback<Exception, Exception> getDelayedIsCreateTableDoneErrback(
      final KuduRpc<R> request) {
    return new Callback<Exception, Exception>() {
      @Override
      public Exception call(Exception e) throws Exception {
        // TODO maybe we can retry it?
        request.errback(e);
        return e;
      }
    };
  }

  /**
   * This method will call IsCreateTableDone on the master after sleeping for
   * getSleepTimeForRpc() based on the provided KuduRpc's number of attempts. Once this is done,
   * the provided callback will be called.
   * @param table the table to lookup
   * @param rpc the original KuduRpc that needs to access the table
   * @param retryCB the callback to call on completion
   * @param errback the errback to call if something goes wrong when calling IsCreateTableDone
   * @return Deferred used to track the provided KuduRpc
   */
  private <R> Deferred<R> delayedIsCreateTableDone(
      final KuduTable table,
      final KuduRpc<R> rpc,
      final Callback<Deferred<R>,
      Master.IsCreateTableDoneResponsePB> retryCB,
      final Callback<Exception, Exception> errback) {

    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        String tableId = table.getTableId();
        final boolean has_permit = acquireMasterLookupPermit();
        if (!has_permit && !tablesNotServed.contains(tableId)) {
          // If we failed to acquire a permit, it's worth checking if someone
          // looked up the tablet we're interested in.  Every once in a while
          // this will save us a Master lookup.
          try {
            retryCB.call(null);
            return;
          } catch (Exception e) {
            // we're calling RetryRpcCB which doesn't throw exceptions, ignore
          }
        }
        IsCreateTableDoneRequest isCreateTableDoneRequest =
            new IsCreateTableDoneRequest(masterTable, tableId);
        isCreateTableDoneRequest.setTimeoutMillis(defaultAdminOperationTimeoutMs);
        isCreateTableDoneRequest.setParentRpc(rpc);
        final Deferred<Master.IsCreateTableDoneResponsePB> d =
            sendRpcToTablet(isCreateTableDoneRequest).addCallback(new IsCreateTableDoneCB(tableId));
        if (has_permit) {
          // The errback is needed here to release the lookup permit
          d.addCallbacks(new ReleaseMasterLookupPermit<Master.IsCreateTableDoneResponsePB>(),
              new ReleaseMasterLookupPermit<Exception>());
        }
        d.addCallbacks(retryCB, errback);
      }
    }

    long sleepTime = getSleepTimeForRpc(rpc);
    if (rpc.deadlineTracker.wouldSleepingTimeout(sleepTime)) {
      return tooManyAttemptsOrTimeout(rpc, null);
    }

    newTimeout(new RetryTimer(), sleepTime);
    return rpc.getDeferred();
  }

  private final class ReleaseMasterLookupPermit<T> implements Callback<T, T> {
    public T call(final T arg) {
      releaseMasterLookupPermit();
      return arg;
    }

    public String toString() {
      return "release master lookup permit";
    }
  }

  /** Callback executed when IsCreateTableDone completes.  */
  private final class IsCreateTableDoneCB implements Callback<Master.IsCreateTableDoneResponsePB,
      Master.IsCreateTableDoneResponsePB> {
    final String tableName;

    IsCreateTableDoneCB(String tableName) {
      this.tableName = tableName;

    }

    public Master.IsCreateTableDoneResponsePB
        call(final Master.IsCreateTableDoneResponsePB response) {
      if (response.getDone()) {
        LOG.debug("Table {} was created", tableName);
        tablesNotServed.remove(tableName);
      } else {
        LOG.debug("Table {} is still being created", tableName);
      }
      return response;
    }

    public String toString() {
      return "ask the master if " + tableName + " was created";
    }
  }

  private long getSleepTimeForRpc(KuduRpc<?> rpc) {
    int attemptCount = rpc.attempt;
    assert (attemptCount > 0);
    if (attemptCount == 0) {
      LOG.warn("Possible bug: attempting to retry an RPC with no attempts. RPC: {}", rpc,
          new Exception("Exception created to collect stack trace"));
      attemptCount = 1;
    }
    // Randomized exponential backoff, truncated at 4096ms.
    long sleepTime = (long)(Math.pow(2.0, Math.min(attemptCount, 12)) *
        sleepRandomizer.nextDouble());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Going to sleep for {} at retry {}", sleepTime, rpc.attempt);
    }
    return sleepTime;
  }

  /**
   * Clears {@link #tableLocations} of the table's entries.
   *
   * This method makes the maps momentarily inconsistent, and should only be
   * used when the {@code AsyncKuduClient} is in a steady state.
   * @param tableId table for which we remove all cached tablet location and
   *                tablet client entries
   */
  @VisibleForTesting
  void emptyTabletsCacheForTable(String tableId) {
    tableLocations.remove(tableId);
  }

  /**
   * Checks whether or not an RPC can be retried once more
   * @param rpc The RPC we're going to attempt to execute
   * @return {@code true} if this RPC already had too many attempts,
   * {@code false} otherwise (in which case it's OK to retry once more)
   */
  private static boolean cannotRetryRequest(final KuduRpc<?> rpc) {
    return rpc.deadlineTracker.timedOut() || rpc.attempt > MAX_RPC_ATTEMPTS;
  }

  /**
   * Returns a {@link Deferred} containing an exception when an RPC couldn't
   * succeed after too many attempts or if it already timed out.
   * @param request The RPC that was retried too many times or timed out.
   * @param cause What was cause of the last failed attempt, if known.
   * You can pass {@code null} if the cause is unknown.
   */
  private static <R> Deferred<R> tooManyAttemptsOrTimeout(final KuduRpc<R> request,
                                                          final KuduException cause) {
    String message;
    if (request.attempt > MAX_RPC_ATTEMPTS) {
      message = "too many attempts: ";
    } else {
      message = "can not complete before timeout: ";
    }
    Status statusTimedOut = Status.TimedOut(message + request);
    LOG.debug("Cannot continue with RPC because of: {}", statusTimedOut);
    Deferred<R> d = request.getDeferred();
    request.errback(new NonRecoverableException(statusTimedOut, cause));
    return d;
  }

  /**
   * Sends a getTableLocations RPC to the master to find the table's tablets.
   * @param table table to lookup
   * @param partitionKey can be null, if not we'll find the exact tablet that contains it
   * @param fetchBatchSize the number of tablets to fetch per round trip from the master
   * @param parentRpc RPC that prompted a master lookup, can be null
   * @return Deferred to track the progress
   */
  private Deferred<Master.GetTableLocationsResponsePB> locateTablet(KuduTable table,
                                                                    byte[] partitionKey,
                                                                    int fetchBatchSize,
                                                                    KuduRpc<?> parentRpc) {
    boolean hasPermit = acquireMasterLookupPermit();
    String tableId = table.getTableId();
    if (!hasPermit) {
      // If we failed to acquire a permit, it's worth checking if someone
      // looked up the tablet we're interested in.  Every once in a while
      // this will save us a Master lookup.
      TableLocationsCache.Entry entry = getTableLocationEntry(tableId, partitionKey);
      if (entry != null && !entry.isNonCoveredRange() &&
          entry.getTablet().getLeaderServerInfo() != null) {
        return Deferred.fromResult(null);  // Looks like no lookup needed.
      }
    }

    // If we know this is going to the master, check the master consensus
    // configuration (as specified by 'masterAddresses' field) to determine and
    // cache the current leader.
    Deferred<Master.GetTableLocationsResponsePB> d;
    if (isMasterTable(tableId)) {
      d = getMasterTableLocationsPB(parentRpc);
    } else {
      // Leave the end of the partition key range empty in order to pre-fetch tablet locations.
      GetTableLocationsRequest rpc =
          new GetTableLocationsRequest(masterTable, partitionKey, null, tableId, fetchBatchSize);
      if (parentRpc != null) {
        rpc.setTimeoutMillis(parentRpc.deadlineTracker.getMillisBeforeDeadline());
        rpc.setParentRpc(parentRpc);
      } else {
        rpc.setTimeoutMillis(defaultAdminOperationTimeoutMs);
      }
      d = sendRpcToTablet(rpc);
    }
    d.addCallback(new MasterLookupCB(table, partitionKey, fetchBatchSize));
    if (hasPermit) {
      d.addBoth(new ReleaseMasterLookupPermit<Master.GetTableLocationsResponsePB>());
    }
    return d;
  }

  /**
   * Update the master config: send RPCs to all config members, use the returned data to
   * fill a {@link Master.GetTabletLocationsResponsePB} object.
   * @return An initialized Deferred object to hold the response.
   */
  Deferred<Master.GetTableLocationsResponsePB> getMasterTableLocationsPB(KuduRpc<?> parentRpc) {
    // TODO(todd): stop using this 'masterTable' hack.
    return ConnectToCluster.run(masterTable, masterAddresses, parentRpc,
        defaultAdminOperationTimeoutMs, Connection.CredentialsPolicy.ANY_CREDENTIALS).addCallback(
            new Callback<Master.GetTableLocationsResponsePB, ConnectToClusterResponse>() {
              @Override
              public Master.GetTableLocationsResponsePB call(ConnectToClusterResponse resp) {
                if (resp.getConnectResponse().hasAuthnToken()) {
                  // If the response has security info, adopt it.
                  securityContext.setAuthenticationToken(resp.getConnectResponse().getAuthnToken());
                }
                List<ByteString> caCerts = resp.getConnectResponse().getCaCertDerList();
                if (!caCerts.isEmpty()) {
                  try {
                    securityContext.trustCertificates(caCerts);
                  } catch (CertificateException e) {
                    LOG.warn("Ignoring invalid CA cert from leader {}: {}",
                        resp.getLeaderHostAndPort(),
                        e.getMessage());
                  }
                }

                // Translate the located master into a TableLocations
                // since the rest of our locations caching code expects this type.
                return resp.getAsTableLocations();
              }
            });
  }

  /**
   * Get all or some tablets for a given table. This may query the master multiple times if there
   * are a lot of tablets.
   * This method blocks until it gets all the tablets.
   * @param table the table to locate tablets from
   * @param startPartitionKey where to start in the table, pass null to start at the beginning
   * @param endPartitionKey where to stop in the table, pass null to get all the tablets until the
   *                        end of the table
   * @param fetchBatchSize the number of tablets to fetch per round trip from the master
   * @param deadline deadline in milliseconds for this method to finish
   * @return a list of the tablets in the table, which can be queried for metadata about
   *         each tablet
   * @throws Exception MasterErrorException if the table doesn't exist
   */
  List<LocatedTablet> syncLocateTable(KuduTable table,
                                      byte[] startPartitionKey,
                                      byte[] endPartitionKey,
                                      int fetchBatchSize,
                                      long deadline) throws Exception {
    return locateTable(table, startPartitionKey, endPartitionKey, fetchBatchSize, deadline).join();
  }

  private Deferred<List<LocatedTablet>> loopLocateTable(final KuduTable table,
                                                        final byte[] startPartitionKey,
                                                        final byte[] endPartitionKey,
                                                        final int fetchBatchSize,
                                                        final List<LocatedTablet> ret,
                                                        final DeadlineTracker deadlineTracker) {
    // We rely on the keys initially not being empty.
    Preconditions.checkArgument(startPartitionKey == null || startPartitionKey.length > 0,
                                "use null for unbounded start partition key");
    Preconditions.checkArgument(endPartitionKey == null || endPartitionKey.length > 0,
                                "use null for unbounded end partition key");

    // The next partition key to look up. If null, then it represents
    // the minimum partition key, If empty, it represents the maximum key.
    byte[] partitionKey = startPartitionKey;
    String tableId = table.getTableId();

    // Continue while the partition key is the minimum, or it is not the maximum
    // and it is less than the end partition key.
    while (partitionKey == null ||
           (partitionKey.length > 0 &&
            (endPartitionKey == null || Bytes.memcmp(partitionKey, endPartitionKey) < 0))) {
      byte[] key = partitionKey == null ? EMPTY_ARRAY : partitionKey;
      TableLocationsCache.Entry entry = getTableLocationEntry(tableId, key);

      if (entry != null) {
        if (!entry.isNonCoveredRange()) {
          ret.add(new LocatedTablet(entry.getTablet()));
        }
        partitionKey = entry.getUpperBoundPartitionKey();
        continue;
      }

      if (deadlineTracker.timedOut()) {
        Status statusTimedOut = Status.TimedOut("Took too long getting the list of tablets, " +
            deadlineTracker);
        return Deferred.fromError(new NonRecoverableException(statusTimedOut));
      }

      // If the partition key location isn't cached, and the request hasn't timed out,
      // then kick off a new tablet location lookup and try again when it completes.
      // When lookup completes, the tablet (or non-covered range) for the next
      // partition key will be located and added to the client's cache.
      final byte[] lookupKey = partitionKey;
      return locateTablet(table, key, fetchBatchSize, null).addCallbackDeferring(
          new Callback<Deferred<List<LocatedTablet>>, GetTableLocationsResponsePB>() {
            @Override
            public Deferred<List<LocatedTablet>> call(GetTableLocationsResponsePB resp) {
              return loopLocateTable(table, lookupKey, endPartitionKey, fetchBatchSize,
                                     ret, deadlineTracker);
            }

            @Override
            public String toString() {
              return "LoopLocateTableCB";
            }
          });
    }

    return Deferred.fromResult(ret);
  }

  /**
   * Get all or some tablets for a given table. This may query the master multiple times if there
   * are a lot of tablets.
   * @param table the table to locate tablets from
   * @param startPartitionKey where to start in the table, pass null to start at the beginning
   * @param endPartitionKey where to stop in the table, pass null to get all the tablets until the
   *                        end of the table
   * @param fetchBatchSize the number of tablets to fetch per round trip from the master
   * @param deadline max time spent in milliseconds for the deferred result of this method to
   *         get called back, if deadline is reached, the deferred result will get erred back
   * @return a deferred object that yields a list of the tablets in the table, which can be queried
   *         for metadata about each tablet
   */
  Deferred<List<LocatedTablet>> locateTable(final KuduTable table,
                                            final byte[] startPartitionKey,
                                            final byte[] endPartitionKey,
                                            int fetchBatchSize,
                                            long deadline) {
    final List<LocatedTablet> ret = Lists.newArrayList();
    final DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(deadline);
    return loopLocateTable(table, startPartitionKey, endPartitionKey, fetchBatchSize,
                           ret, deadlineTracker);
  }

  /**
   * We're handling a tablet server that's telling us it doesn't have the tablet we're asking for.
   * We're in the context of decode() meaning we need to either callback or retry later.
   */
  <R> void handleTabletNotFound(final KuduRpc<R> rpc, KuduException ex, ServerInfo info) {
    invalidateTabletCache(rpc.getTablet(), info);
    handleRetryableError(rpc, ex);
  }

  /**
   * A tablet server is letting us know that it isn't the specified tablet's leader in response
   * a RPC, so we need to demote it and retry.
   */
  <R> void handleNotLeader(final KuduRpc<R> rpc, KuduException ex, ServerInfo info) {
    rpc.getTablet().demoteLeader(info.getUuid());
    handleRetryableError(rpc, ex);
  }

  <R> void handleRetryableError(final KuduRpc<R> rpc, KuduException ex) {
    // TODO we don't always need to sleep, maybe another replica can serve this RPC.
    // We don't care about the returned Deferred in this case, since we're not in a context where
    // we're eventually returning a Deferred.
    delayedSendRpcToTablet(rpc, ex);
  }

  /**
   * Same as {@link #handleRetryableError(KuduRpc, KuduException)}, but without the delay before
   * retrying the RPC.
   *
   * @param rpc the RPC to retry
   * @param ex the exception which lead to the attempt of RPC retry
   */
  <R> void handleRetryableErrorNoDelay(final KuduRpc<R> rpc, KuduException ex) {
    if (cannotRetryRequest(rpc)) {
      tooManyAttemptsOrTimeout(rpc, ex);
      return;
    }
    sendRpcToTablet(rpc);
  }

  /**
   * Handle a RPC failed due to invalid authn token error. In short, connect to the Kudu cluster
   * to acquire a new authentication token and retry the RPC once a new authentication token
   * is put into the {@link #securityContext}.
   *
   * @param rpc the RPC which failed do to invalid authn token
   */
  <R> void handleInvalidToken(KuduRpc<R> rpc) {
    tokenReacquirer.handleAuthnTokenExpiration(rpc);
  }

  /**
   * This methods enable putting RPCs on hold for a period of time determined by
   * {@link #getSleepTimeForRpc(KuduRpc)}. If the RPC is out of time/retries, its errback will
   * be immediately called.
   * @param rpc the RPC to retry later
   * @param ex the reason why we need to retry
   * @return a Deferred object to use if this method is called inline with the user's original
   * attempt to send the RPC. Can be ignored in any other context that doesn't need to return a
   * Deferred back to the user.
   */
  private <R> Deferred<R> delayedSendRpcToTablet(final KuduRpc<R> rpc, KuduException ex) {
    // Here we simply retry the RPC later. We might be doing this along with a lot of other RPCs
    // in parallel. Asynchbase does some hacking with a "probe" RPC while putting the other ones
    // on hold but we won't be doing this for the moment. Regions in HBase can move a lot,
    // we're not expecting this in Kudu.
    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        sendRpcToTablet(rpc);
      }
    }

    assert (ex != null);
    Status reasonForRetry = ex.getStatus();
    rpc.addTrace(
        new RpcTraceFrame.RpcTraceFrameBuilder(
            rpc.method(),
            RpcTraceFrame.Action.SLEEP_THEN_RETRY)
            .callStatus(reasonForRetry)
            .build());

    long sleepTime = getSleepTimeForRpc(rpc);
    if (cannotRetryRequest(rpc) || rpc.deadlineTracker.wouldSleepingTimeout(sleepTime)) {
      // Don't let it retry.
      return tooManyAttemptsOrTimeout(rpc, ex);
    }
    newTimeout(new RetryTimer(), sleepTime);
    return rpc.getDeferred();
  }

  /**
   * Remove the tablet server from the RemoteTablet's locations. Right now nothing is removing
   * the tablet itself from the caches.
   */
  private void invalidateTabletCache(RemoteTablet tablet, ServerInfo info) {
    final String uuid = info.getUuid();
    LOG.info("Removing server {} from this tablet's cache {}", uuid, tablet.getTabletId());
    tablet.removeTabletClient(uuid);
  }

  /**
   * Translate master-provided information {@link Master.TSInfoPB} on a tablet server into internal
   * {@link ServerInfo} representation.
   *
   * @param tsInfoPB master-provided information for the tablet server
   * @return an object that contains all the server's information
   * @throws UnknownHostException if we cannot resolve the tablet server's IP address
   */
  private ServerInfo resolveTS(Master.TSInfoPB tsInfoPB) throws UnknownHostException {
    final List<Common.HostPortPB> addresses = tsInfoPB.getRpcAddressesList();
    final String uuid = tsInfoPB.getPermanentUuid().toStringUtf8();
    if (addresses.isEmpty()) {
      LOG.warn("Received a tablet server with no addresses, UUID: {}", uuid);
      return null;
    }

    // from meta_cache.cc
    // TODO: if the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    final HostAndPort hostPort = ProtobufHelper.hostAndPortFromPB(addresses.get(0));
    final InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
    if (inetAddress == null) {
      throw new UnknownHostException(
          "Failed to resolve the IP of `" + addresses.get(0).getHost() + "'");
    }
    return new ServerInfo(uuid, hostPort, inetAddress);
  }

  /** Callback executed when a master lookup completes.  */
  private final class MasterLookupCB implements Callback<Object,
      Master.GetTableLocationsResponsePB> {
    final KuduTable table;
    private final byte[] partitionKey;
    private final int requestedBatchSize;

    MasterLookupCB(KuduTable table, byte[] partitionKey, int requestedBatchSize) {
      this.table = table;
      this.partitionKey = partitionKey;
      this.requestedBatchSize = requestedBatchSize;
    }

    public Object call(final GetTableLocationsResponsePB response) {
      if (response.hasError()) {
        if (response.getError().getCode() == Master.MasterErrorPB.Code.TABLET_NOT_RUNNING) {
          // Keep a note that the table exists but at least one tablet is not yet running.
          LOG.debug("Table {} has a non-running tablet", table.getName());
          tablesNotServed.add(table.getTableId());
        } else {
          Status status = Status.fromMasterErrorPB(response.getError());
          return new NonRecoverableException(status);
        }
      } else {
        try {
          discoverTablets(table,
                          partitionKey,
                          requestedBatchSize,
                          response.getTabletLocationsList(),
                          response.getTtlMillis());
        } catch (KuduException e) {
          return e;
        }
      }
      return null;
    }

    public String toString() {
      return "get tablet locations from the master for table " + table.getName();
    }
  }

  private boolean acquireMasterLookupPermit() {
    try {
      // With such a low timeout, the JVM may chose to spin-wait instead of
      // de-scheduling the thread (and causing context switches and whatnot).
      return masterLookups.tryAcquire(5, MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();  // Make this someone else's problem.
      return false;
    }
  }

  /**
   * Releases a master lookup permit that was acquired.
   * @see #acquireMasterLookupPermit
   */
  private void releaseMasterLookupPermit() {
    masterLookups.release();
  }

  /**
   * Makes discovered tablet locations visible in the clients caches.
   * @param table the table which the locations belong to
   * @param requestPartitionKey the partition key of the table locations request
   * @param requestedBatchSize the number of tablet locations requested from the master in the
   *                           original request
   * @param locations the discovered locations
   * @param ttl the ttl of the locations
   */
  @VisibleForTesting
  void discoverTablets(KuduTable table,
                       byte[] requestPartitionKey,
                       int requestedBatchSize,
                       List<Master.TabletLocationsPB> locations,
                       long ttl) throws KuduException {
    String tableId = table.getTableId();
    String tableName = table.getName();

    // Doing a get first instead of putIfAbsent to avoid creating unnecessary
    // table locations caches because in the most common case the table should
    // already be present.
    TableLocationsCache locationsCache = tableLocations.get(tableId);
    if (locationsCache == null) {
      locationsCache = new TableLocationsCache();
      TableLocationsCache existingLocationsCache =
          tableLocations.putIfAbsent(tableId, locationsCache);
      if (existingLocationsCache != null) {
        locationsCache = existingLocationsCache;
      }
    }

    // Build the list of discovered remote tablet instances. If we have
    // already discovered the tablet, its locations are refreshed.
    List<RemoteTablet> tablets = new ArrayList<>(locations.size());
    for (Master.TabletLocationsPB tabletPb : locations) {

      List<UnknownHostException> lookupExceptions = new ArrayList<>(tabletPb.getReplicasCount());
      List<ServerInfo> servers = new ArrayList<>(tabletPb.getReplicasCount());
      for (Master.TabletLocationsPB.ReplicaPB replica : tabletPb.getReplicasList()) {
        try {
          ServerInfo serverInfo = resolveTS(replica.getTsInfo());
          if (serverInfo != null) {
            servers.add(serverInfo);
          }
        } catch (UnknownHostException ex) {
          lookupExceptions.add(ex);
        }
      }

      if (!lookupExceptions.isEmpty() &&
          lookupExceptions.size() == tabletPb.getReplicasCount()) {
        Status statusIOE = Status.IOError("Couldn't find any valid locations, exceptions: " +
            lookupExceptions);
        throw new NonRecoverableException(statusIOE);
      }

      RemoteTablet rt = new RemoteTablet(tableId, tabletPb, servers);

      LOG.debug("Learned about tablet {} for table '{}' with partition {}",
                rt.getTabletId(), tableName, rt.getPartition());
      tablets.add(rt);
    }

    // Give the locations to the tablet location cache for the table, so that it
    // can cache them and discover non-covered ranges.
    locationsCache.cacheTabletLocations(tablets, requestPartitionKey, requestedBatchSize, ttl);

    // Now test if we found the tablet we were looking for. If so, RetryRpcCB will retry the RPC
    // right away. If not, we throw an exception that RetryRpcErrback will understand as needing to
    // sleep before retrying.
    TableLocationsCache.Entry entry = locationsCache.get(requestPartitionKey);
    if (!entry.isNonCoveredRange() && entry.getTablet().getLeaderServerInfo() == null) {
      throw new NoLeaderFoundException(
          Status.NotFound("Tablet " + entry.toString() + " doesn't have a leader"));
    }
  }

  /**
   * Gets the tablet location cache entry for the tablet in the table covering a partition key.
   * @param tableId the table
   * @param partitionKey the partition key of the tablet to find
   * @return a tablet location cache entry, or null if the partition key has not been discovered
   */
  TableLocationsCache.Entry getTableLocationEntry(String tableId, byte[] partitionKey) {
    TableLocationsCache cache = tableLocations.get(tableId);
    if (cache == null) {
      return null;
    }
    return cache.get(partitionKey);
  }

  /**
   * Returns a deferred containing the located tablet which covers the partition key in the table.
   * @param table the table
   * @param partitionKey the partition key of the tablet to look up in the table
   * @param deadline deadline in milliseconds for this lookup to finish
   * @return a deferred containing the located tablet
   */
  Deferred<LocatedTablet> getTabletLocation(final KuduTable table,
                                            final byte[] partitionKey,
                                            long deadline) {

    // Locate the tablet at the partition key by locating tablets between
    // the partition key (inclusive), and the incremented partition key (exclusive).
    // We expect this to return at most a single tablet (checked below).
    byte[] startPartitionKey;
    byte[] endPartitionKey;
    if (partitionKey.length == 0) {
      startPartitionKey = null;
      endPartitionKey = new byte[] { 0x00 };
    } else {
      startPartitionKey = partitionKey;
      endPartitionKey = Arrays.copyOf(partitionKey, partitionKey.length + 1);
    }

    Deferred<List<LocatedTablet>> locatedTablets = locateTable(
        table, startPartitionKey, endPartitionKey, FETCH_TABLETS_PER_POINT_LOOKUP, deadline);

    // Then pick out the single tablet result from the list.
    return locatedTablets.addCallbackDeferring(
        new Callback<Deferred<LocatedTablet>, List<LocatedTablet>>() {
          @Override
          public Deferred<LocatedTablet> call(List<LocatedTablet> tablets) {
            Preconditions.checkArgument(tablets.size() <= 1,
                                        "found more than one tablet for a single partition key");
            if (tablets.isEmpty()) {
              // Most likely this indicates a non-covered range, but since this
              // could race with an alter table partitioning operation (which
              // clears the local table locations cache), we check again.
              TableLocationsCache.Entry entry = getTableLocationEntry(table.getTableId(),
                                                                      partitionKey);

              if (entry == null) {
                // This should be extremely rare, but a potential source of tight loops.
                LOG.debug("Table location expired before it could be processed; retrying.");
                return Deferred.fromError(new RecoverableException(Status.NotFound(
                    "Table location expired before it could be processed")));
              }
              if (entry.isNonCoveredRange()) {
                return Deferred.fromError(
                    new NonCoveredRangeException(entry.getLowerBoundPartitionKey(),
                                                 entry.getUpperBoundPartitionKey()));
              }
              return Deferred.fromResult(new LocatedTablet(entry.getTablet()));
            }
            return Deferred.fromResult(tablets.get(0));
          }
        });
  }

  /**
   * Invokes {@link #shutdown()} and waits. This method returns void, so consider invoking
   * {@link #shutdown()} directly if there's a need to handle dangling RPCs.
   *
   * @throws Exception if an error happens while closing the connections
   */
  @Override
  public void close() throws Exception {
    shutdown().join();
  }

  /**
   * Performs a graceful shutdown of this instance.
   * <p>
   * <ul>
   *   <li>{@link AsyncKuduSession#flush Flushes} all buffered edits.</li>
   *   <li>Cancels all the other requests.</li>
   *   <li>Terminates all connections.</li>
   *   <li>Releases all other resources.</li>
   * </ul>
   * <strong>Not calling this method before losing the last reference to this
   * instance may result in data loss and other unwanted side effects.</strong>
   *
   * @return A {@link Deferred}, whose callback chain will be invoked once all
   * of the above have been done. If this callback chain doesn't fail, then
   * the clean shutdown will be successful, and all the data will be safe on
   * the Kudu side. In case of a failure (the "errback" is invoked) you will have
   * to open a new AsyncKuduClient if you want to retry those operations.
   * The Deferred doesn't actually hold any content.
   */
  public Deferred<ArrayList<Void>> shutdown() {
    checkIsClosed();
    closed = true;
    // This is part of step 3.  We need to execute this in its own thread
    // because Netty gets stuck in an infinite loop if you try to shut it
    // down from within a thread of its own thread pool.  They don't want
    // to fix this so as a workaround we always shut Netty's thread pool
    // down from another thread.
    final class ShutdownThread extends Thread {
      ShutdownThread() {
        super("AsyncKuduClient@" + AsyncKuduClient.super.hashCode() + " shutdown");
      }

      @Override
      public void run() {
        // This terminates the Executor.
        channelFactory.releaseExternalResources();
      }
    }

    // 3. Release all other resources.
    final class ReleaseResourcesCB implements Callback<ArrayList<Void>, ArrayList<Void>> {
      public ArrayList<Void> call(final ArrayList<Void> arg) {
        LOG.debug("Releasing all remaining resources");
        timer.stop();
        new ShutdownThread().start();
        return arg;
      }

      public String toString() {
        return "release resources callback";
      }
    }

    // 2. Terminate all connections.
    final class DisconnectCB implements Callback<Deferred<ArrayList<Void>>,
        ArrayList<List<OperationResponse>>> {
      public Deferred<ArrayList<Void>> call(ArrayList<List<OperationResponse>> ignoredResponses) {
        return connectionCache.disconnectEverything().addCallback(new ReleaseResourcesCB());
      }

      public String toString() {
        return "disconnect callback";
      }
    }

    // 1. Flush everything.
    // Notice that we do not handle the errback, if there's an exception it will come straight out.
    return closeAllSessions().addCallbackDeferring(new DisconnectCB());
  }

  private void checkIsClosed() {
    if (closed) {
      throw new IllegalStateException("Cannot proceed, the client has already been closed");
    }
  }

  private Deferred<ArrayList<List<OperationResponse>>> closeAllSessions() {
    // We create a copy because AsyncKuduSession.close will call removeSession which would get us a
    // concurrent modification during the iteration.
    Set<AsyncKuduSession> copyOfSessions;
    synchronized (sessions) {
      copyOfSessions = new HashSet<>(sessions);
    }
    if (sessions.isEmpty()) {
      return Deferred.fromResult(null);
    }
    // Guaranteed that we'll have at least one session to close.
    List<Deferred<List<OperationResponse>>> deferreds = new ArrayList<>(copyOfSessions.size());
    for (AsyncKuduSession session : copyOfSessions ) {
      deferreds.add(session.close());
    }

    return Deferred.group(deferreds);
  }

  private static boolean isMasterTable(String tableId) {
    // Checking that it's the same instance so there's absolutely no chance of confusing the master
    // 'table' for a user one.
    return MASTER_TABLE_NAME_PLACEHOLDER == tableId;
  }

  void newTimeout(final TimerTask task, final long timeoutMs) {
    try {
      timer.newTimeout(task, timeoutMs, MILLISECONDS);
    } catch (IllegalStateException e) {
      // This can happen if the timer fires just before shutdown()
      // is called from another thread, and due to how threads get
      // scheduled we tried to call newTimeout() after timer.stop().
      LOG.warn("Failed to schedule timer." +
          " Ignore this if we're shutting down.", e);
    }
  }

  /**
   * @return copy of the current TabletClients list
   */
  @VisibleForTesting
  List<Connection> getConnectionListCopy() {
    return connectionCache.getConnectionListCopy();
  }

  /**
   * Builder class to use in order to connect to Kudu.
   * All the parameters beyond those in the constructors are optional.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static final class AsyncKuduClientBuilder {
    private static final int DEFAULT_MASTER_PORT = 7051;
    private static final int DEFAULT_BOSS_COUNT = 1;
    private static final int DEFAULT_WORKER_COUNT = 2 * Runtime.getRuntime().availableProcessors();

    private final List<HostAndPort> masterAddresses;
    private long defaultAdminOperationTimeoutMs = DEFAULT_OPERATION_TIMEOUT_MS;
    private long defaultOperationTimeoutMs = DEFAULT_OPERATION_TIMEOUT_MS;
    private long defaultSocketReadTimeoutMs = DEFAULT_SOCKET_READ_TIMEOUT_MS;

    private final HashedWheelTimer timer =
        new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).build(), 20, MILLISECONDS);
    private Executor bossExecutor;
    private Executor workerExecutor;
    private int bossCount = DEFAULT_BOSS_COUNT;
    private int workerCount = DEFAULT_WORKER_COUNT;
    private boolean statisticsDisabled = false;
    private Subject subject;

    /**
     * Creates a new builder for a client that will connect to the specified masters.
     * @param masterAddresses comma-separated list of "host:port" pairs of the masters
     */
    public AsyncKuduClientBuilder(String masterAddresses) {
      this.masterAddresses =
          NetUtil.parseStrings(masterAddresses, DEFAULT_MASTER_PORT);
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
    public AsyncKuduClientBuilder(List<String> masterAddresses) {
      this.masterAddresses =
          Lists.newArrayListWithCapacity(masterAddresses.size());
      for (String address : masterAddresses) {
        this.masterAddresses.add(
            NetUtil.parseString(address, DEFAULT_MASTER_PORT));
      }
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
    public AsyncKuduClientBuilder defaultAdminOperationTimeoutMs(long timeoutMs) {
      this.defaultAdminOperationTimeoutMs = timeoutMs;
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
    public AsyncKuduClientBuilder defaultOperationTimeoutMs(long timeoutMs) {
      this.defaultOperationTimeoutMs = timeoutMs;
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
    public AsyncKuduClientBuilder defaultSocketReadTimeoutMs(long timeoutMs) {
      this.defaultSocketReadTimeoutMs = timeoutMs;
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
    public AsyncKuduClientBuilder nioExecutors(Executor bossExecutor, Executor workerExecutor) {
      this.bossExecutor = bossExecutor;
      this.workerExecutor = workerExecutor;
      return this;
    }

    /**
     * Set the maximum number of boss threads.
     * Optional.
     * If not provided, 1 is used.
     */
    public AsyncKuduClientBuilder bossCount(int bossCount) {
      Preconditions.checkArgument(bossCount > 0, "bossCount should be greater than 0");
      this.bossCount = bossCount;
      return this;
    }

    /**
     * Set the maximum number of worker threads.
     * Optional.
     * If not provided, (2 * the number of available processors) is used.
     */
    public AsyncKuduClientBuilder workerCount(int workerCount) {
      Preconditions.checkArgument(workerCount > 0, "workerCount should be greater than 0");
      this.workerCount = workerCount;
      return this;
    }

    /**
     * Creates the channel factory for Netty. The user can specify the executors, but
     * if they don't, we'll use a simple thread pool.
     */
    private NioClientSocketChannelFactory createChannelFactory() {
      Executor boss = bossExecutor;
      Executor worker = workerExecutor;
      if (boss == null || worker == null) {
        Executor defaultExec = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("kudu-nio-%d")
                .setDaemon(true)
                .build());
        if (boss == null) {
          boss = defaultExec;
        }
        if (worker == null) {
          worker = defaultExec;
        }
      }
      // Share the timer with the socket channel factory so that it does not
      // create an internal timer with a non-daemon thread.
      return new NioClientSocketChannelFactory(boss,
                                               bossCount,
                                               new NioWorkerPool(worker, workerCount),
                                               timer);
    }

    /**
     * Disable this client's collection of statistics.
     * Statistics are enabled by default.
     * @return this builder
     */
    public AsyncKuduClientBuilder disableStatistics() {
      this.statisticsDisabled = true;
      return this;
    }

    /**
     * Creates a new client that connects to the masters.
     * Doesn't block and won't throw an exception if the masters don't exist.
     * @return a new asynchronous Kudu client
     */
    public AsyncKuduClient build() {
      subject = SecurityUtil.getSubjectOrLogin();
      return new AsyncKuduClient(this);
    }
  }
}
