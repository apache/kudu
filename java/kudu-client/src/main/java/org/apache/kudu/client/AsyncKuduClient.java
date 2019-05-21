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
import java.util.function.Consumer;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.kudu.security.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.master.Master;
import org.apache.kudu.master.Master.GetTableLocationsResponsePB;
import org.apache.kudu.master.Master.TableIdentifierPB;
import org.apache.kudu.master.Master.TSInfoPB;
import org.apache.kudu.util.AsyncUtil;
import org.apache.kudu.util.NetUtil;
import org.apache.kudu.util.Pair;

/**
 * A fully asynchronous and thread-safe client for Kudu.
 * <p>
 * A single Kudu client instance corresponds to a single remote Kudu cluster,
 * and can be used to read or write any number of tables within that cluster.
 * An application should use exactly one Kudu client instance per distinct Kudu
 * cluster it connects to.
 *
 * In rare cases where a single application needs multiple instances connected
 * to the same cluster, or when many applications each using one or more Kudu
 * client instances are running on the same machine, it may be necessary to
 * adjust the instances to use less resources. See the options in
 * {@link AsyncKuduClientBuilder}.
 *
 * <h1>Creating a client instance</h1> An {@link AsyncKuduClient} instance may
 * be created using the {@link AsyncKuduClient.AsyncKuduClientBuilder} class. If
 * a synchronous API is preferred, {@link KuduClient.KuduClientBuilder} may be
 * used instead. See the documentation on these classes for more details on
 * client configuration options.
 *
 * <h1>Authenticating to a secure cluster</h1> A Kudu cluster may be configured
 * such that it requires clients to connect using strong authentication. Clients
 * can authenticate to such clusters using either of two methods:
 * <ol>
 * <li><em>Kerberos credentials</em></li>
 * <li><em>Authentication tokens</em></li>
 * </ol>
 *
 * In a typical environment, Kerberos credentials are used for non-distributed
 * client applications and for applications which <em>spawn</em> distributed
 * jobs. Tokens are used for the <em>tasks</em> of distributed jobs, since those
 * tasks do not have access to the user's Kerberos credentials.
 *
 * <h2>Authenticating using Kerberos credentials</h2>
 *
 * In order to integrate with Kerberos, Kudu uses the standard <em>Java
 * Authentication and Authorization Service</em> (JAAS) API provided by the JDK.
 * JAAS provides a common way for applications to initialize Kerberos
 * credentials, store these credentials in a {@link javax.security.auth.Subject}
 * instance, and associate the Subject with the current thread of execution.
 * The Kudu client then accesses the Kerberos credentials in the
 * {@link javax.security.auth.Subject} and uses them to authenticate to the
 * remote cluster as necessary.
 * <p>
 * Kerberos credentials are typically obtained in one of two ways:
 * <ol>
 * <li>The <em>Kerberos ticket cache</em></li>
 * <li>A <em>keytab</em> file</li>
 * </ol>
 *
 * <h3>Authenticating from the Kerberos ticket cache</h3>
 *
 * The Kerberos <em>ticket cache</em> is a file stored on the local file system
 * which is automatically initialized when a user runs <em>kinit</em> at the
 * command line. This is the predominant method for authenticating users in
 * interactive applications: the user is expected to have run <em>kinit</em>
 * recently, and the application will find the appropriate credentials in the
 * ticket cache.
 * <p>
 * In the case of the Kudu client, Kudu will automatically look for credentials
 * in the standard system-configured ticket cache location. No additional code
 * needs to be written to enable this behavior.
 * <p>
 * Kudu will automatically detect if the ticket it has obtained from the ticket
 * cache is about to expire. When that is the case, it will attempt to re-read
 * the ticket cache to obtain a new ticket with a later expiration time. So, if
 * an application needs to run for longer than the lifetime of a single ticket,
 * the user must ensure that the ticket cache is periodically refreshed, for
 * example by re-running 'kinit' once each day.
 *
 * <h3>Authenticating from a keytab</h3>
 *
 * Long-running applications typically obtain Kerberos credentials from a
 * Kerberos <em>keytab</em> file. A keytab is essentially a saved password, and
 * allows the application to obtain new Kerberos tickets whenever the prior
 * ticket is about to expire.
 * <p>
 * The Kudu client does not provide any utility code to facilitate logging in
 * from a keytab. Instead, applications should invoke the JAAS APIs directly,
 * and then ensure that the resulting {@link javax.security.auth.Subject}
 * instance is associated with the current thread's
 * {@link java.security.AccessControlContext} when instantiating the Kudu client
 * instance for the first time. The {@link javax.security.auth.Subject} instance
 * will be stored and used whenever Kerberos authentication is required.
 * <p>
 * <b>Note</b>: if the Kudu client is instantiated with a
 * {@link javax.security.auth.Subject} as described above, it will <em>not</em>
 * make any attempt to re-login from the keytab. Instead, the application should
 * arrange to periodically re-initiate the login process and update the
 * credentials stored in the same Subject instance as was provided when the
 * client was instantiated.
 * <p>
 * In the context of the Hadoop ecosystem, the {@code UserGroupInformation}
 * class provides utility methods to login from a keytab and then run code as
 * the resulting {@link javax.security.auth.Subject}: <pre>{@code
 *   UserGroupInformation.loginUserFromKeytab("my-app", "/path/to/app.keytab");
 *   KuduClient c = UserGroupInformation.getLoginUser().doAs(
 *     new PrivilegedExceptionAction<KuduClient>() {
 *       &#64;Override
 *       public KuduClient run() throws Exception {
 *         return myClientBuilder.build();
 *       }
 *     }
 *   );
 * }</pre> The {@code UserGroupInformation} class will also automatically
 * start a thread to periodically re-login from the keytab.
 *
 * <h3>Debugging Kudu's usage of Kerberos credentials</h3>
 *
 * The Kudu client emits DEBUG-level logs under the
 * {@code org.apache.kudu.client.SecurityContext} slf4j category. Enabling DEBUG
 * logging for this class may help you understand which credentials are being
 * obtained by the Kudu client when it is instantiated. Additionally, if the
 * Java system property {@code kudu.jaas.debug} is set to {@code true}, Kudu
 * will enable the {@code debug} option when configuring {@code Krb5LoginModule}
 * when it attempts to log in from a ticket cache. JDK-specific system properties
 * such as {@code sun.security.krb5.debug} may also be useful in troubleshooting
 * Kerberos authentication failures.
 *
 * <h2>Authenticating using tokens</h2>
 *
 * In the case of distributed applications, the worker tasks often do not have
 * access to Kerberos credentials such as ticket caches or keytabs.
 * Additionally, there may be hundreds or thousands of workers with relatively
 * short life-times, and if each task attempted to authenticate using Kerberos,
 * the amount of load on the Kerberos infrastructure could be substantial enough
 * to cause instability. To solve this issue, Kudu provides support for
 * <em>authentication tokens</em>.
 * <p>
 * An authentication token is a time-limited credential which can be obtained by
 * an application which has already authenticated via Kerberos. The token is
 * represented by an opaque byte string, and it can be passed from one client to
 * another to transfer credentials.
 * <p>
 * A token may be generated using the
 * {@link AsyncKuduClient#exportAuthenticationCredentials()} API, and then
 * imported to another client using
 * {@link AsyncKuduClient#importAuthenticationCredentials(byte[])}.
 *
 * <h2>Authentication in Spark jobs</h2>
 *
 * Note that the Spark integration provided by the <em>kudu-spark</em> package
 * automatically handles the interaction with Kerberos and the passing of tokens
 * from the Spark driver to tasks. Refer to the Kudu documentation for details
 * on how to submit a Spark job on a secure cluster.
 *
 * <h1>API Compatibility</h1>
 *
 * Note that some methods in the Kudu client implementation are public but
 * annotated with the InterfaceAudience.Private annotation. This
 * annotation indicates that, despite having {@code public} visibility, the
 * method is not part of the public API and there is no guarantee that its
 * existence or behavior will be maintained in subsequent versions of the Kudu
 * client library.
 *
 * Other APIs are annotated with the InterfaceStability.Unstable annotation.
 * These APIs are meant for public consumption but may change between minor releases.
 * Note that the asynchronous client is currently considered unstable.
 *
 * <h1>Thread Safety</h1>
 *
 * The Kudu client instance itself is thread-safe; however, not all associated
 * classes are themselves thread-safe. For example, neither
 * {@link AsyncKuduSession} nor its synchronous wrapper {@link KuduSession} is
 * thread-safe. Refer to the documentation for each individual class for more
 * details.
 *
 * <h1>Asynchronous usage</h1>
 *
 * This client is fully non-blocking, any blocking operation will return a
 * {@link Deferred} instance to which you can attach a {@link Callback} chain
 * that will execute when the asynchronous operation completes.
 * <p>
 * The asynchronous calls themselves typically do not throw exceptions. Instead,
 * an {@code errback} should be attached which will be called with the Exception
 * that occurred.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class AsyncKuduClient implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);
  public static final int SLEEP_TIME = 500;
  public static final byte[] EMPTY_ARRAY = new byte[0];
  public static final long NO_TIMESTAMP = -1;
  public static final long DEFAULT_OPERATION_TIMEOUT_MS = 30000;
  public static final long DEFAULT_KEEP_ALIVE_PERIOD_MS = 15000; // 25% of the default scanner ttl.
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

  /** The Hive Metastore configuration of the most recently connected-to master. */
  @GuardedBy("this")
  private HiveMetastoreConfig hiveMetastoreConfig = null;

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
   * @see "src/kudu/common/common.proto"
   */
  private long lastPropagatedTimestamp = NO_TIMESTAMP;

  /**
   * Set to true once we have connected to a master at least once.
   *
   * This determines whether exportAuthenticationCredentials() needs to
   * proactively connect to the cluster to obtain a token.
   */
  private volatile boolean hasConnectedToMaster = false;

  /**
   * The location of this client as assigned by the leader master.
   *
   * If no location is assigned, will be an empty string.
   */
  private String location = "";

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

  private final Statistics statistics;

  private final boolean statisticsDisabled;

  private final RequestTracker requestTracker;

  @InterfaceAudience.LimitedPrivate("Test")
  final SecurityContext securityContext;

  /** A helper to facilitate re-acquiring of authentication token if current one expires. */
  private final AuthnTokenReacquirer tokenReacquirer;

  /** A helper to facilitate retrieving authz tokens */
  private final AuthzTokenCache authzTokenCache;

  private volatile boolean closed;

  private AsyncKuduClient(AsyncKuduClientBuilder b) {
    this.channelFactory = b.createChannelFactory();
    this.masterAddresses = b.masterAddresses;
    this.masterTable = new KuduTable(this, MASTER_TABLE_NAME_PLACEHOLDER,
        MASTER_TABLE_NAME_PLACEHOLDER, null, null, 1);
    this.defaultOperationTimeoutMs = b.defaultOperationTimeoutMs;
    this.defaultAdminOperationTimeoutMs = b.defaultAdminOperationTimeoutMs;
    this.statisticsDisabled = b.statisticsDisabled;
    this.statistics = statisticsDisabled ? null : new Statistics();
    this.timer = b.timer;
    this.requestTracker = new RequestTracker(UUID.randomUUID().toString().replace("-", ""));

    this.securityContext = new SecurityContext();
    this.connectionCache = new ConnectionCache(
        securityContext, timer, channelFactory);
    this.tokenReacquirer = new AuthnTokenReacquirer(this);
    this.authzTokenCache = new AuthzTokenCache(this);
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
        new ServerInfo(getFakeMasterUuid(hostPort),
                       hostPort,
                       inetAddress,
                       /* location= */""),
        credentialsPolicy);
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
       * Report on the token re-acquisition results. The result authn token might be null: in that
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
        synchronized (AsyncKuduClient.this) {
          location = masterResponsePB.getClientLocation();
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
   * Checks if the client received any timestamps from a server. Used for
   * CLIENT_PROPAGATED external consistency.
   *
   * @return true if last propagated timestamp has been set
   */
  public synchronized boolean hasLastPropagatedTimestamp() {
    return lastPropagatedTimestamp != NO_TIMESTAMP;
  }

  /**
   * Returns a string representation of this client's location. If this
   * client was not assigned a location, returns the empty string.
   *
   * @return a string representation of this client's location
   */
  public String getLocationString() {
    return location;
  }

  /**
   * Returns the {@link Timer} instance held by this client. This timer should
   * be used everywhere for scheduling tasks after a delay, e.g., for
   * timeouts.
   * @return the time instance held by this client
   */
  Timer getTimer() {
    return timer;
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

    // Send the CreateTable RPC.
    final CreateTableRequest create = new CreateTableRequest(this.masterTable,
                                                             name,
                                                             schema,
                                                             builder,
                                                             timer,
                                                             defaultAdminOperationTimeoutMs);
    Deferred<CreateTableResponse> createTableD = sendRpcToTablet(create);

    // Add a callback that converts the response into a KuduTable.
    Deferred<KuduTable> kuduTableD = createTableD.addCallbackDeferring(
        new Callback<Deferred<KuduTable>, CreateTableResponse>() {
          @Override
          public Deferred<KuduTable> call(CreateTableResponse resp) throws Exception {
            return getTableSchema(name, resp.getTableId(), create);
          }
        });

    if (!builder.shouldWait()) {
      return kuduTableD;
    }

    // If requested, add a callback that waits until all of the table's tablets
    // have been created.
    return kuduTableD.addCallbackDeferring(new Callback<Deferred<KuduTable>, KuduTable>() {
      @Override
      public Deferred<KuduTable> call(KuduTable tableResp) throws Exception {
        TableIdentifierPB.Builder table = TableIdentifierPB.newBuilder()
            .setTableId(ByteString.copyFromUtf8(tableResp.getTableId()));
        return getDelayedIsCreateTableDoneDeferred(table, create, tableResp);
      }
    });
  }

  /**
   * Check whether a previously issued createTable() is done.
   * @param name table's name
   * @return a deferred object to track the progress of the isCreateTableDone command
   */
  public Deferred<IsCreateTableDoneResponse> isCreateTableDone(String name) {
    return doIsCreateTableDone(TableIdentifierPB.newBuilder().setTableName(name), null);
  }

  /**
   * Check whether a previously issued createTable() is done.
   * @param table table identifier
   * @param parent parent RPC (for tracing), if any
   * @return a deferred object to track the progress of the isCreateTableDone command
   */
  private Deferred<IsCreateTableDoneResponse> doIsCreateTableDone(
      @Nonnull TableIdentifierPB.Builder table,
      @Nullable KuduRpc<?> parent) {
    checkIsClosed();
    IsCreateTableDoneRequest request = new IsCreateTableDoneRequest(this.masterTable,
                                                                    table,
                                                                    timer,
                                                                    defaultAdminOperationTimeoutMs);
    if (parent != null) {
      request.setParentRpc(parent);
    }
    return sendRpcToTablet(request);
  }

  /**
   * Delete a table on the cluster with the specified name.
   * @param name the table's name
   * @return a deferred object to track the progress of the deleteTable command
   */
  public Deferred<DeleteTableResponse> deleteTable(String name) {
    checkIsClosed();
    DeleteTableRequest delete = new DeleteTableRequest(this.masterTable,
                                                       name,
                                                       timer,
                                                       defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(delete);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   *
   * @param name the table's name (old name if the table is being renamed)
   * @param ato the alter table options
   * @return a deferred object to track the progress of the alter command
   */
  public Deferred<AlterTableResponse> alterTable(String name, AlterTableOptions ato) {
    checkIsClosed();
    final AlterTableRequest alter = new AlterTableRequest(this.masterTable,
                                                          name,
                                                          ato,
                                                          timer,
                                                          defaultAdminOperationTimeoutMs);
    Deferred<AlterTableResponse> responseD = sendRpcToTablet(alter);

    if (ato.hasAddDropRangePartitions()) {
      // Clear the table locations cache so the new partition is immediately visible.
      responseD = responseD.addCallback(new Callback<AlterTableResponse, AlterTableResponse>() {
        @Override
        public AlterTableResponse call(AlterTableResponse resp) {
          tableLocations.remove(resp.getTableId());
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
    if (!ato.shouldWait()) {
      return responseD;
    }

    // If requested, add a callback that waits until all of the table's tablets
    // have been altered.
    return responseD.addCallbackDeferring(
        new Callback<Deferred<AlterTableResponse>, AlterTableResponse>() {
      @Override
      public Deferred<AlterTableResponse> call(AlterTableResponse resp) throws Exception {
        TableIdentifierPB.Builder table = TableIdentifierPB.newBuilder()
            .setTableId(ByteString.copyFromUtf8(resp.getTableId()));
        return getDelayedIsAlterTableDoneDeferred(table, alter, resp);
      }
    });
  }

  /**
   * Check whether a previously issued alterTable() is done.
   * @param name table name
   * @return a deferred object to track the progress of the isAlterTableDone command
   */
  public Deferred<IsAlterTableDoneResponse> isAlterTableDone(String name) {
    return doIsAlterTableDone(TableIdentifierPB.newBuilder().setTableName(name), null);
  }

  /**
   * Check whether a previously issued alterTable() is done.
   * @param table table identifier
   * @param parent parent RPC (for tracing), if any
   * @return a deferred object to track the progress of the isAlterTableDone command
   */
  private Deferred<IsAlterTableDoneResponse> doIsAlterTableDone(
      @Nonnull TableIdentifierPB.Builder table,
      @Nullable KuduRpc<?> parent) {
    checkIsClosed();
    IsAlterTableDoneRequest request = new IsAlterTableDoneRequest(this.masterTable,
                                                                  table,
                                                                  timer,
                                                                  defaultAdminOperationTimeoutMs);
    request.setParentRpc(parent);
    return sendRpcToTablet(request);
  }

  /**
   * Get the list of running tablet servers.
   * @return a deferred object that yields a list of tablet servers
   */
  public Deferred<ListTabletServersResponse> listTabletServers() {
    checkIsClosed();
    ListTabletServersRequest rpc = new ListTabletServersRequest(this.masterTable,
                                                                timer,
                                                                defaultAdminOperationTimeoutMs);
    return sendRpcToTablet(rpc);
  }

  /**
   * Gets a table's schema either by ID or by name. Note: the name must be
   * provided, even if the RPC should be sent by ID.
   * @param tableName name of table
   * @param tableId immutable ID of table
   * @param parent parent RPC (for tracing), if any
   * @return a deferred object that yields the schema
   */
  private Deferred<KuduTable> getTableSchema(
      @Nonnull final String tableName,
      @Nullable String tableId,
      @Nullable KuduRpc<?> parent) {
    Preconditions.checkNotNull(tableName);

    // Prefer a lookup by table ID over name, since the former is immutable.
    // For backwards compatibility with older tservers, we don't require authz
    // token support.
    GetTableSchemaRequest rpc = new GetTableSchemaRequest(this.masterTable,
                                                          tableId,
                                                          tableId != null ? null : tableName,
                                                          timer,
                                                          defaultAdminOperationTimeoutMs,
                                                          /*requiresAuthzTokenSupport=*/false);

    rpc.setParentRpc(parent);
    return sendRpcToTablet(rpc).addCallback(new Callback<KuduTable, GetTableSchemaResponse>() {
      @Override
      public KuduTable call(GetTableSchemaResponse resp) throws Exception {
        // When opening a table, clear the existing cached non-covered range entries.
        // This avoids surprises where a new table instance won't be able to see the
        // current range partitions of a table for up to the ttl.
        TableLocationsCache cache = tableLocations.get(resp.getTableId());
        if (cache != null) {
          cache.clearNonCoveredRangeEntries();
        }
        SignedTokenPB authzToken = resp.getAuthzToken();
        if (authzToken != null) {
          authzTokenCache.put(resp.getTableId(), authzToken);
        }

        LOG.debug("Opened table {}", resp.getTableId());
        return new KuduTable(AsyncKuduClient.this,
            tableName,
            resp.getTableId(),
            resp.getSchema(),
            resp.getPartitionSchema(),
            resp.getNumReplicas());
      }
    });
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
    ListTablesRequest rpc = new ListTablesRequest(this.masterTable,
                                                  nameFilter,
                                                  timer,
                                                  defaultAdminOperationTimeoutMs);
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
   * Open the table with the given name.
   *
   * New range partitions created by other clients will immediately be available
   * after opening the table.
   *
   * @param name table to open
   * @return a deferred KuduTable
   */
  public Deferred<KuduTable> openTable(String name) {
    checkIsClosed();
    return getTableSchema(name, null, null);
  }

  /**
   * Export serialized authentication data that may be passed to a different
   * client instance and imported to provide that client the ability to connect
   * to the cluster.
   */
  @InterfaceStability.Unstable
  public Deferred<byte[]> exportAuthenticationCredentials() {
    // This is basically just a hacky way to encapsulate the necessary bits to
    // properly do exponential backoff on retry; there's no actual "RPC" to send.
    KuduRpc<byte[]> fakeRpc = buildFakeRpc("exportAuthenticationCredentials", null);

    // Store the Deferred locally; callback() or errback() on the RPC will
    // reset it and we'd return a different, non-triggered Deferred.
    Deferred<byte[]> fakeRpcD = fakeRpc.getDeferred();
    doExportAuthenticationCredentials(fakeRpc);
    return fakeRpcD;
  }

  private void doExportAuthenticationCredentials(
      final KuduRpc<byte[]> fakeRpc) {
    // If we've already connected to the master, use the authentication
    // credentials that we received when we connected.
    if (hasConnectedToMaster) {
      fakeRpc.callback(
          securityContext.exportAuthenticationCredentials());
      return;
    }

    // We have no authn data -- connect to the master, which will fetch
    // new info.
    fakeRpc.attempt++;
    getMasterTableLocationsPB(null)
        .addCallback(new MasterLookupCB(masterTable,
                                        /* partitionKey */ null,
                                        /* requestedBatchSize */ 1))
        .addCallback(new Callback<Void, Object>() {
          @Override
          public Void call(Object ignored) {
            // Just call ourselves again; we're guaranteed to have the
            // authentication credentials.
            assert hasConnectedToMaster;
            doExportAuthenticationCredentials(fakeRpc);
            return null;
          }
        })
        .addErrback(new RetryTaskErrback<byte[]>(
            fakeRpc, new TimerTask() {
                @Override
                public void run(final Timeout ignored) {
                  doExportAuthenticationCredentials(fakeRpc);
                }
              }));
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public AuthzTokenCache getAuthzTokenCache() {
    return this.authzTokenCache;
  }

  /**
   * Get the Hive Metastore configuration of the most recently connected-to leader master, or
   * {@code null} if the Hive Metastore integration is not enabled.
   */
  @InterfaceAudience.LimitedPrivate("Impala")
  @InterfaceStability.Unstable
  public Deferred<HiveMetastoreConfig> getHiveMetastoreConfig() {
    // This is basically just a hacky way to encapsulate the necessary bits to
    // properly do exponential backoff on retry; there's no actual "RPC" to send.
    KuduRpc<HiveMetastoreConfig> fakeRpc = buildFakeRpc("getHiveMetastoreConfig", null);

    // Store the Deferred locally; callback() or errback() on the RPC will
    // reset it and we'd return a different, non-triggered Deferred.
    Deferred<HiveMetastoreConfig> fakeRpcD = fakeRpc.getDeferred();
    doGetHiveMetastoreConfig(fakeRpc);
    return fakeRpcD;
  }

  private void doGetHiveMetastoreConfig(final KuduRpc<HiveMetastoreConfig> fakeRpc) {
    // If we've already connected to the master, use the config we received when we connected.
    if (hasConnectedToMaster) {
      // Take a ref to the HMS config under the lock, but invoke the callback
      // chain with the lock released.
      HiveMetastoreConfig c;
      synchronized (this) {
        c = hiveMetastoreConfig;
      }
      fakeRpc.callback(c);
      return;
    }

    // We have no Metastore config -- connect to the master, which will fetch new info.
    fakeRpc.attempt++;
    getMasterTableLocationsPB(null)
        .addCallback(new MasterLookupCB(masterTable,
                                        /* partitionKey */ null,
                                        /* requestedBatchSize */ 1))
        .addCallback(new Callback<Void, Object>() {
          @Override
          public Void call(Object ignored) {
            // Just call ourselves again; we're guaranteed to have the HMS config.
            assert hasConnectedToMaster;
            doGetHiveMetastoreConfig(fakeRpc);
            return null;
          }
        })
        .addErrback(new RetryTaskErrback<HiveMetastoreConfig>(
            fakeRpc, new TimerTask() {
                @Override
                public void run(final Timeout ignored) {
                  doGetHiveMetastoreConfig(fakeRpc);
                }
              }));
  }

  /**
   * Errback for retrying a generic TimerTask. Retries RecoverableExceptions;
   * signals fakeRpc's Deferred on a fatal error.
   */
  class RetryTaskErrback<R> implements Callback<Void, Exception> {
    private final KuduRpc<R> fakeRpc;
    private final TimerTask retryTask;

    public RetryTaskErrback(KuduRpc<R> fakeRpc,
                            TimerTask retryTask) {
      this.fakeRpc = fakeRpc;
      this.retryTask = retryTask;
    }

    @Override
    public Void call(Exception arg) {
      if (!(arg instanceof RecoverableException)) {
        fakeRpc.errback(arg);
        return null;
      }

      // Sleep and retry the entire operation.
      RecoverableException ex = (RecoverableException)arg;
      long sleepTime = getSleepTimeForRpcMillis(fakeRpc);
      if (cannotRetryRequest(fakeRpc) ||
          fakeRpc.timeoutTracker.wouldSleepingTimeoutMillis(sleepTime)) {
        tooManyAttemptsOrTimeout(fakeRpc, ex); // invokes fakeRpc.Deferred
        return null;
      }
      fakeRpc.addTrace(
          new RpcTraceFrame.RpcTraceFrameBuilder(
              fakeRpc.method(),
              RpcTraceFrame.Action.SLEEP_THEN_RETRY)
          .callStatus(ex.getStatus())
          .build());
      newTimeout(timer, retryTask, sleepTime);
      return null;

      // fakeRpc.Deferred was not invoked; the user continues to wait until
      // retryTask succeeds or fails with a fatal error.
    }

    @Override
    public String toString() {
      return "retry task after error";
    }
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
   * Socket read timeouts are no longer used in the Java client and have no effect.
   * This method always returns 0, as that previously indicated no socket read timeout.
   * @return a timeout in milliseconds
   * @deprecated socket read timeouts are no longer used
   */
  @Deprecated public long getDefaultSocketReadTimeoutMs() {
    LOG.info("getDefaultSocketReadTimeoutMs is deprecated");
    return 0;
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

  @InterfaceAudience.LimitedPrivate("Test")
  KuduTable getMasterTable() {
    return masterTable;
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
    final ServerInfo info = tablet.getReplicaSelectedServerInfo(nextRequest.getReplicaSelection(),
                                                                location);
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
   * @param scanner the scanner to close.
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
    final ServerInfo info = tablet.getReplicaSelectedServerInfo(closeRequest.getReplicaSelection(),
                                                                location);
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
   * Package-private access point for {@link AsyncKuduScanner}s to keep themselves
   * alive on tablet servers.
   * @param scanner the scanner to keep alive.
   * @return a deferred object that indicates the completion of the request.
   */
  Deferred<Void> keepAlive(final AsyncKuduScanner scanner) {
    checkIsClosed();
    final RemoteTablet tablet = scanner.currentTablet();
    // Getting a null tablet here without being in a closed state means we were in between tablets.
    // If there is no scanner to keep alive, we still return Status.OK().
    if (tablet == null) {
      return Deferred.fromResult(null);
    }

    final KuduRpc<Void> keepAliveRequest = scanner.getKeepAliveRequest();
    final ServerInfo info = tablet.getReplicaSelectedServerInfo(keepAliveRequest.getReplicaSelection(),
                                                                location);
    if (info == null) {
      return Deferred.fromResult(null);
    }

    final Deferred<Void> d = keepAliveRequest.getDeferred();
    keepAliveRequest.attempt++;
    RpcProxy.sendRpc(this, connectionCache.getConnection(
        info, Connection.CredentialsPolicy.ANY_CREDENTIALS), keepAliveRequest);
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
      ServerInfo info = tablet.getReplicaSelectedServerInfo(request.getReplicaSelection(),
                                                            location);
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

    @Override
    public Deferred<R> call(final D arg) {
      LOG.debug("Retrying sending RPC {} after lookup", request);
      return sendRpcToTablet(request);  // Retry the RPC.
    }

    @Override
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
   * Returns an errback ensuring that if the delayed call throws an Exception,
   * it will be propagated back to the user.
   * <p>
   * @param rpc RPC to errback if there's a problem with the delayed call
   * @param <R> RPC's return type
   * @return newly created errback
   */
  private <R> Callback<Exception, Exception> getDelayedIsTableDoneEB(
      final KuduRpc<R> rpc) {
    return new Callback<Exception, Exception>() {
      @Override
      public Exception call(Exception e) throws Exception {
        // TODO maybe we can retry it?
        rpc.errback(e);
        return e;
      }
    };
  }

  /**
   * Creates an RPC that will never be sent, and will instead be used
   * exclusively for timeouts.
   * @param method fake RPC method (shows up in RPC traces)
   * @param parent parent RPC (for tracing), if any
   * @param <R> the expected return type of the fake RPC
   * @param timeoutMs the timeout in milliseconds for the fake RPC
   * @return created fake RPC
   */
  private <R> KuduRpc<R> buildFakeRpc(
      @Nonnull final String method,
      @Nullable final KuduRpc<?> parent,
      long timeoutMs) {
    KuduRpc<R> rpc = new KuduRpc<R>(null, timer, timeoutMs) {
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
        return method;
      }

      @Override
      Pair<R, Object> deserialize(
          CallResponse callResponse, String tsUUID) throws KuduException {
        return null;
      }
    };
    rpc.setParentRpc(parent);
    return rpc;
  }

  /**
   * Creates an RPC that will never be sent, and will instead be used
   * exclusively for timeouts.
   * @param method fake RPC method (shows up in RPC traces)
   * @param parent parent RPC (for tracing), if any
   * @param <R> the expected return type of the fake RPC
   * @return created fake RPC
   */
  private <R> KuduRpc<R> buildFakeRpc(
      @Nonnull final String method,
      @Nullable final KuduRpc<?> parent) {
    return buildFakeRpc(method, parent, defaultAdminOperationTimeoutMs);
  }

  /**
   * Schedules a IsAlterTableDone RPC. When the response comes in, if the table
   * is done altering, the RPC's callback chain is triggered with 'resp' as its
   * value. If not, another IsAlterTableDone RPC is scheduled and the cycle
   * repeats, until the alter is finished or a timeout is reached.
   * @param table table identifier
   * @param parent parent RPC (for tracing), if any
   * @param resp previous AlterTableResponse, if any
   * @return Deferred that will become ready when the alter is done
   */
  Deferred<AlterTableResponse> getDelayedIsAlterTableDoneDeferred(
      @Nonnull TableIdentifierPB.Builder table,
      @Nullable KuduRpc<?> parent,
      @Nullable AlterTableResponse resp) {
    // TODO(adar): By scheduling even the first RPC via timer, the sequence of
    // RPCs is delayed by at least one timer tick, which is unfortunate for the
    // case where the table is already fully altered.
    //
    // Eliminating the delay by sending the first RPC immediately (and
    // scheduling the rest via timer) would also allow us to replace this "fake"
    // RPC with a real one.
    KuduRpc<AlterTableResponse> fakeRpc = buildFakeRpc("IsAlterTableDone", parent);

    // Store the Deferred locally; callback() or errback() on the RPC will
    // reset it and we'd return a different, non-triggered Deferred.
    Deferred<AlterTableResponse> fakeRpcD = fakeRpc.getDeferred();

    delayedIsAlterTableDone(
        table,
        fakeRpc,
        getDelayedIsAlterTableDoneCB(fakeRpc, table, resp),
        getDelayedIsTableDoneEB(fakeRpc));
    return fakeRpcD;
  }

  /**
   * Schedules a IsCreateTableDone RPC. When the response comes in, if the table
   * is done creating, the RPC's callback chain is triggered with 'resp' as its
   * value. If not, another IsCreateTableDone RPC is scheduled and the cycle
   * repeats, until the createis finished or a timeout is reached.
   * @param table table identifier
   * @param parent parent RPC (for tracing), if any
   * @param resp previous KuduTable, if any
   * @return Deferred that will become ready when the create is done
   */
  Deferred<KuduTable> getDelayedIsCreateTableDoneDeferred(
      @Nonnull TableIdentifierPB.Builder table,
      @Nullable KuduRpc<?> parent,
      @Nullable KuduTable resp) {
    // TODO(adar): By scheduling even the first RPC via timer, the sequence of
    // RPCs is delayed by at least one timer tick, which is unfortunate for the
    // case where the table is already fully altered.
    //
    // Eliminating the delay by sending the first RPC immediately (and
    // scheduling the rest via timer) would also allow us to replace this "fake"
    // RPC with a real one.
    KuduRpc<KuduTable> fakeRpc = buildFakeRpc("IsCreateTableDone", parent);

    // Store the Deferred locally; callback() or errback() on the RPC will
    // reset it and we'd return a different, non-triggered Deferred.
    Deferred<KuduTable> fakeRpcD = fakeRpc.getDeferred();

    delayedIsCreateTableDone(
        table,
        fakeRpc,
        getDelayedIsCreateTableDoneCB(fakeRpc, table, resp),
        getDelayedIsTableDoneEB(fakeRpc));
    return fakeRpcD;
  }

  /**
   * Returns a callback to be called upon completion of an IsAlterTableDone RPC.
   * If the table is fully altered, triggers the provided rpc's callback chain
   * with 'alterResp' as its value. Otherwise, sends another IsAlterTableDone
   * RPC after sleeping.
   * <p>
   * @param rpc RPC that initiated this sequence of operations
   * @param table table identifier
   * @param alterResp response from an earlier AlterTable RPC, if any
   * @return callback that will eventually return 'alterResp'
   */
  private Callback<Deferred<AlterTableResponse>, IsAlterTableDoneResponse> getDelayedIsAlterTableDoneCB(
      @Nonnull final KuduRpc<AlterTableResponse> rpc,
      @Nonnull final TableIdentifierPB.Builder table,
      @Nullable final AlterTableResponse alterResp) {
    return new Callback<Deferred<AlterTableResponse>, IsAlterTableDoneResponse>() {
      @Override
      public Deferred<AlterTableResponse> call(IsAlterTableDoneResponse resp) throws Exception {
        // Store the Deferred locally; callback() below will reset it and we'd
        // return a different, non-triggered Deferred.
        Deferred<AlterTableResponse> d = rpc.getDeferred();
        if (resp.isDone()) {
          rpc.callback(alterResp);
        } else {
          rpc.attempt++;
          delayedIsAlterTableDone(
              table,
              rpc,
              getDelayedIsAlterTableDoneCB(rpc, table, alterResp),
              getDelayedIsTableDoneEB(rpc));
        }
        return d;
      }
    };
  }

  /**
   * Returns a callback to be called upon completion of an IsCreateTableDone RPC.
   * If the table is fully created, triggers the provided rpc's callback chain
   * with 'tableResp' as its value. Otherwise, sends another IsCreateTableDone
   * RPC after sleeping.
   * <p>
   * @param rpc RPC that initiated this sequence of operations
   * @param table table identifier
   * @param tableResp previously constructed KuduTable, if any
   * @return callback that will eventually return 'tableResp'
   */
  private Callback<Deferred<KuduTable>, IsCreateTableDoneResponse> getDelayedIsCreateTableDoneCB(
      final KuduRpc<KuduTable> rpc,
      final TableIdentifierPB.Builder table,
      final KuduTable tableResp) {
    return new Callback<Deferred<KuduTable>, IsCreateTableDoneResponse>() {
      @Override
      public Deferred<KuduTable> call(IsCreateTableDoneResponse resp) throws Exception {
        // Store the Deferred locally; callback() below will reset it and we'd
        // return a different, non-triggered Deferred.
        Deferred<KuduTable> d = rpc.getDeferred();
        if (resp.isDone()) {
          rpc.callback(tableResp);
        } else {
          rpc.attempt++;
          delayedIsCreateTableDone(
              table,
              rpc,
              getDelayedIsCreateTableDoneCB(rpc, table, tableResp),
              getDelayedIsTableDoneEB(rpc));
        }
        return d;
      }
    };
  }

  /**
   * Schedules a timer to send an IsCreateTableDone RPC to the master after
   * sleeping for getSleepTimeForRpc() (based on the provided KuduRpc's number
   * of attempts). When the master responds, the provided callback will be called.
   * <p>
   * @param table table identifier
   * @param rpc original KuduRpc that needs to access the table
   * @param callback callback to call on completion
   * @param errback errback to call if something goes wrong
   */
  private void delayedIsCreateTableDone(
      final TableIdentifierPB.Builder table,
      final KuduRpc<KuduTable> rpc,
      final Callback<Deferred<KuduTable>, IsCreateTableDoneResponse> callback,
      final Callback<Exception, Exception> errback) {
    final class RetryTimer implements TimerTask {
      @Override
      public void run(final Timeout timeout) {
        doIsCreateTableDone(table, rpc).addCallbacks(callback, errback);
      }
    }

    long sleepTimeMillis = getSleepTimeForRpcMillis(rpc);
    if (rpc.timeoutTracker.wouldSleepingTimeoutMillis(sleepTimeMillis)) {
      tooManyAttemptsOrTimeout(rpc, null);
      return;
    }
    newTimeout(timer, new RetryTimer(), sleepTimeMillis);
  }

  /**
   * Schedules a timer to send an IsAlterTableDone RPC to the master after
   * sleeping for getSleepTimeForRpc() (based on the provided KuduRpc's number
   * of attempts). When the master responds, the provided callback will be called.
   * <p>
   * @param table table identifier
   * @param rpc original KuduRpc that needs to access the table
   * @param callback callback to call on completion
   * @param errback errback to call if something goes wrong
   */
  private void delayedIsAlterTableDone(
      final TableIdentifierPB.Builder table,
      final KuduRpc<AlterTableResponse> rpc,
      final Callback<Deferred<AlterTableResponse>, IsAlterTableDoneResponse> callback,
      final Callback<Exception, Exception> errback) {
    final class RetryTimer implements TimerTask {
      @Override
      public void run(final Timeout timeout) {
        doIsAlterTableDone(table, rpc).addCallbacks(callback, errback);
      }
    }

    long sleepTimeMillis = getSleepTimeForRpcMillis(rpc);
    if (rpc.timeoutTracker.wouldSleepingTimeoutMillis(sleepTimeMillis)) {
      tooManyAttemptsOrTimeout(rpc, null);
      return;
    }
    newTimeout(timer, new RetryTimer(), sleepTimeMillis);
  }

  private final class ReleaseMasterLookupPermit<T> implements Callback<T, T> {
    @Override
    public T call(final T arg) {
      releaseMasterLookupPermit();
      return arg;
    }

    @Override
    public String toString() {
      return "release master lookup permit";
    }
  }

  private long getSleepTimeForRpcMillis(KuduRpc<?> rpc) {
    int attemptCount = rpc.attempt;
    if (attemptCount == 0) {
      // If this is the first RPC attempt, don't sleep at all.
      return 0;
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
  @InterfaceAudience.LimitedPrivate("Test")
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
    return rpc.timeoutTracker.timedOut() || rpc.attempt > MAX_RPC_ATTEMPTS;
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
      message = "cannot complete before timeout: ";
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
      long timeoutMillis = parentRpc == null ? defaultAdminOperationTimeoutMs :
                                               parentRpc.timeoutTracker.getMillisBeforeTimeout();
      // Leave the end of the partition key range empty in order to pre-fetch tablet locations.
      GetTableLocationsRequest rpc =
          new GetTableLocationsRequest(masterTable,
                                       partitionKey,
                                       null,
                                       tableId,
                                       fetchBatchSize,
                                       timer,
                                       timeoutMillis);
      rpc.setParentRpc(parentRpc);
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

                HiveMetastoreConfig hiveMetastoreConfig = null;
                Master.ConnectToMasterResponsePB respPb = resp.getConnectResponse();
                if (respPb.hasHmsConfig()) {
                  Master.HiveMetastoreConfig metastoreConf = respPb.getHmsConfig();
                  hiveMetastoreConfig = new HiveMetastoreConfig(metastoreConf.getHmsUris(),
                                                                metastoreConf.getHmsSaslEnabled(),
                                                                metastoreConf.getHmsUuid());
                }
                synchronized (AsyncKuduClient.this) {
                  AsyncKuduClient.this.hiveMetastoreConfig = hiveMetastoreConfig;
                  location = respPb.getClientLocation();
                }

                hasConnectedToMaster = true;

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
   * @throws Exception if anything went wrong
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
                                                        final TimeoutTracker timeoutTracker) {
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

      if (timeoutTracker.timedOut()) {
        Status statusTimedOut = Status.TimedOut("Took too long getting the list of tablets, " +
            timeoutTracker);
        return Deferred.fromError(new NonRecoverableException(statusTimedOut));
      }

      // If the partition key location isn't cached, and the request hasn't timed out,
      // then kick off a new tablet location lookup and try again when it completes.
      // When lookup completes, the tablet (or non-covered range) for the next
      // partition key will be located and added to the client's cache.
      final byte[] lookupKey = partitionKey;

      // Build a fake RPC to encapsulate and propagate the timeout. There's no actual "RPC" to send.
      KuduRpc fakeRpc = buildFakeRpc("loopLocateTable",
                                     null,
                                     timeoutTracker.getMillisBeforeTimeout());

      return locateTablet(table, key, fetchBatchSize, fakeRpc).addCallbackDeferring(
          new Callback<Deferred<List<LocatedTablet>>, GetTableLocationsResponsePB>() {
            @Override
            public Deferred<List<LocatedTablet>> call(GetTableLocationsResponsePB resp) {
              return loopLocateTable(table,
                                     lookupKey,
                                     endPartitionKey,
                                     fetchBatchSize,
                                     ret,
                  timeoutTracker);
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
    final TimeoutTracker timeoutTracker = new TimeoutTracker();
    timeoutTracker.setTimeout(deadline);
    return loopLocateTable(table,
                           startPartitionKey,
                           endPartitionKey,
                           fetchBatchSize,
                           ret,
        timeoutTracker);
  }

  /**
   * We're handling a tablet server that's telling us it doesn't have the tablet we're asking for.
   * We're in the context of decode() meaning we need to either callback or retry later.
   */
  <R> void handleTabletNotFound(final KuduRpc<R> rpc, KuduException ex, ServerInfo info) {
    invalidateTabletCache(rpc.getTablet(), info, ex.getMessage());
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
   * Handle an RPC failed due to invalid authn token error. In short, connect to the Kudu cluster
   * to acquire a new authentication token and retry the RPC once a new authentication token
   * is put into the {@link #securityContext}.
   *
   * @param rpc the RPC which failed with an invalid authn token
   */
  <R> void handleInvalidAuthnToken(KuduRpc<R> rpc) {
    // TODO(awong): plumb the offending KuduException into the reacquirer.
    tokenReacquirer.handleAuthnTokenExpiration(rpc);
  }

  /**
   * Handle an RPC that failed due to an invalid authorization token error. The
   * RPC will be retried after fetching a new authz token.
   *
   * @param rpc the RPC that failed with an invalid authz token
   * @param ex the KuduException that led to this handling
   */
  <R> void handleInvalidAuthzToken(KuduRpc<R> rpc, KuduException ex) {
    authzTokenCache.retrieveAuthzToken(rpc, ex);
  }

  /**
   * Gets an authorization token for the given table from the cache, or nullptr
   * if none exists.
   *
   * @param tableId the table ID for which to get an authz token
   * @return a signed authz token for the table
   */
  SignedTokenPB getAuthzToken(String tableId) {
    return authzTokenCache.get(tableId);
  }

  /**
   * This methods enable putting RPCs on hold for a period of time determined by
   * {@link #getSleepTimeForRpcMillis(KuduRpc)}. If the RPC is out of time/retries, its errback will
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
      @Override
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

    long sleepTime = getSleepTimeForRpcMillis(rpc);
    if (cannotRetryRequest(rpc) ||
        rpc.timeoutTracker.wouldSleepingTimeoutMillis(sleepTime)) {
      // Don't let it retry.
      return tooManyAttemptsOrTimeout(rpc, ex);
    }
    newTimeout(timer, new RetryTimer(), sleepTime);
    return rpc.getDeferred();
  }

  /**
   * Remove the tablet server from the RemoteTablet's locations. Right now nothing is removing
   * the tablet itself from the caches.
   */
  private void invalidateTabletCache(RemoteTablet tablet, ServerInfo info,
                                     String errorMessage) {
    final String uuid = info.getUuid();
    LOG.info("Invalidating location {} for tablet {}: {}",
             info, tablet.getTabletId(), errorMessage);
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

    // From meta_cache.cc:
    // TODO: If the TS advertises multiple host/ports, pick the right one
    // based on some kind of policy. For now just use the first always.
    final HostAndPort hostPort = ProtobufHelper.hostAndPortFromPB(addresses.get(0));
    final InetAddress inetAddress = NetUtil.getInetAddress(hostPort.getHost());
    if (inetAddress == null) {
      throw new UnknownHostException(
          "Failed to resolve the IP of `" + addresses.get(0).getHost() + "'");
    }
    return new ServerInfo(uuid, hostPort, inetAddress, tsInfoPB.getLocation());
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

    @Override
    public Object call(final GetTableLocationsResponsePB response) {
      if (response.hasError()) {
        Status status = Status.fromMasterErrorPB(response.getError());
        return new NonRecoverableException(status);
      } else {
        try {
          discoverTablets(table,
                          partitionKey,
                          requestedBatchSize,
                          response.getTabletLocationsList(),
                          response.getTsInfosList(),
                          response.getTtlMillis());
        } catch (KuduException e) {
          return e;
        }
      }
      return null;
    }

    @Override
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
   * Makes discovered tablet locations visible in the client's caches.
   * @param table the table which the locations belong to
   * @param requestPartitionKey the partition key of the table locations request
   * @param requestedBatchSize the number of tablet locations requested from the master in the
   *                           original request
   * @param locations the discovered locations
   * @param tsInfosList a list of ts info that the replicas in 'locations' references by index.
   * @param ttl the ttl of the locations
   */
  @InterfaceAudience.LimitedPrivate("Test")
  void discoverTablets(KuduTable table,
                       byte[] requestPartitionKey,
                       int requestedBatchSize,
                       List<Master.TabletLocationsPB> locations,
                       List<Master.TSInfoPB> tsInfosList,
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
    int numTsInfos = tsInfosList.size();
    List<RemoteTablet> tablets = new ArrayList<>(locations.size());
    for (Master.TabletLocationsPB tabletPb : locations) {

      List<Exception> lookupExceptions = new ArrayList<>(tabletPb.getReplicasCount());
      List<ServerInfo> servers = new ArrayList<>(tabletPb.getReplicasCount());

      // Lambda that does the common handling of a ts info.
      Consumer<Master.TSInfoPB> updateServersAndCollectExceptions = tsInfo -> {
        try {
          ServerInfo serverInfo = resolveTS(tsInfo);
          if (serverInfo != null) {
            servers.add(serverInfo);
          }
        } catch (UnknownHostException ex) {
          lookupExceptions.add(ex);
        }
      };

      // Handle "old-style" non-interned replicas.
      for (Master.TabletLocationsPB.ReplicaPB replica : tabletPb.getReplicasList()) {
        updateServersAndCollectExceptions.accept(replica.getTsInfo());
      }

      // Handle interned replicas. As a shim, we also need to create a list of "old-style" ReplicaPBs
      // to be stored inside the RemoteTablet.
      // TODO(wdberkeley): Change this so ReplicaPBs aren't used by the client at all anymore.
      List<Master.TabletLocationsPB.ReplicaPB> replicas = new ArrayList<>();
      for (Master.TabletLocationsPB.InternedReplicaPB replica : tabletPb.getInternedReplicasList()) {
        int tsInfoIdx = replica.getTsInfoIdx();
        if (tsInfoIdx >= numTsInfos) {
          lookupExceptions.add(new NonRecoverableException(Status.Corruption(
              String.format("invalid response from master: referenced tablet idx %d but only %d present",
                            tsInfoIdx, numTsInfos))));
          continue;
        }
        TSInfoPB tsInfo = tsInfosList.get(tsInfoIdx);
        updateServersAndCollectExceptions.accept(tsInfo);
        Master.TabletLocationsPB.ReplicaPB.Builder builder = Master.TabletLocationsPB.ReplicaPB.newBuilder();
        builder.setRole(replica.getRole());
        builder.setTsInfo(tsInfo);
        replicas.add(builder.build());
      }

      if (!lookupExceptions.isEmpty() &&
          lookupExceptions.size() == tabletPb.getReplicasCount()) {
        Status statusIOE = Status.IOError("Couldn't find any valid locations, exceptions: " +
            lookupExceptions);
        throw new NonRecoverableException(statusIOE);
      }

      RemoteTablet rt = new RemoteTablet(tableId,
                                         tabletPb.getTabletId().toStringUtf8(),
                                         ProtobufHelper.pbToPartition(tabletPb.getPartition()),
                                         replicas.isEmpty() ? tabletPb.getReplicasList() : replicas,
                                         servers);

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

  enum LookupType {
    // The lookup should only return a tablet which actually covers the
    // requested partition key.
    POINT,
    // The lookup should return the next tablet after the requested
    // partition key if the requested key does not fall within a covered
    // range.
    LOWER_BOUND
  }

  /**
   * Returns a deferred containing the located tablet which covers the partition key in the table.
   * @param table the table
   * @param partitionKey the partition key of the tablet to look up in the table
   * @param lookupType the type of lookup to use
   * @param deadline deadline in milliseconds for this lookup to finish
   * @return a deferred containing the located tablet
   */
  Deferred<LocatedTablet> getTabletLocation(final KuduTable table,
                                            final byte[] partitionKey,
                                            final LookupType lookupType,
                                            long timeout) {

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

    final TimeoutTracker timeoutTracker = new TimeoutTracker();
    timeoutTracker.setTimeout(timeout);
    Deferred<List<LocatedTablet>> locatedTablets = locateTable(
        table, startPartitionKey, endPartitionKey, FETCH_TABLETS_PER_POINT_LOOKUP, timeout);

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
                if (lookupType == LookupType.POINT
                    || entry.getUpperBoundPartitionKey().length == 0) {
                  return Deferred.fromError(
                      new NonCoveredRangeException(entry.getLowerBoundPartitionKey(),
                          entry.getUpperBoundPartitionKey()));
                }
                // This is a LOWER_BOUND lookup, get the tablet location from the upper bound key
                // of the non-covered range to return the next valid tablet location.
                return getTabletLocation(table, entry.getUpperBoundPartitionKey(),
                    LookupType.POINT, timeoutTracker.getMillisBeforeTimeout());
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
      @Override
      public ArrayList<Void> call(final ArrayList<Void> arg) {
        LOG.debug("Releasing all remaining resources");
        timer.stop();
        new ShutdownThread().start();
        return arg;
      }

      @Override
      public String toString() {
        return "release resources callback";
      }
    }

    // 2. Terminate all connections.
    final class DisconnectCB implements Callback<Deferred<ArrayList<Void>>,
        ArrayList<List<OperationResponse>>> {
      @Override
      public Deferred<ArrayList<Void>> call(ArrayList<List<OperationResponse>> ignoredResponses) {
        return connectionCache.disconnectEverything().addCallback(new ReleaseResourcesCB());
      }

      @Override
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
    if (copyOfSessions.isEmpty()) {
      return Deferred.fromResult(null);
    }
    // Guaranteed that we'll have at least one session to close.
    List<Deferred<List<OperationResponse>>> deferreds = new ArrayList<>(copyOfSessions.size());
    for (AsyncKuduSession session : copyOfSessions ) {
      deferreds.add(session.close());
    }

    return Deferred.group(deferreds);
  }

  @SuppressWarnings("ReferenceEquality")
  private static boolean isMasterTable(String tableId) {
    // Checking that it's the same instance so there's absolutely no chance of confusing the master
    // 'table' for a user one.
    return MASTER_TABLE_NAME_PLACEHOLDER == tableId;
  }

  /**
   * Utility function to register a timeout task 'task' on timer 'timer' that
   * will fire after 'timeoutMillis' milliseconds. Returns a handle to the
   * scheduled timeout, which can be used to cancel the task and release its
   * resources.
   * @param timer the timer on which the task is scheduled
   * @param task the task that will be run when the timeout hits
   * @param timeoutMillis the timeout, in milliseconds
   * @return a handle to the scheduled timeout
   */
  static Timeout newTimeout(final Timer timer,
                            final TimerTask task,
                            final long timeoutMillis) {
    Preconditions.checkNotNull(timer);
    try {
      return timer.newTimeout(task, timeoutMillis, MILLISECONDS);
    } catch (IllegalStateException e) {
      // This can happen if the timer fires just before shutdown()
      // is called from another thread, and due to how threads get
      // scheduled we tried to call newTimeout() after timer.stop().
      LOG.warn("Failed to schedule timer. Ignore this if we're shutting down.", e);
    }
    return null;
  }

  /**
   * @return copy of the current TabletClients list
   */
  @InterfaceAudience.LimitedPrivate("Test")
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

    private final HashedWheelTimer timer =
        new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).build(), 20, MILLISECONDS);
    private Executor bossExecutor;
    private Executor workerExecutor;
    private int bossCount = DEFAULT_BOSS_COUNT;
    private int workerCount = DEFAULT_WORKER_COUNT;
    private boolean statisticsDisabled = false;

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
     * Socket read timeouts are no longer used in the Java client and have no effect.
     * Setting this has no effect.
     * @param timeoutMs a timeout in milliseconds
     * @return this builder
     * @deprecated this option no longer has any effect
     */
    @Deprecated public AsyncKuduClientBuilder defaultSocketReadTimeoutMs(long timeoutMs) {
      LOG.info("defaultSocketReadTimeoutMs is deprecated");
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
     * If not provided, (2 * the number of available processors) is used. If
     * this client instance will be used on a machine running many client
     * instances, it may be wise to lower this count, for example to avoid
     * resource limits, at the possible cost of some performance of this client
     * instance.
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
      return new AsyncKuduClient(this);
    }
  }
}
