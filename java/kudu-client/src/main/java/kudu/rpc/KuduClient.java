/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 * Portions copyright (c) 2014 Cloudera, Inc.
 * Confidential Cloudera Information: Covered by NDA.
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

package kudu.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.master.Master;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.metadata.Metadata;
import kudu.util.NetUtil;
import kudu.util.Slice;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static kudu.rpc.ExternalConsistencyMode.CLIENT_PROPAGATED;
import static kudu.rpc.KuduScanner.ReadMode.READ_AT_SNAPSHOT;

/**
 * A fully asynchronous, thread-safe, modern Kudu client.
 * <p>
 * This client should be
 * instantiated only once. You can use it with any number of tables at the
 * same time. The only case where you should have multiple instances is when
 * you want to use multiple different clusters at the same time.
 * <p>
 * If you play by the rules, this client is (in theory {@code :D}) completely
 * thread-safe.  Read the documentation carefully to know what the requirements
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
public class KuduClient {

  public static final Logger LOG = LoggerFactory.getLogger(KuduClient.class);
  public static final int SLEEP_TIME = 500;
  public static final byte[] EMPTY_ARRAY = new byte[0];
  public static final int NO_TIMESTAMP = -1;

  private final ClientSocketChannelFactory channelFactory;

  /**
   * This map and the next 2 maps contain the same data, but indexed
   * differently. There is no consistency guarantee across the maps.
   * They are not updated all at the same time atomically.  This map
   * is always the first to be updated, because that's the map from
   * which all the lookups are done in the fast-path of the requests
   * that need to locate a tablet. The second map to be updated is
   * tablet2client, because it comes second in the fast-path
   * of every requests that need to locate a tablet. The third map
   * is only used to handle TabletServer disconnections gracefully.
   */
  private final ConcurrentHashMap<String, ConcurrentSkipListMap<byte[],
      RemoteTablet>> tabletsCache = new ConcurrentHashMap<String, ConcurrentSkipListMap<byte[],
      RemoteTablet>>();

  /**
   * Maps a tablet ID to the to the RemoteTablet that knows where all the replicas are served
   */
  private final ConcurrentHashMap<Slice, RemoteTablet> tablet2client =
      new ConcurrentHashMap<Slice, RemoteTablet>();

  /**
   * Maps a client connected to a TabletServer to the list of tablets we know
   * it's serving so far.
   */
  private final ConcurrentHashMap<TabletClient, ArrayList<RemoteTablet>> client2tablets =
      new ConcurrentHashMap<TabletClient, ArrayList<RemoteTablet>>();

  /**
   * Cache that maps a TabletServer address ("ip:port") to the clients
   * connected to it.
   * <p>
   * Access to this map must be synchronized by locking its monitor.
   * Lock ordering: when locking both this map and a TabletClient, the
   * TabletClient must always be locked first to avoid deadlocks.  Logging
   * the contents of this map (or calling toString) requires copying it first.
   * <p>
   * This isn't a {@link ConcurrentHashMap} because we don't use it frequently
   * (just when connecting to / disconnecting from TabletClients) and when we
   * add something to it, we want to do an atomic get-and-put, but
   * {@code putIfAbsent} isn't a good fit for us since it requires to create
   * an object that may be "wasted" in case another thread wins the insertion
   * race, and we don't want to create unnecessary connections.
   * <p>
   * Upon disconnection, clients are automatically removed from this map.
   * We don't use a {@code ChannelGroup} because a {@code ChannelGroup} does
   * the clean-up on the {@code channelClosed} event, which is actually the
   * 3rd and last event to be fired when a channel gets disconnected.  The
   * first one to get fired is, {@code channelDisconnected}.  This matters to
   * us because we want to purge disconnected clients from the cache as
   * quickly as possible after the disconnection, to avoid handing out clients
   * that are going to cause unnecessary errors.
   * @see TabletClientPipeline#handleDisconnect
   */
  private final HashMap<String, TabletClient> ip2client =
      new HashMap<String, TabletClient>();

  @GuardedBy("sessions")
  private final Set<KuduSession> sessions = new HashSet<KuduSession>();

  // TODO Below is an uber hack, the master is considered a tablet with
  // a table name.
  static final String MASTER_TABLE_HACK =  "~~~masterTableHack~~~";
  final KuduTable masterTableHack;
  private final List<HostAndPort> masterAddresses;
  // END OF HACK

  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  /**
   * Timestamp required for HybridTime external consistency through timestamp
   * propagation.
   * @see src/kudu/common/common.proto
   */
  private long lastPropagatedTimestamp = NO_TIMESTAMP;

  // A table is considered not served when we get an empty list of locations but know
  // that a tablet exists. This is currently only used for new tables.
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

  private volatile boolean closed;

  /**
   * Create a new client that connects to masters specified by a comma-separated
   * list.
   * Doesn't block and won't throw an exception if the masters don't exist.
   * @param masterQuorum Comma-separated list of "host:port" pairs of the masters
   */
  public KuduClient(final String masterQuorum) {
    this(NetUtil.parseStrings(masterQuorum, 0));
  }

  public KuduClient(final List<HostAndPort> masterAddresses) {
    this(masterAddresses, defaultChannelFactory());
  }

  /**
   * Create a new client that connects to the specified masters.
   * Doesn't block and won't throw an exception if the masters don't exist.
   * @param masterAddresses masters' addresses
   * @param channelFactory socket channel factory for this client; can be
   *                       configured to specify a custom connection timeout
   */
  public KuduClient(final List<HostAndPort> masterAddresses,
                    final ClientSocketChannelFactory channelFactory) {
    this.channelFactory = channelFactory;
    this.masterAddresses = masterAddresses;
    this.masterTableHack = new KuduTable(this, MASTER_TABLE_HACK,
       new Schema(new ArrayList<ColumnSchema>(0)));
  }

  /**
   * Updates the last timestamp received from a server. Used for CLIENT_PROPAGATED
   * external consistency. This is only publicly visible so that it can be set
   * on tests, users should generally disregard this method.
   *
   * @param lastPropagatedTimestamp the last timestamp received from a server
   */
  @VisibleForTesting
  public synchronized void updateLastPropagatedTimestamp(long lastPropagatedTimestamp) {
    if (this.lastPropagatedTimestamp == -1 ||
      this.lastPropagatedTimestamp < lastPropagatedTimestamp) {
      this.lastPropagatedTimestamp = lastPropagatedTimestamp;
    }
  }

  @VisibleForTesting
  public synchronized long getLastPropagatedTimestamp() {
    return lastPropagatedTimestamp;
  }

  /**
   * Create a table on the cluster with the specified name and schema. Default table
   * configurations are used, mainly the tablet will have one tablet.
   * @param name Table's name
   * @param schema Table's schema
   * @return Deferred object to track the progress
   */
  public Deferred<CreateTableResponse> createTable(String name, Schema schema) {
    return this.createTable(name, schema, new CreateTableBuilder());
  }

  /**
   * Create a table on the cluster with the specified name, schema, and table configurations
   * @param name Table's name
   * @param schema Table's schema
   * @param builder Table's configurations
   * @return Deferred object to track the progress
   */
  public Deferred<CreateTableResponse> createTable(String name, Schema schema, CreateTableBuilder builder) {
    checkIsClosed();
    if (builder == null) {
      builder = new CreateTableBuilder();
    }
    CreateTableRequest create = new CreateTableRequest(this.masterTableHack, name, schema,
        builder);
    return sendRpcToTablet(create);
  }

  /**
   * Delete a tablet on the cluster with the specified name
   * @param name Table's name
   * @return Deferred object to track the progress
   */
  public Deferred<DeleteTableResponse> deleteTable(String name) {
    checkIsClosed();
    DeleteTableRequest delete = new DeleteTableRequest(this.masterTableHack, name);
    return sendRpcToTablet(delete);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   * @param name Table's name, if this is a table rename then the old table name must be passed
   * @param atb The information for the alter command
   * @return Deferred object to track the progress of the alter command,
   * but it's completion only means that the master received the request. Use
   * syncWaitOnAlterCompletion() to know when the alter completes.
   */
  public Deferred<AlterTableResponse> alterTable(String name, AlterTableBuilder atb) {
    checkIsClosed();
    AlterTableRequest alter = new AlterTableRequest(this.masterTableHack, name, atb);
    return sendRpcToTablet(alter);
  }

  /**
   * Helper method that checks and waits until the completion of an alter command.
   * It will block until the alter command is done or the deadline is reached.
   * @param name Table's name, if the table was renamed then that name must be checked against
   * @param deadline For how many milliseconds this method can block
   * @return
   */
  public boolean syncWaitOnAlterCompletion(String name, long deadline) throws Exception {
    checkIsClosed();
    if (deadline < SLEEP_TIME) {
      throw new IllegalArgumentException("deadline must be at least " + SLEEP_TIME + "ms");
    }
    long totalSleepTime = 0;
    while (totalSleepTime < deadline) {
      long start = System.currentTimeMillis();

      // Send RPC, wait for reponse, process it
      Deferred<Master.IsAlterTableDoneResponsePB> d =
          sendRpcToTablet(new IsAlterTableDoneRequest(this.masterTableHack, name));
      Master.IsAlterTableDoneResponsePB response;
      try {
        response = d.join(SLEEP_TIME);
      } catch (Exception ex) {
        throw ex;
      }

      if (response.getDone()) {
        return true;
      }

      // Count time that was slept and see if we need to wait a little more
      long elapsed = System.currentTimeMillis() - start;
      // Don't oversleep the deadline
      if (totalSleepTime + SLEEP_TIME > deadline) {
        return false;
      }
      // elapsed can be bigger if we slept about 500ms
      if (elapsed <= SLEEP_TIME) {
        LOG.debug("Alter not done, sleep " + (SLEEP_TIME - elapsed) + " and slept " +
            totalSleepTime);
        Thread.sleep(SLEEP_TIME - elapsed);
        totalSleepTime += SLEEP_TIME;
      } else {
        totalSleepTime += elapsed;
      }
    }
    return false;
  }

  /**
   * Get the count of running tablet servers
   * @return an int, the count
   */
  public Deferred<ListTabletServersResponse> listTabletServers() {
    checkIsClosed();
    ListTabletServersRequest rpc = new ListTabletServersRequest(this.masterTableHack);
    return sendRpcToTablet(rpc);
  }

  Deferred<GetTableSchemaResponse> getTableSchema(String name) {
    return sendRpcToTablet(new GetTableSchemaRequest(this.masterTableHack, name));
  }

  /**
   * Get the list of all the tables.
   * @return a list of all the tables
   */
  public Deferred<ListTablesResponse> getTablesList() {
    return getTablesList(null);
  }

  /**
   * Get a list of table names. Passing a null filter returns all the tables. When a filter is
   * specified, it only returns tables that satisfy a substring match.
   * @param nameFilter an optional table name filter
   * @return a deferred that contains the list of table names
   */
  public Deferred<ListTablesResponse> getTablesList(String nameFilter) {
    ListTablesRequest rpc = new ListTablesRequest(this.masterTableHack, nameFilter);
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
   * @param name table to open
   * @return a KuduTable if the table exists, else a MasterErrorException
   */
  public Deferred<KuduTable> openTable(final String name) {
    checkIsClosed();
    return getTableSchema(name).addCallback(new Callback<KuduTable, GetTableSchemaResponse>() {
      @Override
      public KuduTable call(GetTableSchemaResponse response) throws Exception {
        return new KuduTable(KuduClient.this, name, response.getSchema());
      }
    });
  }

  /**
   * Creates a new {@link KuduScanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * The scanner will be created with ReadMode.READ_LATEST
   * @return A new scanner for this table.
   * @see src/kudu/common/common.proto for info on ReadModes
   */
  public KuduScanner newScanner(final KuduTable table, Schema schema) {
    checkIsClosed();
    return new KuduScanner(this, table, schema);
  }

  /**
   * Like the above but creates a snapshot scanner. Snapshot scanners
   * are required for externally consistent reads. In this overload the server
   * will assign the snapshot timestamp.
   *
   * @see src/kudu/common/common.proto for info on ReadModes and snapshot scanners
   */
  public KuduScanner newSnapshotScanner(final KuduTable table, Schema schema) {
    checkIsClosed();
    return new KuduScanner(this, table, schema, READ_AT_SNAPSHOT);
  }

  /**
   * Like the above, also creates a Snapshot scanner, but allows to specify
   * the snapshot timestamp.
   *
   * @see src/kudu/common/common.proto for info on ReadModes and snapshot scanners
   */
  public KuduScanner newSnapshotScanner(final KuduTable table,
    Schema schema, long timestamp, TimeUnit timeUnit) {
    checkIsClosed();
    return new KuduScanner(this, table, schema, READ_AT_SNAPSHOT);
  }


  /**
   * Package-private access point for {@link KuduScanner}s to open themselves.
   * @param scanner The scanner to open.
   * @return A deferred {@link KuduScanner.Response}
   */
  Deferred<KuduScanner.Response> openScanner(final KuduScanner scanner) {
    return sendRpcToTablet(scanner.getOpenRequest()).addErrback(
        new Callback<Exception, Exception>() {
          public Exception call(final Exception e) {
            String message = "Cannot openScanner because: ";
            LOG.warn(message, e);
            // Don't let the scanner think it's opened on this tablet.
            scanner.invalidate();
            return e;  // Let the error propagate.
          }
          public String toString() {
            return "openScanner errback";
          }
        });
  }

  /**
   * Create a new session for interacting with the cluster.
   * User is responsible for destroying the session object.
   * This is a fully local operation (no RPCs or blocking).
   * @return a new KuduSession
   */
  public KuduSession newSession() {
    checkIsClosed();
    KuduSession session = new KuduSession(this);
    synchronized (sessions) {
      sessions.add(session);
    }
    return session;
  }

  /**
   * Same as {@link #newSession} but returns a synchronous version of {@link KuduSession},
   * see {@link kudu.rpc.SynchronousKuduSession}.
   * @return A synchronous wrapper around KuduSession.
   */
  public SynchronousKuduSession newSynchronousSession() {
    KuduSession session = newSession();
    return new SynchronousKuduSession(session);
  }

  /**
   * This method is for KuduSessions so that they can remove themselves as part of closing down.
   * @param session Session to remove
   */
  void removeSession(KuduSession session) {
    synchronized (sessions) {
      boolean removed = sessions.remove(session);
      assert removed == true;
    }
  }

  /**
   * Package-private access point for {@link KuduScanner}s to scan more rows.
   * @param scanner The scanner to use.
   * @return A deferred row.
   */
  Deferred<KuduScanner.Response> scanNextRows(final KuduScanner scanner) {
    if (scanner.timedOut()) {
      Exception e = new NonRecoverableException("Time out:" + scanner);
      return Deferred.fromError(e);
    }
    final RemoteTablet tablet = scanner.currentTablet();
    final TabletClient client = clientFor(tablet);
    final KuduRpc<KuduScanner.Response> next_request = scanner.getNextRowsRequest();
    final Deferred<KuduScanner.Response> d = next_request.getDeferred();
    if (client == null) {
      // Oops, we no longer know anything about this client or tabletSlice.  Our
      // cache was probably invalidated while the client was scanning.  This
      // means that we lost the connection to that TabletServer, so we have to
      // try to re-connect and check if the scanner is still good.
      return sendRpcToTablet(next_request);
    }
    client.sendRpc(next_request);
    return d;
  }

  /**
   * Package-private access point for {@link KuduScanner}s to close themselves.
   * @param scanner The scanner to close.
   * @return A deferred object that indicates the completion of the request.
   * The {@link KuduScanner.Response} can contain rows that were left to scan.
   */
  Deferred<KuduScanner.Response> closeScanner(final KuduScanner scanner) {
    final RemoteTablet tablet = scanner.currentTablet();
    final TabletClient client = clientFor(tablet);
    if (client == null) {
      // Oops, we no longer know anything about this client or tabletSlice.  Our
      // cache was probably invalidated while the client was scanning.  So
      // we can't close this scanner properly.
      LOG.warn("Cannot close " + scanner + " properly, no connection open for "
          + (tablet == null ? null : tablet));
      return Deferred.fromResult(null);
    }
    final KuduRpc<KuduScanner.Response>  close_request = scanner.getCloseRequest();
    final Deferred<KuduScanner.Response> d = close_request.getDeferred();
    client.sendRpc(close_request);
    return d;
  }

  <R> Deferred<R> sendRpcToTablet(final KuduRpc<R> request) {
    if (cannotRetryRequest(request)) {
      return tooManyAttemptsOrTimeout(request, null);
    }
    request.attempt++;
    final String tableName = request.getTable().getName();
    byte[] rowkey = null;
    if (request instanceof KuduRpc.HasKey) {
       rowkey = ((KuduRpc.HasKey)request).key();
    }
    final RemoteTablet tablet = getTablet(tableName, rowkey);

    // Set the propagated timestamp so that the next time we send a message to
    // the server the message includes the last propagated timestamp.
    long lastPropagatedTs = getLastPropagatedTimestamp();
    if (request.getExternalConsistencyMode() == CLIENT_PROPAGATED &&
      lastPropagatedTs != NO_TIMESTAMP) {
      request.setPropagatedTimestamp(lastPropagatedTs);
    }

    if (tablet != null) {
      TabletClient tabletClient = clientFor(tablet);
      if (tabletClient != null) {
        request.setTablet(tablet);
        final Deferred<R> d = request.getDeferred();
        tabletClient.sendRpc(request);
        return d;
      }
    }

    // Right after creating a table a request will fall into locateTablet since we don't know yet
    // if the table is ready or not. If discoverTablets() didn't get any tablets back,
    // then on retry we'll fall into the following block. It will sleep, then call the master to
    // see if the table was created. We'll spin like this until the table is created and then
    // we'll try to locate the tablet again.
    if (tablesNotServed.contains(tableName)) {
      return delayedIsCreateTableDone(tableName, request,
          new RetryRpcCB<R, Master.IsCreateTableDoneResponsePB>(request),
          getDelayedIsCreateTableDoneErrback(request));
    }
    Callback<Deferred<R>, Master.GetTableLocationsResponsePB> cb = new RetryRpcCB<R,
        Master.GetTableLocationsResponsePB>(request);
    Deferred<Master.GetTableLocationsResponsePB> returnedD = locateTablet(tableName, rowkey);
    // not adding an errback, if we failed locating the tablet we just throw
    return returnedD.addCallbackDeferring(cb);
  }

  /**
   * Callback used to retry a RPC after another query finished, like looking up where that RPC
   * should go.
   * @param <R> RPC's return type.
   * @param <D> Previous query's return type, which we don't use, but need to specify in order to
   *           tie it all together.
   */
  final class RetryRpcCB<R, D> implements Callback<Deferred<R>, D> {
    final KuduRpc<R> request;
    RetryRpcCB(KuduRpc<R> request) {
      this.request = request;
    }
    public Deferred<R> call(final D arg) {
      return sendRpcToTablet(request);  // Retry the RPC.
    }
    public String toString() {
      return "retry RPC";
    }
  }

  /**
   * This errback ensures that if the delayed call to IsCreateTableDone throws an Exception that
   * it will be propagated back to the user.
   * @param request Request to errback if there's a problem with the delayed call.
   * @param <R> Request's return type.
   * @return An errback.
   */
  <R> Callback<Exception, Exception> getDelayedIsCreateTableDoneErrback(final KuduRpc<R> request) {
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
   * @param tableName Table to lookup.
   * @param rpc Original KuduRpc that needs to access the table.
   * @param retryCB Callback to call on completion.
   * @param errback Errback to call if something goes wrong when calling IsCreateTableDone.
   * @return Deferred used to track the provided KuduRpc
   */
  <R> Deferred<R> delayedIsCreateTableDone(final String tableName, final KuduRpc<R> rpc,
                                           final Callback<Deferred<R>,
                                               Master.IsCreateTableDoneResponsePB> retryCB,
                                           final Callback<Exception, Exception> errback) {

    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        final boolean has_permit = acquireMasterLookupPermit();
        if (!has_permit) {
          // If we failed to acquire a permit, it's worth checking if someone
          // looked up the tablet we're interested in.  Every once in a while
          // this will save us a Master lookup.
          if (!tablesNotServed.contains(tableName)) {
            try {
              retryCB.call(null);
              return;
            } catch (Exception e) {
              // we're calling RetryRpcCB which doesn't throw exceptions, ignore
            }
          }
        }
        IsCreateTableDoneRequest rpc = new IsCreateTableDoneRequest(masterTableHack, tableName);
        final Deferred<Master.IsCreateTableDoneResponsePB> d =
            sendRpcToTablet(rpc).addCallback(new IsCreateTableDoneCB(tableName));
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
    final String table;
    IsCreateTableDoneCB(String table) {
      this.table = table;
    }
    public Master.IsCreateTableDoneResponsePB call(final Master.IsCreateTableDoneResponsePB response) {
      if (response.getDone()) {
        LOG.debug("Table " + table + " was created");
        tablesNotServed.remove(table);
      } else {
        LOG.debug("Table " + table + " is still being created");
      }
      return response;
    }
    public String toString() {
      return "ask the master if " + table + " was created";
    }
  };

  boolean isTableNotServed(String tableName) {
    return tablesNotServed.contains(tableName);
  }


  long getSleepTimeForRpc(KuduRpc<?> rpc) {
    // TODO backoffs? Sleep in increments of 500 ms, plus some random time up to 50
    long sleepTime = (rpc.attempt * SLEEP_TIME) + sleepRandomizer.nextInt(50);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to sleep for " + sleepTime + " at retry " + rpc.attempt);
    }
    return sleepTime;
  }

  /**
   * Modifying the list returned by this method won't change how KuduClient behaves,
   * but calling certain methods on the returned TabletClients can. For example,
   * it's possible to forcefully shutdown a connection to a tablet server by calling {@link
   * TabletClient#shutdown()}.
   * @return Copy of the current TabletClients list
   */
  @VisibleForTesting
  List<TabletClient> getTableClients() {
    synchronized (ip2client) {
      return new ArrayList<TabletClient>(ip2client.values());
    }
  }

  /**
   * This method first clears tabletsCache and then tablet2client without any regards for
   * calls to {@link #discoverTablets(String, kudu.master.Master.GetTableLocationsResponsePB)}.
   * Call only when KuduClient is in a steady state.
   * @param tableName Table for which we remove all the RemoteTablet entries
   */
  @VisibleForTesting
  void emptyTabletsCacheForTable(String tableName) {
    tabletsCache.remove(tableName);
    Set<Map.Entry<Slice, RemoteTablet>> tablets = tablet2client.entrySet();
    for (Map.Entry<Slice, RemoteTablet> entry : tablets) {
      if (entry.getValue().getTable().equals(tableName)) {
        tablets.remove(entry);
      }
    }
  }

  TabletClient clientFor(RemoteTablet tablet) {
    if (tablet == null) {
      return null;
    }

    synchronized (tablet.tabletServers) {
      if (tablet.tabletServers.isEmpty()) {
        return null;
      }
      if (tablet.leaderIndex == RemoteTablet.NO_LEADER_INDEX) {
        // TODO we don't know where the leader is, either because one wasn't provided or because
        // we couldn't resolve its IP. We'll just send the client back so it retries and probably
        // dies after too many attempts.
        return null;
      } else {
        // TODO we currently always hit the leader, we probably don't need to except for writes
        // and some reads.
        return tablet.tabletServers.get(tablet.leaderIndex);
      }
    }
  }

  /**
   * Checks whether or not an RPC can be retried once more.
   * @param rpc The RPC we're going to attempt to execute.
   * @return {@code true} if this RPC already had too many attempts,
   * {@code false} otherwise (in which case it's OK to retry once more).
   * @throws NonRecoverableException if the request has had too many attempts
   * already.
   */
  static boolean cannotRetryRequest(final KuduRpc<?> rpc) {
    return rpc.deadlineTracker.timedOut() || rpc.attempt > 100;  // TODO Don't hardcode.
  }

  /**
   * Returns a {@link Deferred} containing an exception when an RPC couldn't
   * succeed after too many attempts or if it already timed out.
   * @param request The RPC that was retried too many times or timed out.
   * @param cause What was cause of the last failed attempt, if known.
   * You can pass {@code null} if the cause is unknown.
   */
  static <R> Deferred<R> tooManyAttemptsOrTimeout(final KuduRpc<R> request,
                                                  final KuduException cause) {
    String message;
    if (request.deadlineTracker.timedOut()) {
      message = "Time out: ";
    } else {
      message = "Too many attempts: ";
    }
    final Exception e = new NonRecoverableException(message + request, cause);
    request.errback(e);
    return Deferred.fromError(e);
  }

  /**
   * Sends a getTableLocations RPC to the master to find the table's tablets
   * @param table table's name, in the future this will use a tablet id
   * @param rowkey can be null, if not we'll find the exact tablet that contains it
   * @return Deferred to track the progress
   */
  Deferred<Master.GetTableLocationsResponsePB> locateTablet(String table, byte[] rowkey) {
    final boolean has_permit = acquireMasterLookupPermit();
    if (!has_permit) {
      // If we failed to acquire a permit, it's worth checking if someone
      // looked up the tablet we're interested in.  Every once in a while
      // this will save us a Master lookup.
      RemoteTablet tablet = getTablet(table, rowkey);
      if (tablet != null && clientFor(tablet) != null) {
        return Deferred.fromResult(null);  // Looks like no lookup needed.
      }
    }
    GetTableLocationsRequest rpc = new GetTableLocationsRequest(masterTableHack, rowkey,
        rowkey, table);
    final Deferred<Master.GetTableLocationsResponsePB> d;

    // If we know this is going to the master, check the master quorum (as specified by
    // 'masterAddresses' field) to determine and cache the current leader.
    if (isMasterTable(table)) {
      d = getMasterTableLocationsPB();
    } else {
      d = sendRpcToTablet(rpc);
    }
    d.addCallback(new MasterLookupCB(table));
    if (has_permit) {
      d.addBoth(new ReleaseMasterLookupPermit<Master.GetTableLocationsResponsePB>());
    }
    return d;
  }

  /**
   * Update the master quorum: send RPCs to all quorum members, use the returned data to
   * fill a {@link kudu.master.Master.GetTableLocationsResponsePB} object.
   * @return A Deferred object for the most recent configuration for the master quorum.
   */
  Deferred<Master.GetTableLocationsResponsePB> getMasterTableLocationsPB() {
    ArrayList<Deferred<GetMasterRegistrationResponse>> deferreds =
        Lists.newArrayListWithCapacity(masterAddresses.size());
    for (HostAndPort hostAndPort : masterAddresses) {

      Deferred<GetMasterRegistrationResponse> d;
      try {
        // Note: we need to create a client for that host first, as there's a
        // chicken and egg problem: since there is no source of truth beyond
        // the master, the only way to get information about a master host is
        // by making an RPC to that host.
        TabletClient clientForHostAndPort = newMasterClient(hostAndPort);
        d = getMasterRegistration(clientForHostAndPort);
      } catch (Exception e) {
        LOG.warn("Error creating a TabletClient for " + hostAndPort.toString(), e);
        d = Deferred.fromError(e);
      }
      d = d.addCallbacks(new GetMasterRegistrationCB<GetMasterRegistrationResponse>(hostAndPort),
          new GetMasterRegistrationCB<Exception>(hostAndPort));
      deferreds.add(d);
    }
    // TODO: Don't use group: pass a Deferred<GetTableLocationsResponsePB> in to this
    // method, pass it into to GetMasterRegistrationCB's constructor, and then have
    // GetMasterRegistrationCB set the deferred once a leader has been found (or return
    // an exception if we received responses/exception from all of the masters, but no
    // leader was found).
    return Deferred.group(deferreds).addCallback(new GetMasterRegistrationGroupCB());
  }

  /**
   * Pass through callback to allow grouping deferred GetMasterRegistrationResponses:
   * if there's an error in a response, returns a null.
   * @param <T> Either an exception type or GetMasterRegistrationResponse
   */
  final class GetMasterRegistrationCB<T> implements Callback<GetMasterRegistrationResponse, T> {
    final HostAndPort hostAndPort;

    public GetMasterRegistrationCB(HostAndPort hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    @Override
    public GetMasterRegistrationResponse call(T arg) throws Exception {
      if (arg instanceof Exception) {
        Exception e = (Exception) arg;
        LOG.warn("Encountered an error getting registration from " + hostAndPort, e);
        return null;
      } else {
        return (GetMasterRegistrationResponse) arg;
      }
    }

    @Override
    public String toString() {
      return "get master registration for " + hostAndPort.toString();
    }
  }

  /**
   * Converts a list of {@link kudu.rpc.GetMasterRegistrationResponse} objects to a
   * single {@link kudu.master.Master.GetTableLocationsRequestPB} object containing all
   * of the master replicas.
   */
  final class GetMasterRegistrationGroupCB implements Callback<Master.GetTableLocationsResponsePB,
      ArrayList<GetMasterRegistrationResponse>> {

    @Override
    public Master.GetTableLocationsResponsePB call(
        ArrayList<GetMasterRegistrationResponse> responses) throws Exception {
      Master.TabletLocationsPB.Builder locationBuilder = Master.TabletLocationsPB.newBuilder();
      locationBuilder.setStartKey(ByteString.copyFromUtf8(""));
      locationBuilder.setEndKey(ByteString.copyFromUtf8(""));
      locationBuilder.setTabletId(ByteString.copyFromUtf8(MASTER_TABLE_HACK));
      locationBuilder.setStale(false);
      boolean leaderFound = false;
      for (GetMasterRegistrationResponse r : responses) {
        if (r == null) {
          continue;
        }
        Master.TabletLocationsPB.ReplicaPB.Builder replicaBuilder = Master.TabletLocationsPB
            .ReplicaPB.newBuilder();
        replicaBuilder.setRole(r.getRole());
        Master.TSInfoPB.Builder tsInfoBuilder = Master.TSInfoPB.newBuilder();
        tsInfoBuilder.addRpcAddresses(r.getServerRegistration().getRpcAddresses(0));
        tsInfoBuilder.setPermanentUuid(r.getInstanceId().getPermanentUuid());
        replicaBuilder.setTsInfo(tsInfoBuilder);
        if (r.getRole().equals(Metadata.QuorumPeerPB.Role.LEADER)) {
          leaderFound = true;
          locationBuilder.addReplicas(0, replicaBuilder);
        } else {
          locationBuilder.addReplicas(replicaBuilder);
        }
      }
      if (!leaderFound) {
        LOG.info("No leader master found among responses: " + responses.toString());
      }
      return Master.GetTableLocationsResponsePB.newBuilder().addTabletLocations(
          locationBuilder.build()).build();
    }

    public String toString() {
      return "get registration information for all masters.";
    }
  }

  /**
   * Get all or some tablets for a given table. This may query the master multiple times if there
   * are a lot of tablets.
   * This method blocks until it gets all the tablets.
   * @param table Table name
   * @param startKey where to start in the table, pass null to start at the beginning
   * @param endKey where to stop in the table, pass null to get all the tablets until the end of
   *               the table
   * @param deadline deadline in milliseconds for this method to finish
   * @return a list of the tablets in the table, which can be queried for metadata about
   *         each tablet
   * @throws Exception MasterErrorException if the table doesn't exist
   */
  List<LocatedTablet> syncLocateTable(String table,
                                      byte[] startKey,
                                      byte[] endKey,
                                      long deadline) throws Exception {
    List<LocatedTablet> ret = Lists.newArrayList();
    byte[] lastEndKey = null;

    DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(deadline);
    while (true) {
      if (deadlineTracker.timedOut()) {
        throw new NonRecoverableException("Took too long getting the list of tablets, " +
            "deadline=" + deadline);
      }
      GetTableLocationsRequest rpc = new GetTableLocationsRequest(masterTableHack, startKey,
          endKey, table);
      final Deferred<Master.GetTableLocationsResponsePB> d = sendRpcToTablet(rpc);
      Master.GetTableLocationsResponsePB response =
          d.join(deadlineTracker.getMillisBeforeDeadline());
      // Table doesn't exist or is being created.
      if (response.getTabletLocationsCount() == 0) {
        break;
      }
      for (Master.TabletLocationsPB tabletPb : response.getTabletLocationsList()) {
        LocatedTablet locs = new LocatedTablet(tabletPb);
        ret.add(locs);

        if (lastEndKey != null &&
            locs.getEndKey().length != 0 &&
            Bytes.memcmp(locs.getEndKey(), lastEndKey) < 0) {
          throw new IllegalStateException(
            "Server returned tablets out of order: " +
            "end key '" + Bytes.pretty(locs.getEndKey()) + "' followed " +
            "end key '" + Bytes.pretty(lastEndKey) + "'");
        }
        lastEndKey = locs.getEndKey();
      }
      // If true, we're done, else we have to go back to the master with the last end key
      if (lastEndKey.length == 0 ||
          (endKey != null && Bytes.memcmp(lastEndKey, endKey) > 0)) {
        break;
      } else {
        startKey = lastEndKey;
      }
    }
    return ret;
  }

  /**
   * We're handling a tablet server that's telling us it doesn't have the tablet we're asking for.
   * We're in the context of decode() meaning we need to either callback or retry later.
   */
  <R> void handleTabletNotFound(final KuduRpc<R> rpc, KuduException ex, TabletClient server) {
    invalidateTabletCache(rpc.getTablet(), server);
    handleRetryableError(rpc, ex);
  }

  /**
   * A tablet server is letting us know that it isn't the specified tablet's leader in response
   * a RPC, so we need to demote it and retry.
   */
  <R> void handleNotLeader(final KuduRpc<R> rpc, KuduException ex, TabletClient server) {
    rpc.getTablet().demoteLeader(server);
    handleRetryableError(rpc, ex);
  }

  <R> void handleRetryableError(final KuduRpc<R> rpc, KuduException ex) {
    // TODO we don't always need to sleep, maybe another replica can serve this RPC

    boolean cannotRetry = cannotRetryRequest(rpc);
    if (cannotRetry) {
      // This is a callback
      tooManyAttemptsOrTimeout(rpc, ex);
    }

    delayedSendRpcToTablet(rpc, ex);
  }

  private <R> void delayedSendRpcToTablet(final KuduRpc<R> rpc, KuduException ex) {
    // Here we simply retry the RPC later. We might be doing this along with a lot of other RPCs
    // in parallel. Asynchbase does some hacking with a "probe" RPC while putting the other ones
    // on hold but we won't be doing this for the moment. Regions in HBase can move a lot,
    // we're not expecting this in Kudu.
    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        sendRpcToTablet(rpc);
      }
    }
    long sleepTime = getSleepTimeForRpc(rpc);
    if (rpc.deadlineTracker.wouldSleepingTimeout(sleepTime)) {
      tooManyAttemptsOrTimeout(rpc, ex);
    }
    newTimeout(new RetryTimer(), sleepTime);
  }

  /**
   * Remove the tablet server from the RemoteTablet's locations. Right now nothing is removing
   * the tablet itself from the caches.
   */
  private void invalidateTabletCache(RemoteTablet tablet, TabletClient server) {
    LOG.info("Removing a tablet server location from " +
        tablet.getTabletIdAsString());
    tablet.removeTabletServer(server);
  }

  /** Callback executed when a master lookup completes.  */
  private final class MasterLookupCB implements Callback<Object,
      Master.GetTableLocationsResponsePB> {
    final String table;
    MasterLookupCB(String table) {
      this.table = table;
    }
    public Object call(final Master.GetTableLocationsResponsePB arg) {
      discoverTablets(table, arg);
      return null;
    }
    public String toString() {
      return "get tablet locations from the master";
    }
  };

  boolean acquireMasterLookupPermit() {
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
  void releaseMasterLookupPermit() {
    masterLookups.release();
  }

  private void discoverTablets(String table, Master.GetTableLocationsResponsePB response) {
    if (response.hasError()) {
      throw new MasterErrorException(response.getError());
    }
    if (response.getTabletLocationsCount() == 0) {
      // Keep a note that the table exists but it's not served yet, we'll retry
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table " + table + " hasn't been created yet");
      }
      tablesNotServed.add(table);
      return;
    }
    // Doing a get first instead of putIfAbsent to avoid creating unnecessary CSLMs because in
    // the most common case the table should already be present
    ConcurrentSkipListMap<byte[], RemoteTablet> tablets = tabletsCache.get(table);
    if (tablets == null) {
      tablets = new ConcurrentSkipListMap<byte[], RemoteTablet>(Bytes.MEMCMP);
      ConcurrentSkipListMap<byte[], RemoteTablet> oldTablets = tabletsCache.putIfAbsent
          (table, tablets);
      if (oldTablets != null) {
        tablets = oldTablets;
      }
    }

    for (Master.TabletLocationsPB tabletPb : response.getTabletLocationsList()) {
      // Early creating the tablet so that it parses out the pb
      RemoteTablet rt = createTabletFromPb(table, tabletPb);
      Slice tabletId = rt.tabletId;

      // If we already know about this one, just refresh the locations
      RemoteTablet currentTablet = tablet2client.get(tabletId);
      if (currentTablet != null) {
        currentTablet.refreshServers(tabletPb);
        continue;
      }

      // Putting it here first doesn't make it visible because tabletsCache is always looked up
      // first.
      RemoteTablet oldRt = tablet2client.putIfAbsent(tabletId, rt);
      if (oldRt != null) {
        // someone beat us to it
        continue;
      }
      LOG.info("Discovering tablet " + tabletId.toString(Charset.defaultCharset()) + " for " +
          "table " + table + " with start key " + Bytes.pretty(rt.startKey) + " and endkey " +
          Bytes.pretty(rt.endKey));
      rt.refreshServers(tabletPb);
      // This is making this tablet available
      // Even if two clients were racing in this method they are putting the same RemoteTablet
      // with the same start key in the CSLM in the end
      tablets.put(rt.getStartKey(), rt);
    }
  }

  RemoteTablet createTabletFromPb(String table, Master.TabletLocationsPB tabletPb) {
    byte[] startKey = tabletPb.getStartKey().toByteArray();
    byte[] endKey = tabletPb.getEndKey().toByteArray();
    Slice tabletId = new Slice(tabletPb.getTabletId().toByteArray());
    RemoteTablet rt = new RemoteTablet(table, tabletId, startKey, endKey);
    return rt;
  }

  /**
   * Gives the tablet's ID for the table name. In the future there will be multiple tablets
   * and this method will find the right one.
   * @param tableName table to find the tablet
   * @return a tablet ID as a slice or null if not found
   */
  RemoteTablet getTablet(String tableName, byte[] rowkey) {
    ConcurrentSkipListMap<byte[], RemoteTablet> tablets = tabletsCache.get(tableName);

    if (tablets == null) {
      return null;
    }

    // We currently only have one master tablet.
    if (tableName.equals(MASTER_TABLE_HACK)) {
      if (tablets.firstEntry() == null) {
        return null;
      }
      return tablets.firstEntry().getValue();
    }

    Map.Entry<byte[], RemoteTablet> tabletPair = tablets.floorEntry(rowkey);

    if (tabletPair == null) {
      return null;
    }

    byte[] endKey = tabletPair.getValue().getEndKey();

    if (endKey != EMPTY_ARRAY
        // If the stop key is an empty byte array, it means this tablet is the
        // last tablet for this table and this key ought to be in that tablet.
        && Bytes.memcmp(rowkey, endKey) >= 0) {
      return null;
    }

    return tabletPair.getValue();
  }

  /**
   * Retrieve the master registration (see {@link kudu.rpc.GetMasterRegistrationResponse}
   * for a replica.
   * @param masterClient An initialized client for the master replica.
   * @return A Deferred object for the master replica's current registration.
   */
  Deferred<GetMasterRegistrationResponse> getMasterRegistration(TabletClient masterClient) {
    GetMasterRegistrationRequest rpc = new GetMasterRegistrationRequest(masterTableHack);
    Deferred<GetMasterRegistrationResponse> d = rpc.getDeferred();
    masterClient.sendRpc(rpc);
    return d;
  }

  /**
   * If a live client already exists for the specified master server, returns that client;
   * otherwise, creates a new client for the specified master server.
   * @param masterHostPort The RPC host and port for the master server.
   * @return A live and initialized client for the specified master server.
   * @throws Exception If we are unable to create a client.
   */
  TabletClient newMasterClient(HostAndPort masterHostPort) throws Exception {
    return newClient(MASTER_TABLE_HACK + masterHostPort.toString(),
        masterHostPort.getHostText(), masterHostPort.getPort(), true);
  }

  TabletClient newClient(String uuid, final String host, final int port, boolean
      isMasterTable) {
    final String hostport = host + ':' + port;
    TabletClient client;
    SocketChannel chan;
    synchronized (ip2client) {
      client = ip2client.get(hostport);
      if (client != null && client.isAlive()) {
        return client;
      }
      final TabletClientPipeline pipeline = new TabletClientPipeline();
      client = pipeline.init(uuid, isMasterTable);
      chan = channelFactory.newChannel(pipeline);
      ip2client.put(hostport, client);  // This is guaranteed to return null.
    }
    this.client2tablets.put(client, new ArrayList<RemoteTablet>());
    final SocketChannelConfig config = chan.getConfig();
    config.setConnectTimeoutMillis(5000);
    config.setTcpNoDelay(true);
    // Unfortunately there is no way to override the keep-alive timeout in
    // Java since the JRE doesn't expose any way to call setsockopt() with
    // TCP_KEEPIDLE.  And of course the default timeout is >2h. Sigh.
    config.setKeepAlive(true);
    chan.connect(new InetSocketAddress(host, port));  // Won't block.
    return client;
  }

  /**
   * Performs a graceful shutdown of this instance.
   * <p>
   * <ul>
   *   <li>{@link KuduSession#flush Flushes} all buffered edits.</li>
   *   <li>Cancels all the other requests.</li>
   *   <li>Terminates all connections.</li>
   *   <li>Releases all other resources.</li>
   * </ul>
   * <strong>Not calling this method before losing the last reference to this
   * instance may result in data loss and other unwanted side effects</strong>
   * @return A {@link Deferred}, whose callback chain will be invoked once all
   * of the above have been done. If this callback chain doesn't fail, then
   * the clean shutdown will be successful, and all the data will be safe on
   * the Kudu side. In case of a failure (the "errback" is invoked) you will have
   * to open a new KuduClient if you want to retry those operations.
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
        super("KuduClient@" + KuduClient.super.hashCode() + " shutdown");
      }
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
        ArrayList<ArrayList<OperationResponse>>> {
      public Deferred<ArrayList<Void>> call(final ArrayList<ArrayList<OperationResponse>> arg) {
        return disconnectEverything().addCallback(new ReleaseResourcesCB());
      }
      public String toString() {
        return "disconnect callback";
      }
    }
    // 1. Flush everything.
    return closeAllSessions().addBothDeferring(new DisconnectCB());
  }

  private void checkIsClosed() {
    if (closed) {
      throw new IllegalStateException("Cannot proceed, the client has already been closed");
    }
  }

  private Deferred<ArrayList<ArrayList<OperationResponse>>> closeAllSessions() {
    // We create a copy because KuduSession.close will call removeSession which would get us a
    // concurrent modification during the iteration.
    Set<KuduSession> copyOfSessions;
    synchronized (sessions) {
      copyOfSessions = new HashSet<KuduSession>(sessions);
    }
    if (sessions.isEmpty()) {
      return Deferred.fromResult(null);
    }
    // Guaranteed that we'll have at least one session to close.
    ArrayList<Deferred<ArrayList<OperationResponse>>> deferreds =
        new ArrayList<Deferred<ArrayList<OperationResponse>>>(copyOfSessions.size());
    for (KuduSession session : copyOfSessions ) {
      deferreds.add(session.close());
    }

    return Deferred.group(deferreds);
  }

  /**
   * Closes every socket, which will also cancel all the RPCs in flight.
   */
  private Deferred<ArrayList<Void>> disconnectEverything() {
    ArrayList<Deferred<Void>> deferreds =
        new ArrayList<Deferred<Void>>(2);
    HashMap<String, TabletClient> ip2client_copy;
    synchronized (ip2client) {
      // Make a local copy so we can shutdown every Tablet Server clients
      // without hold the lock while we iterate over the data structure.
      ip2client_copy = new HashMap<String, TabletClient>(ip2client);
    }

    for (TabletClient ts : ip2client_copy.values()) {
      deferreds.add(ts.shutdown());
    }
    final int size = deferreds.size();
    return Deferred.group(deferreds).addCallback(
        new Callback<ArrayList<Void>, ArrayList<Void>>() {
          public ArrayList<Void> call(final ArrayList<Void> arg) {
            // Normally, now that we've shutdown() every client, all our caches should
            // be empty since each shutdown() generates a DISCONNECTED event, which
            // causes TabletClientPipeline to call removeClientFromCache().
            HashMap<String, TabletClient> logme = null;
            synchronized (ip2client) {
              if (!ip2client.isEmpty()) {
                logme = new HashMap<String, TabletClient>(ip2client);
              }
            }
            if (logme != null) {
              // Putting this logging statement inside the synchronized block
              // can lead to a deadlock, since HashMap.toString() is going to
              // call TabletClient.toString() on each entry, and this locks the
              // client briefly.  Other parts of the code lock clients first and
              // the ip2client HashMap second, so this can easily deadlock.
              LOG.error("Some clients are left in the client cache and haven't"
                  + " been cleaned up: " + logme);
            }
            return arg;
          }

          public String toString() {
            return "wait " + size + " TabletClient.shutdown()";
          }
        });
  }

  /**
   * Blocking call.
   * Performs a slow search of the IP used by the given client.
   * <p>
   * This is needed when we're trying to find the IP of the client before its
   * channel has successfully connected, because Netty's API offers no way of
   * retrieving the IP of the remote peer until we're connected to it.
   * @param client The client we want the IP of.
   * @return The IP of the client, or {@code null} if we couldn't find it.
   */
  private InetSocketAddress slowSearchClientIP(final TabletClient client) {
    String hostport = null;
    synchronized (ip2client) {
      for (final Map.Entry<String, TabletClient> e : ip2client.entrySet()) {
        if (e.getValue() == client) {
          hostport = e.getKey();
          break;
        }
      }
    }

    if (hostport == null) {
      HashMap<String, TabletClient> copy;
      synchronized (ip2client) {
        copy = new HashMap<String, TabletClient>(ip2client);
      }
      LOG.error("WTF?  Should never happen!  Couldn't find " + client
          + " in " + copy);
      return null;
    }
    final int colon = hostport.indexOf(':', 1);
    if (colon < 1) {
      LOG.error("WTF?  Should never happen!  No `:' found in " + hostport);
      return null;
    }
    final String host = getIP(hostport.substring(0, colon));
    int port;
    try {
      port = parsePortNumber(hostport.substring(colon + 1,
          hostport.length()));
    } catch (NumberFormatException e) {
      LOG.error("WTF?  Should never happen!  Bad port in " + hostport, e);
      return null;
    }
    return new InetSocketAddress(host, port);
  }

  /**
   * Removes all the cache entries referred to the given client.
   * @param client The client for which we must invalidate everything.
   * @param remote The address of the remote peer, if known, or null.
   */
  private void removeClientFromCache(final TabletClient client,
                                     final SocketAddress remote) {

    if (remote == null) {
      return;  // Can't continue without knowing the remote address.
    }

    String hostport;
    if (remote instanceof InetSocketAddress) {
      final InetSocketAddress sock = (InetSocketAddress) remote;
      final InetAddress addr = sock.getAddress();
      if (addr == null) {
        LOG.error("WTF?  Unresolved IP for " + remote
            + ".  This shouldn't happen.");
        return;
      } else {
        hostport = addr.getHostAddress() + ':' + sock.getPort();
      }
    } else {
      LOG.error("WTF?  Found a non-InetSocketAddress remote: " + remote
          + ".  This shouldn't happen.");
      return;
    }

    TabletClient old;
    synchronized (ip2client) {
      old = ip2client.remove(hostport);
    }
    LOG.debug("Removed from IP cache: {" + hostport + "} -> {" + client + "}");
    if (old == null) {
      LOG.warn("When expiring " + client + " from the client cache (host:port="
          + hostport + "), it was found that there was no entry"
          + " corresponding to " + remote + ".  This shouldn't happen.");
    }

    ArrayList<RemoteTablet> tablets = client2tablets.remove(client);
    if (tablets != null) {
      // Make a copy so we don't need to synchronize on it while iterating.
      RemoteTablet[] tablets_copy;
      synchronized (tablets) {
        tablets_copy = tablets.toArray(new RemoteTablet[tablets.size()]);
        tablets = null;
        // If any other thread still has a reference to `tablets', their
        // updates will be lost (and we don't care).
      }
      for (final RemoteTablet remoteTablet : tablets_copy) {
        remoteTablet.removeTabletServer(client);
      }
    }
  }

  private boolean isMasterTable(String tableName) {
    return MASTER_TABLE_HACK.equals(tableName);
  }

  private final class TabletClientPipeline extends DefaultChannelPipeline {

    private Logger log = LoggerFactory.getLogger(TabletClientPipeline.class);
    /**
     * Have we already disconnected?.
     * We use this to avoid doing the cleanup work for the same client more
     * than once, even if we get multiple events indicating that the client
     * is no longer connected to the TabletServer (e.g. DISCONNECTED, CLOSED).
     * No synchronization needed as this is always accessed from only one
     * thread at a time (equivalent to a non-shared state in a Netty handler).
     */
    private boolean disconnected = false;

    TabletClient init(String uuid, boolean isMasterTable) {
      final TabletClient client = new TabletClient(KuduClient.this, uuid, isMasterTable);
      super.addLast("handler", client);
      return client;
    }

    @Override
    public void sendDownstream(final ChannelEvent event) {
      if (event instanceof ChannelStateEvent) {
        handleDisconnect((ChannelStateEvent) event);
      }
      super.sendDownstream(event);
    }

    @Override
    public void sendUpstream(final ChannelEvent event) {
      if (event instanceof ChannelStateEvent) {
        handleDisconnect((ChannelStateEvent) event);
      }
      super.sendUpstream(event);
    }

    private void handleDisconnect(final ChannelStateEvent state_event) {
      if (disconnected) {
        return;
      }
      switch (state_event.getState()) {
        case OPEN:
          if (state_event.getValue() == Boolean.FALSE) {
            break;  // CLOSED
          }
          return;
        case CONNECTED:
          if (state_event.getValue() == null) {
            break;  // DISCONNECTED
          }
          return;
        default:
          return;  // Not an event we're interested in, ignore it.
      }

      disconnected = true;  // So we don't clean up the same client twice.
      try {
        final TabletClient client = super.get(TabletClient.class);
        SocketAddress remote = super.getChannel().getRemoteAddress();
        // At this point Netty gives us no easy way to access the
        // SocketAddress of the peer we tried to connect to. This
        // kinda sucks but I couldn't find an easier way.
        if (remote == null) {
          remote = slowSearchClientIP(client);
        }

        // Prevent the client from buffering requests while we invalidate
        // everything we have about it.
        synchronized (client) {
          removeClientFromCache(client, remote);
        }
      } catch (Exception e) {
        log.error("Uncaught exception when handling a disconnection of " + getChannel(), e);
      }
    }

  }

  /** Creates a default channel factory in case we haven't been given one.  */
  private static NioClientSocketChannelFactory defaultChannelFactory() {
    final Executor executor = Executors.newCachedThreadPool();
    return new NioClientSocketChannelFactory(executor, executor);
  }

  /**
   * Gets a hostname or an IP address and returns the textual representation
   * of the IP address.
   * <p>
   * <strong>This method can block</strong> as there is no API for
   * asynchronous DNS resolution in the JDK.
   * @param host The hostname to resolve.
   * @return The IP address associated with the given hostname,
   * or {@code null} if the address couldn't be resolved.
   */
  private static String getIP(final String host) {
    final long start = System.nanoTime();
    try {
      final String ip = InetAddress.getByName(host).getHostAddress();
      final long latency = System.nanoTime() - start;
      if (latency > 500000/*ns*/ && LOG.isDebugEnabled()) {
        LOG.debug("Resolved IP of `" + host + "' to "
            + ip + " in " + latency + "ns");
      } else if (latency >= 3000000/*ns*/) {
        LOG.warn("Slow DNS lookup!  Resolved IP of `" + host + "' to "
            + ip + " in " + latency + "ns");
      }
      return ip;
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve the IP of `" + host + "' in "
          + (System.nanoTime() - start) + "ns");
      return null;
    }
  }

  /**
   * Parses a TCP port number from a string.
   * @param portnum The string to parse.
   * @return A strictly positive, validated port number.
   * @throws NumberFormatException if the string couldn't be parsed as an
   * integer or if the value was outside of the range allowed for TCP ports.
   */
  private static int parsePortNumber(final String portnum)
      throws NumberFormatException {
    final int port = Integer.parseInt(portnum);
    if (port <= 0 || port > 65535) {
      throw new NumberFormatException(port == 0 ? "port is zero" :
          (port < 0 ? "port is negative: "
              : "port is too large: ") + port);
    }
    return port;
  }

  void newTimeout(final TimerTask task, final long timeout_ms) {
    try {
      timer.newTimeout(task, timeout_ms, MILLISECONDS);
    } catch (IllegalStateException e) {
      // This can happen if the timer fires just before shutdown()
      // is called from another thread, and due to how threads get
      // scheduled we tried to call newTimeout() after timer.stop().
      LOG.warn("Failed to schedule timer."
          + "  Ignore this if we're shutting down.", e);
    }
  }

  /**
   * This class encapsulates the information regarding a tablet and its locations.
   *
   * Leader failover mecanism:
   * When we get a complete quorum list from the master, we place the leader in the first
   * position of the tabletServers array. When we detect that it isn't the leader anymore (in
   * TabletClient), we demote it and set the next TS in the array as the leader. When the RPC
   * gets retried, it will use that TS since we always pick the leader.
   *
   * If that TS turns out to not be the leader, we will demote it and promote the next one, retry.
   * When we hit the end of the list, we set the leaderIndex to NO_LEADER_INDEX which forces us
   * to fetch the tablet locations from the master. We'll repeat this whole process until a RPC
   * succeeds.
   *
   * Subtleties:
   * We don't keep track of a TS after it disconnects (via removeTabletServer), so if we
   * haven't contacted one for 10 seconds (socket timeout), it will be removed from the list of
   * tabletServers. This means that if the leader fails, we only have one other TS to "promote"
   * or maybe none at all. This is partly why we then set leaderIndex to NO_LEADER_INDEX.
   *
   * The effect of treating a TS as the new leader means that the Scanner will also try to hit it
   * with requests. It's currently unclear if that's a good or a bad thing.
   *
   * Unlike the C++ client, we don't short-circuit the call to the master if it isn't available.
   * This means that after trying all the peers to find the leader, we might get stuck waiting on
   * a reachable master.
   */
  public class RemoteTablet implements Comparable<RemoteTablet> {

    private static final int NO_LEADER_INDEX = -1;
    private final String table;
    private final Slice tabletId;
    private final ArrayList<TabletClient> tabletServers = new ArrayList<TabletClient>();
    private final byte[] startKey;
    private final byte[] endKey;
    private int leaderIndex = NO_LEADER_INDEX;

    RemoteTablet(String table, Slice tabletId, byte[] startKey, byte[] endKey) {
      this.tabletId = tabletId;
      this.table = table;
      this.startKey = startKey;
      // makes comparisons easier in getTablet, we just do == instead of equals.
      this.endKey = endKey.length == 0 ? EMPTY_ARRAY : endKey;
    }

    void refreshServers(Master.TabletLocationsPB tabletLocations) {

      synchronized (tabletServers) { // TODO not a fat lock with IP resolving in it
        tabletServers.clear();
        leaderIndex = NO_LEADER_INDEX;
        for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {

          List<Common.HostPortPB> addresses = replica.getTsInfo().getRpcAddressesList();
          if (addresses.isEmpty()) {
            LOG.warn("Tablet server for tablet " + getTabletIdAsString() + " doesn't have any " +
                "address");
            continue;
          }
          byte[] buf = Bytes.get(replica.getTsInfo().getPermanentUuid());
          String uuid = Bytes.getString(buf);
          // from meta_cache.cc
          // TODO: if the TS advertises multiple host/ports, pick the right one
          // based on some kind of policy. For now just use the first always.
          addTabletClient(uuid, addresses.get(0).getHost(), addresses.get(0).getPort(),
              isMasterTable(table), replica.getRole().equals(Metadata.QuorumPeerPB.Role.LEADER));
        }
        leaderIndex = 0;
        if (leaderIndex == NO_LEADER_INDEX) {
          LOG.warn("No leader provided for tablet " + getTabletIdAsString());
        }
      }
    }

    // Must be called with tabletServers synchronized
    void addTabletClient(String uuid, String host, int port, boolean isMasterTable, boolean
        isLeader) {
      String ip = getIP(host);
      if (ip == null) {
        return;
      }
      TabletClient client = newClient(uuid, ip, port, isMasterTable);

      final ArrayList<RemoteTablet> tablets = client2tablets.get(client);

      if (tablets == null) {
        // We raced with removeClientFromCache and lost. The client we got was just disconnected.
        // Reconnect.
        addTabletClient(uuid, host, port, isMasterTable, isLeader);
      } else {
        synchronized (tablets) {
          if (isLeader) {
            tabletServers.add(0, client);
          } else {
            tabletServers.add(client);
          }
          tablets.add(this);
        }
      }
    }

    /**
     * Removes the passed TabletClient from this tablet's list of tablet servers. If it was the
     * leader, then we "promote" the next one unless it was the last one in the list.
     * @param ts A TabletClient that was disconnected.
     * @return True if this method removed ts from the list, else false.
     */
    boolean removeTabletServer(TabletClient ts) {
      synchronized (tabletServers) {
        // TODO unit test for this once we have the infra
        int index = tabletServers.indexOf(ts);
        if (index == -1) {
          return false; // we removed it already
        }

        tabletServers.remove(index);
        if (leaderIndex == index && leaderIndex == tabletServers.size()) {
          leaderIndex = NO_LEADER_INDEX;
        } else if (leaderIndex > index) {
          leaderIndex--; // leader moved down the list
        }

        return true;
        // TODO if we reach 0 TS, maybe we should remove ourselves?
      }
    }

    /**
     * If the passed TabletClient is the current leader, then the next one in the list will be
     * "promoted" unless we're at the end of the list, in which case we set the leaderIndex to
     * NO_LEADER_INDEX which will force a call to the master.
     * @param ts A TabletClient that gave a sign that it isn't this tablet's leader.
     */
    void demoteLeader(TabletClient ts) {
      synchronized (tabletServers) {
        int index = tabletServers.indexOf(ts);
        // If this TS was removed or we're already forcing a call to the master (meaning someone
        // else beat us to it), then we just noop.
        if (index == -1 || leaderIndex == NO_LEADER_INDEX) {
          return;
        }

        if (leaderIndex == index) {
          if (leaderIndex + 1 == tabletServers.size()) {
            leaderIndex = NO_LEADER_INDEX;
          } else {
            leaderIndex++;
          }
        }
      }
    }

    public String getTable() {
      return table;
    }

    Slice getTabletId() {
      return tabletId;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }

    byte[] getTabletIdAsBytes() {
      return tabletId.getBytes();
    }

    String getTabletIdAsString() {
      return tabletId.toString(Charset.defaultCharset());
    }

    List<Common.HostPortPB> getAddressesFromPb(Master.TabletLocationsPB tabletLocations) {
      List<Common.HostPortPB> addresses = new ArrayList<Common.HostPortPB>(tabletLocations
          .getReplicasCount());
      for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {
        addresses.add(replica.getTsInfo().getRpcAddresses(0));
      }
      return addresses;
    }

    @Override
    public int compareTo(RemoteTablet remoteTablet) {
      if (remoteTablet == null) {
        return 1;
      }

      int result = ComparisonChain.start()
          .compare(this.table, remoteTablet.table)
          .compare(this.startKey, remoteTablet.startKey, Bytes.MEMCMP).result();

      if (result != 0) {
        return result;
      }

      int endKeyCompare = Bytes.memcmp(this.endKey, remoteTablet.endKey);

      if (endKeyCompare != 0) {
        if (this.startKey != EMPTY_ARRAY
            && this.endKey == EMPTY_ARRAY) {
          return 1; // this is last tablet
        }
        if (remoteTablet.startKey != EMPTY_ARRAY
            && remoteTablet.endKey == EMPTY_ARRAY) {
          return -1; // remoteTablet is the last tablet
        }
        return endKeyCompare;
      }
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoteTablet that = (RemoteTablet) o;

      return this.compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(table, startKey, endKey);
    }
  }

}
