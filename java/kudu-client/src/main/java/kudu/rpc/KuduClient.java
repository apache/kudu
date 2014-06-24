/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 * Portions copyright (c) 2014 Cloudera, Inc.
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
import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.master.Master;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.metadata.Metadata;
import kudu.tserver.Tserver;
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
  static final Slice MASTER_TABLE_HACK_SLICE = new Slice(MASTER_TABLE_HACK.getBytes());
  final RemoteTablet masterTabletHack;
  final KuduTable masterTableHack;
  private final String masterAddress;
  private final int masterPort;
  // END OF HACK

  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  /**
   * Timestamp required for HybridTime external consistency through timestamp
   * propagation.
   * @see src/common/common.proto
   */
  private long lastPropagatedTimestamp = NO_TIMESTAMP;

  // A table is considered not served when we get an empty list of locations but know
  // that a tablet exists. This is currently only used for new tables
  private final ConcurrentHashMap<String, Object> tablesNotServed
      = new ConcurrentHashMap<String, Object>();

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
   * Create a new client that connects to the specified master
   * Doesn't block and won't throw an exception if the master doesn't exist
   * @param address master's address
   * @param port master's port
   */
  public KuduClient(final String address, int port) {
    this(address, port, defaultChannelFactory());
  }

  /**
   * Create a new client that connects to the specified master
   * Doesn't block and won't throw an exception if the master doesn't exist
   * @param address master's address
   * @param port master's port
   * @param channelFactory
   */
  public KuduClient(final String address, int port,
                    final ClientSocketChannelFactory channelFactory) {
    this.channelFactory = channelFactory;
    this.masterAddress = address;
    this.masterPort = port;
    this.masterTableHack = new KuduTable(this, MASTER_TABLE_HACK,
       new Schema(new ArrayList<ColumnSchema>(0)));
    this.masterTabletHack = new RemoteTablet(MASTER_TABLE_HACK,
        MASTER_TABLE_HACK_SLICE, EMPTY_ARRAY, EMPTY_ARRAY);
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

  public Deferred<Tserver.PingResponsePB> ping() {
    checkIsClosed();
    PingRequest ping = new PingRequest(this.masterTableHack);
    return (Deferred<Tserver.PingResponsePB>) sendRpcToTablet(ping);
  }

  /**
   * Create a table on the cluster with the specified name and schema. Default table
   * configurations are used, mainly the tablet will have one tablet.
   * @param name Table's name
   * @param schema Table's schema
   * @return Deferred object to track the progress
   */
  public Deferred<Master.CreateTableResponsePB> createTable(String name, Schema schema) {
    return this.createTable(name, schema, new CreateTableBuilder());
  }

  /**
   * Create a table on the cluster with the specified name, schema, and table configurations
   * @param name Table's name
   * @param schema Table's schema
   * @param builder Table's configurations
   * @return Deferred object to track the progress
   */
  public Deferred<Master.CreateTableResponsePB> createTable(String name, Schema schema, CreateTableBuilder builder) {
    checkIsClosed();
    if (builder == null) {
      builder = new CreateTableBuilder();
    }
    CreateTableRequest create = new CreateTableRequest(this.masterTableHack, name, schema,
        builder);
    return (Deferred<Master.CreateTableResponsePB>) sendRpcToTablet(create);
  }

  /**
   * Delete a tablet on the cluster with the specified name
   * @param name Table's name
   * @return Deferred object to track the progress
   */
  public Deferred<Master.DeleteTableResponsePB> deleteTable(String name) {
    checkIsClosed();
    DeleteTableRequest delete = new DeleteTableRequest(this.masterTableHack, name);
    return (Deferred<Master.DeleteTableResponsePB>) sendRpcToTablet(delete);
  }

  /**
   * Alter a table on the cluster as specified by the builder.
   * @param name Table's name, if this is a table rename then the old table name must be passed
   * @param atb The information for the alter command
   * @return Deferred object to track the progress of the alter command,
   * but it's completion only means that the master received the request. Use
   * syncWaitOnAlterCompletion() to know when the alter completes.
   */
  public Deferred<Master.AlterTableResponsePB> alterTable(String name, AlterTableBuilder atb) {
    checkIsClosed();
    AlterTableRequest alter = new AlterTableRequest(this.masterTableHack, name, atb);
    return (Deferred<Master.AlterTableResponsePB>) sendRpcToTablet(alter);
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
      Deferred<?> d = sendRpcToTablet(new IsAlterTableDoneRequest(this.masterTableHack, name));
      Object response;
      try {
        response = d.join(SLEEP_TIME);
      } catch (Exception ex) {
        throw ex;
      }
      if (response instanceof MasterErrorException) {
        throw (MasterErrorException) response;
      } else if (response instanceof Master.IsAlterTableDoneResponsePB) {
        Master.IsAlterTableDoneResponsePB pb = (Master.IsAlterTableDoneResponsePB) response;
        if (pb.getDone()) {
          return true;
        }
      } else {
        throw new NonRecoverableException("Cannot parse reponse " + response);
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
  public Deferred<Integer> getTabletServersCount() {
    checkIsClosed();
    ListTabletServersRequest rpc = new ListTabletServersRequest(this.masterTableHack);
    return (Deferred<Integer>) sendRpcToTablet(rpc);
  }

  Deferred<Schema> getTableSchema(String name) {
    return (Deferred<Schema>) sendRpcToTablet(new GetTableSchemaRequest(this.masterTableHack,
        name));
  }

  /**
   * Open the table with the given name.
   * @param name table to open
   * @return a KuduTable if the table exists, else a MasterErrorException
   */
  public Deferred<KuduTable> openTable(final String name) {
    checkIsClosed();
    return getTableSchema(name).addCallback(new Callback<KuduTable, Schema>() {
      @Override
      public KuduTable call(Schema schema) throws Exception {
        return new KuduTable(KuduClient.this, name, schema);
      }
    });
  }

  /**
   * Creates a new {@link KuduScanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * The scanner will be created with ReadMode.READ_LATEST
   * @return A new scanner for this table.
   * @see src/common/common.proto for info on ReadModes
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
   * @see src/common/common.proto for info on ReadModes and snapshot scanners
   */
  public KuduScanner newSnapshotScanner(final KuduTable table, Schema schema) {
    checkIsClosed();
    return new KuduScanner(this, table, schema, READ_AT_SNAPSHOT);
  }

  /**
   * Like the above, also creates a Snapshot scanner, but allows to specify
   * the snapshot timestamp.
   *
   * @see src/common/common.proto for info on ReadModes and snapshot scanners
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
    return (Deferred<KuduScanner.Response>) sendRpcToTablet(scanner.getOpenRequest()).addErrback(
        new Callback<Object, Object>() {
          public Object call(final Object error) {
            String message = "Cannot openScanner because: ";
            if (error instanceof Exception) {
              LOG.warn(message, (Exception)error);
            } else {
              LOG.warn(message + " because of " + error);
            }
            // Don't let the scanner think it's opened on this tablet.
            scanner.invalidate();
            return error;  // Let the error propagate.
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
    if (client == null) {
      // Oops, we no longer know anything about this client or tabletSlice.  Our
      // cache was probably invalidated while the client was scanning.  This
      // means that we lost the connection to that TabletServer, so we have to
      // re-open this scanner if we wanna keep scanning.
      scanner.invalidate();        // Invalidate the scanner so that ...
      Exception e = new NonRecoverableException("Scanner encountered a tablet server failure: " +
          scanner);
      return Deferred.fromError(e);
    }
    //num_scans.increment();
    final KuduRpc next_request = scanner.getNextRowsRequest();
    final Deferred<?> d = next_request.getDeferred();
    client.sendRpc(next_request);
    return (Deferred<KuduScanner.Response>) d;
  }

  /**
   * Package-private access point for {@link KuduScanner}s to close themselves.
   * @param scanner The scanner to close.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
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
    final KuduRpc close_request = scanner.getCloseRequest();
    final Deferred<?> d = close_request.getDeferred();
    client.sendRpc(close_request);
    return (Deferred<KuduScanner.Response>) d;
  }

  Deferred<?> sendRpcToTablet(final KuduRpc request) {
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
        final Deferred<Object> d = request.getDeferred();
        tabletClient.sendRpc(request);
        return d;
      }
    }

    // Right after creating a table a request will fall into locateTablet since we don't know yet
    // if the table is ready or not. If discoverTablets() didn't get any tablets back,
    // then on retry we'll fall into the following block. It will sleep, then call the master to
    // see if the table was created. We'll spin like this until the table is created and then
    // we'll try to locate the tablet again.
    if (tablesNotServed.containsKey(tableName)) {
      return delayedIsCreateTableDone(tableName, request, getRetryRpcCB(request));
    }
    // TODO Needed to remove the generics to make it compile, is there a better way?
    Callback cb = getRetryRpcCB(request);
    return locateTablet(tableName, rowkey).addBothDeferring(cb);
  }

  private Callback<Deferred<?>, Object> getRetryRpcCB(final KuduRpc request) {
    final class RetryRpcCB implements Callback<Deferred<?>, Object> {
      public Deferred<?> call(final Object arg) {
        Deferred<?> d = handleLookupExceptions(request, arg);
        return d == null ? sendRpcToTablet(request):  // Retry the RPC.
                           d; // something went wrong
      }
      public String toString() {
        return "retry RPC";
      }
    }
    return new RetryRpcCB();
  }

  /**
   * This method will call IsCreateTableDone on the master after sleeping for
   * getSleepTimeForRpc() based on the provided KuduRpc's number of attempts. Once this is done,
   * the provided callback will be called
   * @param tableName Table to lookup
   * @param rpc Original KuduRpc that needs to access the table
   * @param cb Callback to call on completion
   * @return Deferred used to track the provided KuduRpc
   */
  Deferred<?> delayedIsCreateTableDone(final String tableName, final KuduRpc rpc,
                                       final Callback cb) {

    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        final boolean has_permit = acquireMasterLookupPermit();
        if (!has_permit) {
          // If we failed to acquire a permit, it's worth checking if someone
          // looked up the tablet we're interested in.  Every once in a while
          // this will save us a Master lookup.
          if (!tablesNotServed.containsKey(tableName)) {
            try {
              cb.call(null);
              return;
            } catch (Exception e) {
              // we're calling RetryRpcCB which doesn't throw exceptions, ignore
            }
          }
        }
        final Deferred<Object> d =
            clientFor(masterTabletHack).isCreateTableDone(tableName).addCallback(new
                IsCreateTableDoneCB(tableName));
        if (has_permit) {
          d.addBoth(new ReleaseMasterLookupPermit());
        }
        d.addBothDeferring(cb);
      }
    };
    long sleepTime = getSleepTimeForRpc(rpc);
    if (rpc.deadlineTracker.wouldSleepingTimeout(sleepTime)) {
      return tooManyAttemptsOrTimeout(rpc, null);
    }

    newTimeout(new RetryTimer(), sleepTime);
    return rpc.getDeferred();
  }

  private final class ReleaseMasterLookupPermit implements Callback<Object, Object> {
    public Object call(final Object arg) {
      releaseMasterLookupPermit();
      return arg;
    }
    public String toString() {
      return "release master lookup permit";
    }
  };

  /** Callback executed when IsCreateTableDone completes.  */
  private final class IsCreateTableDoneCB implements Callback<Object,
      Master.IsCreateTableDoneResponsePB> {
    final String table;
    IsCreateTableDoneCB(String table) {
      this.table = table;
    }
    public Object call(final Master.IsCreateTableDoneResponsePB response) {
      if (response.getDone()) {
        LOG.debug("Table " + table + " was created");
        tablesNotServed.remove(table);
      } else if (response.hasError()) {
        throw new MasterErrorException(response.getError());
      } else {
        LOG.debug("Table " + table + " is still being created");
      }
      return null;
    }
    public String toString() {
      return "ask the master if " + table + " was created";
    }
  };

  boolean isTableNotServed(String tableName) {
    return tablesNotServed.containsKey(tableName);
  }


  long getSleepTimeForRpc(KuduRpc rpc) {
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
    if (masterTabletHack == tablet) {
      synchronized (masterTabletHack.tabletServers) {
        if (masterTabletHack.tabletServers.isEmpty()) {
          // TODO We currently always reconnect to the same master, too bad if it dies
          masterTabletHack.addTabletClient(masterAddress, masterPort);
        }
        return masterTabletHack.tabletServers.get(0);
      }
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

  Deferred<?> handleLookupExceptions(final KuduRpc request, Object arg) {
    if (arg instanceof NonRecoverableException) {
      // No point in retrying here, so fail the RPC.
      KuduException e = (NonRecoverableException) arg;
      if (e instanceof HasFailedRpcException
          && ((HasFailedRpcException) e).getFailedRpc() != request) {
        // If we get here it's because a dependent RPC (such as a master
        // lookup) has failed.  Therefore the exception we're getting
        // indicates that the master lookup failed, but we need to return
        // to our caller here that it's their RPC that failed.  Here we
        // re-create the exception but with the correct RPC in argument.
        e = e.make(e, request);  // e is likely a PleaseThrottleException.
      }
      request.callback(e);
      return Deferred.fromError(e);
    }
    return null;
  }

  /**
   * Checks whether or not an RPC can be retried once more.
   * @param rpc The RPC we're going to attempt to execute.
   * @return {@code true} if this RPC already had too many attempts,
   * {@code false} otherwise (in which case it's OK to retry once more).
   * @throws NonRecoverableException if the request has had too many attempts
   * already.
   */
  static boolean cannotRetryRequest(final KuduRpc rpc) {
    return rpc.deadlineTracker.timedOut() || rpc.attempt > 10;  // TODO Don't hardcode.
  }

  /**
   * Returns a {@link Deferred} containing an exception when an RPC couldn't
   * succeed after too many attempts or if it already timed out.
   * @param request The RPC that was retried too many times or timed out.
   * @param cause What was cause of the last failed attempt, if known.
   * You can pass {@code null} if the cause is unknown.
   */
  static Deferred<?> tooManyAttemptsOrTimeout(final KuduRpc request,
                                              final KuduException cause) {
    String message;
    if (request.deadlineTracker.timedOut()) {
      message = "Time out: ";
    } else {
      message = "Too many attempts: ";
    }
    final Exception e = new NonRecoverableException(message + request, cause);
    request.callback(e);
    return Deferred.fromError(e);
  }

  /**
   * Sends a getTableLocations RPC to the master to find the table's tablets
   * @param table table's name, in the future this will use a tablet id
   * @param rowkey can be null, if not we'll find the exact tablet that contains it
   * @return Deferred to track the progress
   */
  Deferred<Object> locateTablet(String table, byte[] rowkey) {
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
    final Deferred<Object> d =
        clientFor(masterTabletHack).getTableLocations(table, rowkey, rowkey).
            addCallback(new MasterLookupCB(table));
    if (has_permit) {
      d.addBoth(new ReleaseMasterLookupPermit());
    }
    return d;
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
   * @throws Exception
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
      final Deferred<Master.GetTableLocationsResponsePB> d =
          clientFor(masterTabletHack).getTableLocations(table, startKey, endKey);
      // TODO we should post-process exceptions that come out here, do in KUDU-169
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
  void handleNSTE(final KuduRpc rpc, KuduException ex, TabletClient server) {
    invalidateTabletCache(rpc.getTablet(), server);
    handleRetryableError(rpc, ex);
  }

  void handleRetryableError(final KuduRpc rpc, KuduException ex) {
    // TODO we don't always need to sleep, maybe another replica can serve this RPC

    boolean cannotRetry = cannotRetryRequest(rpc);
    if (cannotRetry) {
      // This is a callback
      tooManyAttemptsOrTimeout(rpc, ex);
    }

    delayedSendRpcToTablet(rpc, ex);
  }

  private void delayedSendRpcToTablet(final KuduRpc rpc, KuduException ex) {
    // Here we simply retry the RPC later. We might be doing this along with a lot of other RPCs
    // in parallel. Asynchbase does some hacking with a "probe" RPC while putting the other ones
    // on hold but we won't be doing this for the moment. Regions in HBase can move a lot,
    // we're not expecting this in Kudu.
    final class RetryTimer implements TimerTask {
      public void run(final Timeout timeout) {
        sendRpcToTablet(rpc);
      }
    };
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
      tablesNotServed.putIfAbsent(table, new Object());
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
    // The hack for the master only has one tablet, we can just return it right away
    if (tableName.equals(MASTER_TABLE_HACK)) {
      return masterTabletHack;
    }

    ConcurrentSkipListMap<byte[], RemoteTablet> tablets = tabletsCache.get(tableName);

    if (tablets == null) {
      return null;
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

  private TabletClient newClient(final String host, final int port) {
    final String hostport = host + ':' + port;
    TabletClient client;
    SocketChannel chan;
    synchronized (ip2client) {
      client = ip2client.get(hostport);
      if (client != null && client.isAlive()) {
        return client;
      }
      final TabletClientPipeline pipeline = new TabletClientPipeline();
      client = pipeline.init();
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
   *   <li>Completes all outstanding requests.</li>
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
   */
  public Deferred<Object> shutdown() {
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
    };

    // 3. Release all other resources.
    final class ReleaseResourcesCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
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
    final class DisconnectCB implements Callback<Object,
        ArrayList<ArrayList<Tserver.WriteResponsePB>>> {
      public Object call(final ArrayList<ArrayList<Tserver.WriteResponsePB>> arg) {
        return disconnectEverything().addCallback(new ReleaseResourcesCB());
      }
      public String toString() {
        return "disconnect callback";
      }
    }

    // 1. Flush everything.
    return closeAllSessions().addBoth(new DisconnectCB());
  }

  private void checkIsClosed() {
    if (closed) {
      throw new IllegalStateException("Cannot proceed, the client has already been closed");
    }
  }

  private Deferred<ArrayList<ArrayList<Tserver.WriteResponsePB>>> closeAllSessions() {
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
    ArrayList<Deferred<ArrayList<Tserver.WriteResponsePB>>> deferreds =
        new ArrayList<Deferred<ArrayList<Tserver.WriteResponsePB>>>(copyOfSessions.size());
    for (KuduSession session : copyOfSessions ) {
      deferreds.add(session.close());
    }

    return Deferred.group(deferreds);
  }


  /**
   * Closes every socket, which will also flush all internal tabletSlice caches.
   */
  private Deferred<Object> disconnectEverything() {
    ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(2);
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
        new Callback<Object, ArrayList<Object>>() {
          public Object call(final ArrayList<Object> arg) {
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
              return disconnectEverything();  // Try again.
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

    TabletClient init() {
      final TabletClient client = new TabletClient(KuduClient.this);
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

    /** A custom channel factory that doesn't shutdown its executor.  */
  private static final class CustomChannelFactory
      extends NioClientSocketChannelFactory {
    CustomChannelFactory(final Executor executor) {
      super(executor, executor);
    }
    @Override
    public void releaseExternalResources() {
      // Do nothing, we don't want to shut down the executor.
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
   * This class encapsulates the information regarding a tablet and its locations
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
      // makes comparisons easier in getTablet, no need to
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
          // from meta_cache.cc
          // TODO: if the TS advertises multiple host/ports, pick the right one
          // based on some kind of policy. For now just use the first always.
          int index = addTabletClient(addresses.get(0).getHost(), addresses.get(0).getPort());
          if (replica.getRole().equals(Metadata.QuorumPeerPB.Role.LEADER) &&
              index != NO_LEADER_INDEX) {
            leaderIndex = index;
          }
        }
        if (leaderIndex == NO_LEADER_INDEX) {
          LOG.warn("No leader provided for tablet " + getTabletIdAsString());
        }
      }
    }

    // Must be called with tabletServers synchronized
    int addTabletClient(String host, int port) {
      String ip = getIP(host);
      if (ip == null) {
        return NO_LEADER_INDEX;
      }
      TabletClient client = newClient(ip, port);
      tabletServers.add(client);

      synchronized (client) {
        final ArrayList<RemoteTablet> tablets = client2tablets.get(client);
        synchronized (tablets) {
          tablets.add(this);
        }
      }
      // guaranteed to return at least 0
      return tabletServers.size() - 1;
    }

    boolean removeTabletServer(TabletClient ts) {
      synchronized (tabletServers) {
        // TODO unit test for this once we have the infra
        int index = tabletServers.indexOf(ts);
        if (index == -1) {
          return false; // we removed it already
        }
        if (leaderIndex == index) {
          leaderIndex = NO_LEADER_INDEX; // we lost the leader
        } else if (leaderIndex > index) {
          leaderIndex--; // leader moved down the list
        }
        tabletServers.remove(index);
        return true;
        // TODO if we reach 0 TS, maybe we should remove ourselves?
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
