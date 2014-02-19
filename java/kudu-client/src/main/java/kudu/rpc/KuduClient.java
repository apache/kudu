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

import kudu.ColumnSchema;
import kudu.Common;
import kudu.Schema;
import kudu.master.Master;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

  private final ClientSocketChannelFactory channelFactory;

  private final ConcurrentHashMap<Slice, RemoteTablet> tablet2client =
      new ConcurrentHashMap<Slice, RemoteTablet>();

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

  // TODO Below is an uber hack, the master is considered a tablet with
  // a table name.
  private final TabletClient master; // currently the master cannot move
  static final String MASTER_TABLE_HACK =  "~~~masterTableHack~~~";
  static final Slice MASTER_TABLE_HACK_SLICE = new Slice(MASTER_TABLE_HACK.getBytes());
  final KuduTable masterTableHack;

  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  // A table is considered not served when we get an empty list of locations but know
  // that a tablet exists. Eventually this will be a the tablet-level.
  // TODO once we have multiple tablets per table, do more of what asynchbase does
  private final ConcurrentHashMap<String, Object> tablesNotServed
      = new ConcurrentHashMap<String, Object>();

  /**
   * Semaphore used to rate-limit master lookups
   * Once we have more than this number of concurrent META lookups, we'll
   * start to throttle ourselves slightly.
   * @see #acquireMasterLookupPermit
   */
  private final Semaphore masterLookups = new Semaphore(100);

  private final Random sleepRandomizer = new Random();

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
    // since we aren't going the normal path, we need to provide an IP here
    String ip = getIP(address);
    this.master = newClient(ip, port);
    this.masterTableHack = new KuduTable(this, MASTER_TABLE_HACK,
       new Schema(new ArrayList<ColumnSchema>(0)));
  }

  public Deferred<Object> ping() {
    PingRequest ping = new PingRequest(this.masterTableHack);
    return sendRpcToTablet(ping);
  }

  /**
   * Open the table with the given name.
   * @param name table to open
   * @param schema schema to use. TODO retrieve from the cluster in the future
   * @return a table to use Operations
   */
  public KuduTable openTable(String name, Schema schema) {
    return new KuduTable(this, name, schema);
  }

  /***
   * Create a tablet on the cluster with the specified name and a schema
   * @param name Table's name
   * @param schema Table's schema
   * @return Deferred object to track the progress
   */
  public Deferred<Object> createTable(String name, Schema schema) {
    CreateTableRequest create = new CreateTableRequest(this.masterTableHack, name, schema);
    return sendRpcToTablet(create);
  }

  /**
   * Delete a tablet on the cluster with the specified name
   * @param name Table's name
   * @return Deferred object to track the progress
   */
  public Deferred<Object> deleteTable(String name) {
    DeleteTableRequest delete = new DeleteTableRequest(this.masterTableHack, name);
    return sendRpcToTablet(delete);
  }

  /**
   * Creates a new {@link KuduScanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return A new scanner for this table.
   */
  public KuduScanner newScanner(final KuduTable table) {
    return new KuduScanner(this, table);
  }

  /**
   * Package-private access point for {@link KuduScanner}s to open themselves.
   * @param scanner The scanner to open.
   * @return A deferred {@link KuduScanner.Response}
   */
  Deferred<Object> openScanner(final KuduScanner scanner) {
    //num_scanners_opened.increment();
    return sendRpcToTablet(scanner.getOpenRequest()).addCallbacks(
        scanner_opened,
        new Callback<Object, Object>() {
          public Object call(final Object error) {
            String message = "Cannot openScanner because: ";
            if (error instanceof Exception) {
              LOG.warn(message, (Exception)error);
            } else {
              LOG.warn(message + " because of " + error);
            }
            // Don't let the scanner think it's opened on this region.
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
   * @return
   */
  public KuduSession newSession() {
    return new KuduSession(this);
  }

  /**
   * Package-private access point for {@link KuduScanner}s to scan more rows.
   * @param scanner The scanner to use.
   * @return A deferred row.
   */
  Deferred<Object> scanNextRows(final KuduScanner scanner) {
    final Slice tablet = scanner.currentTablet();
    final TabletClient client = clientFor(tablet);
    if (client == null) {
      // Oops, we no longer know anything about this client or tabletSlice.  Our
      // cache was probably invalidated while the client was scanning.  This
      // means that we lost the connection to that TabletServer, so we have to
      // re-open this scanner if we wanna keep scanning.
      scanner.invalidate();        // Invalidate the scanner so that ...
      @SuppressWarnings("unchecked")
      final Deferred<Object> d = (Deferred) scanner.nextRows();
      return d;  // ... this will re-open it ______.^
    }
    //num_scans.increment();
    final KuduRpc next_request = scanner.getNextRowsRequest();
    final Deferred<Object> d = next_request.getDeferred();
    client.sendRpc(next_request);
    return d;
  }

  /**
   * Package-private access point for {@link KuduScanner}s to close themselves.
   * @param scanner The scanner to close.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
   */
  Deferred<Object> closeScanner(final KuduScanner scanner) {
    final Slice tablet = scanner.currentTablet();
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
    final Deferred<Object> d = close_request.getDeferred();
    client.sendRpc(close_request);
    return d;
  }

  /** Singleton callback to handle responses of "openScanner" RPCs.  */
  private static final Callback<Object, Object> scanner_opened =
      new Callback<Object, Object>() {
        public Object call(final Object response) {
          if (response instanceof KuduScanner.Response) {
            return response;
          } else {
            throw new InvalidResponseException(Long.class, response);
          }
        }
        public String toString() {
          return "type openScanner response";
        }
      };


  Deferred<Object> sendRpcToTablet(final KuduRpc request) {
    if (cannotRetryRequest(request)) {
      return tooManyAttempts(request, null);
    }
    request.attempt++;
    final String tableName = request.getTable().getName();
    Slice tablet = getTablet(tableName);

    final class RetryRpc implements Callback<Deferred<Object>, Object> {
      public Deferred<Object> call(final Object arg) {
        Deferred<Object> d = handleLookupExceptions(request, arg);
        return d == null ? sendRpcToTablet(request):  // Retry the RPC.
                           d; // something went wrong
      }
      public String toString() {
        return "retry RPC";
      }
    }
    // We know that recently this RPC or another couldn't find a location for
    // this table, sleep some time before retrying.
    if (tablesNotServed.containsKey(tableName)) {
      Deferred<Object> d = request.getDeferred();
      final class NSRETimer implements TimerTask {
        public void run(final Timeout timeout) {
          tablesNotServed.remove(tableName);
          sendRpcToTablet(request);
        }
      };
      // TODO backoffs? Sleep in increments of 500 ms, plus some random time up to 50
      long sleepTime = (request.attempt * 500) + sleepRandomizer.nextInt(50);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to sleep for " + sleepTime + " at retry " + request.attempt);
      }
      newTimeout(new NSRETimer(), sleepTime);
      return d;
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
    // we'll just get all the tablets
    return locateTablet(tableName).addBothDeferring(new RetryRpc());
  }

  TabletClient clientFor(Slice tablet) {
    if (tablet == null) {
      return null;
    }
    if (MASTER_TABLE_HACK_SLICE.equals(tablet)) {
      return master;
    }
    RemoteTablet rt = tablet2client.get(tablet);
    return rt == null ? null : rt.tabletServers.get(0);
  }

  Deferred<Object> handleLookupExceptions(final KuduRpc request, Object arg) {
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
    return rpc.attempt > 10;  // TODO Don't hardcode.
  }

  /**
   * Returns a {@link Deferred} containing an exception when an RPC couldn't
   * succeed after too many attempts.
   * @param request The RPC that was retried too many times.
   * @param cause What was cause of the last failed attempt, if known.
   * You can pass {@code null} if the cause is unknown.
   */
  static Deferred<Object> tooManyAttempts(final KuduRpc request,
                                          final KuduException cause) {
    final Exception e = new NonRecoverableException("Too many attempts: "
        + request, cause);
    request.callback(e);
    return Deferred.fromError(e);
  }

  /**
   * Sends a getTableLocations RPC to the master to find the table's tablets
   * @param table table's name, in the future this will use a tablet id
   * @return Deferred to track the progress
   */
  Deferred<Object> locateTablet(String table) {
    final boolean has_permit = acquireMasterLookupPermit();
    if (!has_permit) {
      // If we failed to acquire a permit, it's worth checking if someone
      // looked up the tabletSlice we're interested in.  Every once in a while
      // this will save us a Master lookup.
      if (getTablet(table) != null) {
        return Deferred.fromResult(null);  // Looks like no lookup needed.
      }
    }
    final Deferred<Object> d =
        master.getTableLocations(table).addCallback(new MasterLookupCB(table));
    if (has_permit) {
      final class ReleaseMetaLookupPermit implements Callback<Object, Object> {
        public Object call(final Object arg) {
          releaseMasterLookupPermit();
          return arg;
        }
        public String toString() {
          return "release master lookup permit";
        }
      };
      d.addBoth(new ReleaseMetaLookupPermit());
    }
    return d;
  }

  /** Callback executed when a lookup in META completes.  */
  private final class MasterLookupCB implements Callback<Object,
      Master.GetTableLocationsResponsePB> {
    final String table;
    MasterLookupCB(String table) {
      this.table = table;
    }
    public Object call(final Master.GetTableLocationsResponsePB arg) {
      return discoverTablet(table, arg);
    }
    public String toString() {
      return "get tabletSlice locations from the master";
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

  private Slice discoverTablet(String table, Master.GetTableLocationsResponsePB response) {
    if (response.hasError()) {
      throw new MasterErrorException(response.getError());
    }
    if (response.getTabletLocationsCount() == 0) {
      // Keep a note that the tablet exists but it's not served yet
      tablesNotServed.putIfAbsent(table, new Object());
      return null;
    }
    Master.TabletLocationsPB tabletLocations = response.getTabletLocations(0);
    if (tabletLocations.getReplicasCount() == 0) {
      throw new NonRecoverableException("Tablet does not exist: " +
          tabletLocations.getTabletId().toStringUtf8());
    }
    Slice tabletId = new Slice(tabletLocations.getTabletId().toByteArray());
    RemoteTablet rt = tablet2client.get(tabletId);
    if (rt == null) {
      rt = new RemoteTablet(table, tabletId);
      RemoteTablet oldRt = tablet2client.putIfAbsent(tabletId, rt);
      if (oldRt != null) {
        // someone beat us to it
        return getTablet(table);
      }
    }
    rt.refreshServers(tabletLocations);
    return getTablet(table);
  }

  /**
   * Gives the tablet's ID for the table name. In the future there will be multiple tablets
   * and this method will find the right one.
   * @param tableName table to find the tablet
   * @return a tablet ID as a slice or null if not found
   */
  Slice getTablet(String tableName) {
    // TODO return tablets, not the table name back
    if (tableName.equals(MASTER_TABLE_HACK)) {
      return MASTER_TABLE_HACK_SLICE;
    }
    // TODO currently handle 1 tablet per table!
    for (Map.Entry<Slice, RemoteTablet> entry : tablet2client.entrySet()) {
      if (entry.getValue().table.equals(tableName)) {
        return entry.getKey();
      }
    }
    return null;
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
   * of the above have been done.  If this callback chain doesn't fail, then
   * the clean shutdown will be successful, and all the data will be safe on
   * the Kudu side (provided that you use <a href="#durability">durable</a>
   * edits).  In case of a failure (the "errback" is invoked) you may want to
   * retry the shutdown to avoid losing data, depending on the nature of the
   * failure.
   */
  public Deferred<Object> shutdown() {
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
    disconnectEverything().addCallback(new ReleaseResourcesCB());

    // TODO re-add where the buffers are

    /*final class DisconnectCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
        return disconnectEverything().addCallback(new ReleaseResourcesCB());
      }
      public String toString() {
        return "disconnect callback";
      }
    }*/

    // 1. Flush everything.
    //return flush().addCallback(new DisconnectCB());

    return null;
  }


  /**
   * Closes every socket, which will also flush all internal tabletSlice caches.
   */
  private Deferred<Object> disconnectEverything() {
    ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(2);
    HashMap<String, TabletClient> ip2client_copy;
    synchronized (ip2client) {
      // Make a local copy so we can shutdown every Region Server client
      // without hold the lock while we iterate over the data structure.
      ip2client_copy = new HashMap<String, TabletClient>(ip2client);
    }

    deferreds.add(master.shutdown());
    for (TabletClient ts : ip2client_copy.values()) {
      deferreds.add(ts.shutdown());
    }
    final int size = deferreds.size();
    return Deferred.group(deferreds).addCallback(
        new Callback<Object, ArrayList<Object>>() {
          public Object call(final ArrayList<Object> arg) {
            // Normally, now that we've shutdown() every client, all our caches should
            // be empty since each shutdown() generates a DISCONNECTED event, which
            // causes RegionClientPipeline to call removeClientFromCache().
            HashMap<String, TabletClient> logme = null;
            synchronized (ip2client) {
              if (!ip2client.isEmpty()) {
                logme = new HashMap<String, TabletClient>(ip2client);
              }
            }
            if (logme != null) {
              // Putting this logging statement inside the synchronized block
              // can lead to a deadlock, since HashMap.toString() is going to
              // call RegionClient.toString() on each entry, and this locks the
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
   * TODO
   * Removes all the cache entries referred to the given client.
   * @param client The client for which we must invalidate everything.
   * @param remote The address of the remote peer, if known, or null.
   */
  private void removeClientFromCache(final TabletClient client,
                                     final SocketAddress remote) {

    /*ArrayList<RegionInfo> regions = client2regions.remove(client);
    if (regions != null) {
      // Make a copy so we don't need to synchronize on it while iterating.
      RegionInfo[] regions_copy;
      synchronized (regions) {
        regions_copy = regions.toArray(new RegionInfo[regions.size()]);
        regions = null;
        // If any other thread still has a reference to `regions', their
        // updates will be lost (and we don't care).
      }
      for (final RegionInfo region : regions_copy) {
        final byte[] table = region.table();
        final byte[] stop_key = region.stopKey();
        // If stop_key is the empty array:
        //   This region is the last region for this table.  In order to
        //   find the start key of the last region, we append a '\0' byte
        //   at the end of the table name and search for the entry with a
        //   key right before it.
        // Otherwise:
        //   Search for the entry with a key right before the stop_key.
        final byte[] search_key =
            createRegionSearchKey(stop_key.length == 0
                ? Arrays.copyOf(table, table.length + 1)
                : table, stop_key);
        final Map.Entry<byte[], RegionInfo> entry =
            regions_cache.lowerEntry(search_key);
        if (entry != null && entry.getValue() == region) {
          // Invalidate the regions cache first, as it's the most damaging
          // one if it contains stale data.
          regions_cache.remove(entry.getKey());
          LOG.debug("Removed from regions cache: {}", region);
        }
        final TabletClient oldclient = region2client.remove(region);
        if (client == oldclient) {
          LOG.debug("Association removed: {} -> {}", region, client);
        } else if (oldclient != null) {  // Didn't remove what we expected?!
          LOG.warn("When handling disconnection of " + client
              + " and removing " + region + " from region2client"
              + ", it was found that " + oldclient + " was in fact"
              + " serving this region");
        }
      }
    }*/

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
      //LoggerFactory.getLogger(RegionClientPipeline.class)
      //  .debug("hooked sendDownstream " + event);
      if (event instanceof ChannelStateEvent) {
        handleDisconnect((ChannelStateEvent) event);
      }
      super.sendDownstream(event);
    }

    @Override
    public void sendUpstream(final ChannelEvent event) {
      //LoggerFactory.getLogger(RegionClientPipeline.class)
      //  .debug("hooked sendUpstream " + event);
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

  class RemoteTablet {

    private final String table;
    private final Slice tabletId;
    private final ArrayList<TabletClient> tabletServers = new ArrayList<TabletClient>();

    public RemoteTablet(String table, Slice tabletId) {
      this.tabletId = tabletId;
      this.table = table;
    }

    public void refreshServers(Master.TabletLocationsPB tabletLocations) {

      synchronized (tabletServers) { // TODO not a fat lock with IP resolving in it
        tabletServers.clear();
        for (Master.TabletLocationsPB.ReplicaPB replica : tabletLocations.getReplicasList()) {
          for (Common.HostPortPB hp : replica.getTsInfo().getRpcAddressesList()) {
            String ip = getIP(hp.getHost());
            if (ip == null) continue;

            TabletClient client = newClient(ip, hp.getPort());
            tabletServers.add(client);
            /*if (client == oldClient) {
              break;
            }
            TODO more handling from discoverRegion */
            break;
          }
        }
      }
    }
  }

}
