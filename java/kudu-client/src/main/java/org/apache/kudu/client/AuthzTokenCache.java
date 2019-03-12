/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kudu.client;

import com.google.common.base.Preconditions;
import com.stumbleupon.async.Callback;
import org.apache.kudu.security.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache for authz tokens received from the master of unbounded capacity. A
 * client will receive an authz token upon opening a table and put it into the
 * cache. A subsequent operation that requires an authz token (e.g. writes,
 * scans) will fetch it from the cache and attach it to the operation request.
 */
@ThreadSafe
@InterfaceAudience.Private
public class AuthzTokenCache {
  private static class RpcAndException {
    final KuduRpc<?> rpc;
    final KuduException ex;

    RpcAndException(KuduRpc<?> rpc, KuduException ex) {
      this.rpc = rpc;
      this.ex = ex;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AuthzTokenCache.class);
  private final AsyncKuduClient client;

  // Map from a table ID to an authz token for that table.
  private final ConcurrentHashMap<String, Token.SignedTokenPB> authzTokens = new ConcurrentHashMap<>();

  // Map from a table ID that has an in-flight RPC to get a new authz token, to
  // the list of RPCs waiting to be retried once that token is received and the
  // exception each is handling.
  // Note: Unlike the token map which is synchronized to make it threadsafe,
  // synchronization of this map also serves to ensure requests for the same
  // table ID get grouped together.
  @GuardedBy("retriesLock")
  private final Map<String, List<RpcAndException>>
      retriesForTable = new HashMap<>();
  private final Object retriesLock = new Object();

  // Number of RPCs sent to retrieve authz tokens. Useful for testing.
  private AtomicInteger numRetrievalsSent;

  /**
   * Create a new AuthzTokenCache object.
   *
   * @param client the Kudu client object with which to send requests.
   */
  AuthzTokenCache(@Nonnull AsyncKuduClient client) {
    this.client = client;
    numRetrievalsSent = new AtomicInteger(0);
  }

  /**
   * Returns the number of RPCs sent to retrieve authz token over the lifetime
   * of this cache.
   * @return number of RPCs sent
   */
  @InterfaceAudience.LimitedPrivate("Test")
  int numRetrievalsSent() {
    return numRetrievalsSent.get();
  }

  /**
   * Puts the given token into the cache. No validation is done on the validity
   * or expiration of the token -- that happens on the tablet servers.
   *
   * @param tableId the table ID the authz token is for
   * @param token an authz token to put into the cache
   */
  void put(@Nonnull String tableId, @Nonnull Token.SignedTokenPB token) {
    authzTokens.put(tableId, token);
  }

  /**
   * Returns the cached token for the given 'tableId' if one exists.
   *
   * @param tableId table ID to get an authz token for
   * @return the token for the table ID if one exists
   */
  Token.SignedTokenPB get(@Nonnull String tableId) {
    return authzTokens.get(tableId);
  }

  /**
   * Returns the list of pending RPCs waiting on a new authz token for the given
   * table, clearing the table's entry in the pending map.
   *
   * @param tableId the table ID whose RPCs should be cleared
   * @return the RPCs to be retried for the given table ID and the
   */
  private List<RpcAndException> clearPendingRetries(@Nonnull String tableId) {
    List<RpcAndException> pendingRetries;
    synchronized (retriesLock) {
      pendingRetries = retriesForTable.remove(tableId);
    }
    Preconditions.checkState(!pendingRetries.isEmpty(),
        "no pending retries for table " + tableId);
    return pendingRetries;
  }

  /**
   * Sends an RPC to retrieve an authz token for retrying the specified parent
   * RPC, calling 'cb' on success and 'eb' on failure.
   *
   * 'parentRpc' is used for logging and deadline tracking.
   *
   * @param parentRpc the RPC that is waiting on the authz token
   * @param cb callback to be called after receiving a response from the master
   * @param eb errback to be called after hitting an exception
   */
  private void sendRetrievalForRpc(@Nonnull KuduRpc<?> parentRpc,
                                   @Nonnull Callback<Void, GetTableSchemaResponse> cb,
                                   @Nonnull Callback<Void, Exception> eb) {
    String tableId = parentRpc.getTable().getTableId();
    LOG.debug("sending RPC to retrieve token for table ID " + tableId);
    GetTableSchemaRequest retrieveAuthzTokenReq = new GetTableSchemaRequest(
        client.getMasterTable(), tableId, /*name=*/null, client.getTimer(),
        client.getDefaultAdminOperationTimeoutMs(), /*requiresAuthzTokenSupport=*/true);
    retrieveAuthzTokenReq.setParentRpc(parentRpc);
    retrieveAuthzTokenReq.timeoutTracker.setTimeout(parentRpc.timeoutTracker.getTimeout());
    numRetrievalsSent.incrementAndGet();
    client.sendRpcToTablet(retrieveAuthzTokenReq).addCallback(cb)
                                                 .addErrback(eb);
  }

  /**
   * Method to call upon receiving an RPC that indicates it had an invalid authz
   * token and needs a new one. If there is already an in-flight RPC to retrieve
   * a new authz token for the given table, add the 'rpc' to the collection of
   * RPCs to be retried once the retrieval completes.
   *
   * @param rpc the RPC that needs a new authz token
   * @param ex error that caused triggered this retrieval
   * @param <R> the RPC type
   */
  <R> void retrieveAuthzToken(@Nonnull final KuduRpc<R> rpc, @Nonnull final KuduException ex) {
    /**
     * Handles a response from getting an authz token.
     */
    final class NewAuthzTokenCB implements Callback<Void, GetTableSchemaResponse> {
      private final String tableId;

      public NewAuthzTokenCB(String tableId) {
        this.tableId = tableId;
      }

      @Override
      public Void call(@Nonnull GetTableSchemaResponse resp) throws Exception {
        if (resp.getAuthzToken() == null) {
          // Note: If we were talking to an old master, we would hit an
          // exception earlier in the RPC handling.
          throw new NonRecoverableException(
              Status.InvalidArgument("no authz token retrieved for " + tableId));
        }
        LOG.debug("retrieved authz token for " + tableId);
        put(tableId, resp.getAuthzToken());
        for (RpcAndException rpcAndEx : clearPendingRetries(tableId)) {
          client.handleRetryableErrorNoDelay(rpcAndEx.rpc, rpcAndEx.ex);
        }
        return null;
      }
    }

    /**
     * Handles the case where there was an error getting the new authz token.
     */
    final class NewAuthzTokenErrB implements Callback<Void, Exception> {
      private KuduRpc<?> parentRpc;
      private final NewAuthzTokenCB cb;

      public NewAuthzTokenErrB(@Nonnull NewAuthzTokenCB cb, @Nonnull KuduRpc<?> parentRpc) {
        this.cb = cb;
        this.parentRpc = parentRpc;
      }

      @Override
      public Void call(@Nonnull Exception e) {
        String tableId = cb.tableId;
        if (e instanceof RecoverableException) {
          sendRetrievalForRpc(parentRpc, cb, this);
        } else {
          for (RpcAndException rpcAndEx : clearPendingRetries(tableId)) {
            rpcAndEx.rpc.errback(e);
          }
        }
        return null;
      }
    }

    final String tableId = rpc.getTable().getTableId();
    RpcAndException rpcAndEx = new RpcAndException(rpc, ex);
    synchronized (retriesLock) {
      List<RpcAndException> pendingRetries = retriesForTable.putIfAbsent(
          tableId, new ArrayList<>(Arrays.asList(rpcAndEx)));
      if (pendingRetries == null) {
        // There isn't an in-flight RPC to retrieve a new authz token.
        NewAuthzTokenCB newTokenCB = new NewAuthzTokenCB(tableId);
        NewAuthzTokenErrB newTokenErrB = new NewAuthzTokenErrB(newTokenCB, rpc);
        sendRetrievalForRpc(rpc, newTokenCB, newTokenErrB);
      } else {
        Preconditions.checkState(!pendingRetries.isEmpty(),
            "no pending retries for table " + tableId);
        pendingRetries.add(rpcAndEx);
      }
    }
  }
}
