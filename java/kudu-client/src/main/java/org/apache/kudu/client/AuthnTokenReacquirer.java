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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An utility class to reacquire authentication token when the current one expires.
 */
@InterfaceAudience.Private
final class AuthnTokenReacquirer {
  /** The Kudu client object the AuthnTokenReacquirer is bound to. */
  private final AsyncKuduClient client;

  /** A dedicated synchronization object for #queuedRpcs */
  private final Object queuedRpcsLock = new Object();

  /**
   * Container to store information on RPCs affected by authn token expiration error. The RPCs
   * will be retried on successful token re-acquisition attempt or their errback() method
   * will be called if authn token re-acquisition fails.
   */
  @GuardedBy("queuedRpcsLock")
  private ArrayList<KuduRpc<?>> queuedRpcs = Lists.newArrayList();

  /**
   * Create a new AuthnTokenReacquirer object.
   *
   * @param client the Kudu client object
   */
  AuthnTokenReacquirer(AsyncKuduClient client) {
    this.client = client;
  }

  /**
   * Add information on the RPC which failed due to expired authentication token and requires a new
   * authn token to retry. Calling this method triggers authn token re-acquisition if there is not
   * active one yet.
   *
   * @param rpc the RPC which failed due to the expired authn token error
   */
  <R> void handleAuthnTokenExpiration(KuduRpc<R> rpc) {
    boolean doReacquire = false;
    synchronized (queuedRpcsLock) {
      if (queuedRpcs.isEmpty()) {
        // Using non-emptiness of the container as a state here. If the container is empty, that
        // means a new re-acquisition round should be started.  If the container is not empty,
        // that means the process of token re-acquisition has already been already and the
        // elements of the #queuedRpcs container should be processed once a new token
        // re-acquisition completes (it could succeed or fail).
        //
        // TODO(aserbin): introduce a timestamp for the recently acquired authn token, so it would
        //   not try to re-acquire a token too often if there is a race between clearing
        //   the container after token acquisition is completed and scheduling token re-acquisition.
        doReacquire = true;
      }
      queuedRpcs.add(rpc);
    }
    rpc.addTrace(new RpcTraceFrame.RpcTraceFrameBuilder(
        rpc.method(), RpcTraceFrame.Action.GET_NEW_AUTHENTICATION_TOKEN_THEN_RETRY)
        .build());

    if (doReacquire) {
      reacquireAuthnToken();
    }
  }

  private List<KuduRpc<?>> swapQueuedRpcs() {
    List<KuduRpc<?>> rpcList;
    synchronized (queuedRpcsLock) {
      rpcList = queuedRpcs;
      queuedRpcs = Lists.newArrayList();
    }
    assert !rpcList.isEmpty();
    return rpcList;
  }

  private void reacquireAuthnToken() {

    /**
     * An utility class providing callbacks for successful completion of authn token re-acqusition.
     */
    final class NewAuthnTokenCB implements Callback<Void, Boolean> {
      /**
       * Callback upon 'successful' completion of an attempt to acquire a new token,
       * i.e. an attempt where no exception detected in the code path.
       *
       * @param tokenAcquired {@code true} if a new token acquired, {@code false} if
       *                      the ConnectToCluster yielded no authn token.
       */
      @Override
      public Void call(Boolean tokenAcquired) throws Exception {
        // TODO(aserbin): do we need to handle a successful re-connect with no token some other way?
        retryQueuedRpcs();
        return null;
      }

      /**
       * Handle the affected RPCs on the completion of authn token re-acquisition. The result authn
       * token might be null, so in that case primary credentials will be used for future
       * connection negotiations.
       */
      void retryQueuedRpcs() {
        List<KuduRpc<?>> list = swapQueuedRpcs();
        for (KuduRpc<?> rpc : list) {
          client.handleRetryableErrorNoDelay(rpc, null);
        }
      }
    }

    /**
     * Errback to retry authn token re-acquisition and notify the handle the affected RPCs if the
     * re-acquisition failed after some number of retries (currently, it's 5 attempts).
     *
     * TODO(aserbin): perhaps we should retry indefinitely with increasing backoff, but aggressively
     *                timeout RPCs in the queue after each failure.
     */
    final class NewAuthnTokenErrB implements Callback<Void, Exception> {
      private static final int MAX_ATTEMPTS = 5;
      private final NewAuthnTokenCB cb;
      private int attempts = 0;

      NewAuthnTokenErrB(NewAuthnTokenCB cb) {
        this.cb = cb;
      }

      @Override
      public Void call(Exception e) {
        if (e instanceof RecoverableException && attempts < MAX_ATTEMPTS) {
          client.reconnectToCluster(cb, this);
          ++attempts;
          return null;
        }

        Exception reason = new NonRecoverableException(Status.NotAuthorized(String.format(
            "cannot re-acquire authentication token after %d attempts (%s)",
            MAX_ATTEMPTS,
            e.getMessage())));
        failQueuedRpcs(reason);
        return null;
      }

      /** Handle the affected RPCs if authn token re-acquisition fails.
       */
      void failQueuedRpcs(Exception reason) {
        List<KuduRpc<?>> rpcList = swapQueuedRpcs();
        for (KuduRpc<?> rpc : rpcList) {
          rpc.errback(reason);
        }
      }
    }

    final NewAuthnTokenCB newTokenCb = new NewAuthnTokenCB();
    final NewAuthnTokenErrB newTokenErrb = new NewAuthnTokenErrB(newTokenCb);
    client.reconnectToCluster(newTokenCb, newTokenErrb);
  }
}
