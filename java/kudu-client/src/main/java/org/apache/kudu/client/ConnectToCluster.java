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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.consensus.Metadata.RaftPeerPB.Role;
import org.apache.kudu.master.Master.ConnectToMasterResponsePB;
import org.apache.kudu.rpc.RpcHeader.ErrorStatusPB.RpcErrorCodePB;
import org.apache.kudu.util.NetUtil;

/**
 * Class responsible for fanning out RPCs to all of the configured masters,
 * finding a leader, and responding when the leader has been located.
 */
@InterfaceAudience.Private
final class ConnectToCluster {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectToCluster.class);

  private final List<HostAndPort> masterAddrs;
  private final Deferred<ConnectToClusterResponse> responseD;
  private final int numMasters;

  // Used to avoid calling 'responseD' twice.
  private final AtomicBoolean responseDCalled = new AtomicBoolean(false);

  /** Number of responses we've received so far */
  private final AtomicInteger countResponsesReceived = new AtomicInteger(0);

  // Exceptions received so far: kept for debugging purposes.
  private final List<Exception> exceptionsReceived =
      Collections.synchronizedList(new ArrayList<Exception>());

  /**
   * Creates an object that holds the state needed to retrieve master table's location.
   * @param masterAddrs Addresses of all master replicas that we want to retrieve the
   *                    registration from.
   */
  ConnectToCluster(List<HostAndPort> masterAddrs) {
    this.masterAddrs = masterAddrs;
    this.responseD = new Deferred<>();
    this.numMasters = masterAddrs.size();
  }

  @VisibleForTesting
  public Deferred<ConnectToClusterResponse> getDeferred() {
    return responseD;
  }

  private static Deferred<ConnectToMasterResponsePB> connectToMaster(
      final KuduTable masterTable,
      final RpcProxy masterProxy,
      KuduRpc<?> parentRpc,
      long defaultTimeoutMs) {
    // TODO: Handle the situation when multiple in-flight RPCs all want to query the masters,
    // basically reuse in some way the master permits.
    final ConnectToMasterRequest rpc = new ConnectToMasterRequest(masterTable);
    if (parentRpc != null) {
      rpc.setTimeoutMillis(parentRpc.deadlineTracker.getMillisBeforeDeadline());
      rpc.setParentRpc(parentRpc);
    } else {
      rpc.setTimeoutMillis(defaultTimeoutMs);
    }
    Deferred<ConnectToMasterResponsePB> d = rpc.getDeferred();
    rpc.attempt++;
    masterProxy.sendRpc(rpc);

    // If we are connecting to an older version of Kudu, we'll get an invalid request
    // error. In that case, we resend using the older version of the RPC.
    d.addErrback(new Callback<Deferred<ConnectToMasterResponsePB>, Exception>() {
      @Override
      public Deferred<ConnectToMasterResponsePB> call(Exception result)
          throws Exception {
        if (result instanceof RpcRemoteException) {
          RpcRemoteException rre = (RpcRemoteException)result;
          if (rre.getErrPB().getCode() == RpcErrorCodePB.ERROR_INVALID_REQUEST &&
              rre.getErrPB().getUnsupportedFeatureFlagsCount() > 0) {
            AsyncKuduClient.LOG.debug("Falling back to GetMasterRegistration() RPC to connect " +
                "to server running Kudu < 1.3.");
            final Deferred<ConnectToMasterResponsePB> newAttempt =
                Preconditions.checkNotNull(rpc.getDeferred());
            rpc.setUseOldMethod();
            masterProxy.sendRpc(rpc);
            return newAttempt;
          }
        }
        return Deferred.fromError(result);
      }
    });

    return d;
  }

  /**
   * Locate the leader master and retrieve the cluster information
   * (see {@link ConnectToClusterResponse}.
   *
   * @param masterTable the "placeholder" table used by AsyncKuduClient
   * @param masterAddresses the addresses of masters to fetch from
   * @param parentRpc RPC that prompted a master lookup, can be null
   * @param defaultTimeoutMs timeout to use for RPCs if the parentRpc has no timeout
   * @param credentialsPolicy credentials policy to use for connection negotiation
   * @return a Deferred object for the cluster connection status
   */
  public static Deferred<ConnectToClusterResponse> run(
      KuduTable masterTable,
      List<HostAndPort> masterAddresses,
      KuduRpc<?> parentRpc,
      long defaultTimeoutMs,
      Connection.CredentialsPolicy credentialsPolicy) {
    ConnectToCluster connector = new ConnectToCluster(masterAddresses);

    // Try to connect to each master. The ConnectToCluster instance
    // waits until it gets a good response before firing the returned
    // deferred.
    for (HostAndPort hostAndPort : masterAddresses) {
      Deferred<ConnectToMasterResponsePB> d;
      RpcProxy proxy = masterTable.getAsyncClient().newMasterRpcProxy(
          hostAndPort, credentialsPolicy);
      if (proxy != null) {
        d = connectToMaster(masterTable, proxy, parentRpc, defaultTimeoutMs);
      } else {
        String message = "Couldn't resolve this master's address " + hostAndPort.toString();
        LOG.warn(message);
        Status statusIOE = Status.IOError(message);
        d = Deferred.fromError(new NonRecoverableException(statusIOE));
      }
      d.addCallbacks(connector.callbackForNode(hostAndPort),
          connector.errbackForNode(hostAndPort));
    }
    return connector.responseD;
  }

  /**
   * Creates a callback for a ConnectToMaster RPC that was sent to 'hostAndPort'.
   * @see ConnectToMasterCB
   * @param hostAndPort Host and part for the RPC we're attaching this to. Host and port must
   *                    be valid.
   * @return The callback object that can be added to the RPC request.
   */
  @VisibleForTesting
  Callback<Void, ConnectToMasterResponsePB> callbackForNode(HostAndPort hostAndPort) {
    return new ConnectToMasterCB(hostAndPort);
  }

  /**
   * Creates an errback for a ConnectToMaster that was sent to 'hostAndPort'.
   * @see ConnectToMasterErrCB
   * @param hostAndPort Host and port for the RPC we're attaching this to. Used for debugging
   *                    purposes.
   * @return The errback object that can be added to the RPC request.
   */
  @VisibleForTesting
  Callback<Void, Exception> errbackForNode(HostAndPort hostAndPort) {
    return new ConnectToMasterErrCB(hostAndPort);
  }

  /**
   * Checks if we've already received a response or an exception from every master that
   * we've sent a ConnectToMaster to. If so -- and no leader has been found
   * (that is, 'responseD' was never called) -- pass a {@link NoLeaderFoundException}
   * to responseD.
   */
  private void incrementCountAndCheckExhausted() {
    if (countResponsesReceived.incrementAndGet() == numMasters &&
        responseDCalled.compareAndSet(false, true)) {
      // We want `allUnrecoverable` to only be true if all the masters came back with
      // NonRecoverableException so that we know for sure we can't retry anymore. Just one master
      // that replies with RecoverableException or with an ok response but is a FOLLOWER is
      // enough to keep us retrying.
      boolean allUnrecoverable = true;
      if (exceptionsReceived.size() == countResponsesReceived.get()) {
        for (Exception ex : exceptionsReceived) {
          if (!(ex instanceof NonRecoverableException)) {
            allUnrecoverable = false;
            break;
          }
        }
      } else {
        allUnrecoverable = false;
      }

      String allHosts = NetUtil.hostsAndPortsToString(masterAddrs);
      if (allUnrecoverable) {
        // This will stop retries.
        String msg = String.format("Couldn't find a valid master in (%s). " +
            "Exceptions received: %s", allHosts,
            Joiner.on(",").join(Lists.transform(
                exceptionsReceived, Functions.toStringFunction())));
        Status s = Status.ServiceUnavailable(msg);
        responseD.callback(new NonRecoverableException(s));
      } else {
        String message = String.format("Master config (%s) has no leader.",
            allHosts);
        Exception ex;
        if (exceptionsReceived.isEmpty()) {
          LOG.warn("None of the provided masters {} is a leader; will retry", allHosts);
          ex = new NoLeaderFoundException(Status.ServiceUnavailable(message));
        } else {
          LOG.warn("Unable to find the leader master {}; will retry", allHosts);
          String joinedMsg = message + " Exceptions received: " +
              Joiner.on(",").join(Lists.transform(
                  exceptionsReceived, Functions.toStringFunction()));
          Status s = Status.ServiceUnavailable(joinedMsg);
          ex = new NoLeaderFoundException(s,
              exceptionsReceived.get(exceptionsReceived.size() - 1));
        }
        responseD.callback(ex);
      }
    }
  }

  /**
   * Callback for each ConnectToCluster RPC sent in connectToMaster() above.
   * If a request (paired to a specific master) returns a reply that indicates it's a leader,
   * the callback in 'responseD' is invoked with an initialized GetTableLocationResponsePB
   * object containing the leader's RPC address.
   * If the master is not a leader, increment 'countResponsesReceived': if the count equals to
   * the number of masters, pass {@link NoLeaderFoundException} into
   * 'responseD' if no one else had called 'responseD' before; otherwise, do nothing.
   */
  final class ConnectToMasterCB implements Callback<Void, ConnectToMasterResponsePB> {
    private final HostAndPort hostAndPort;

    public ConnectToMasterCB(HostAndPort hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    @Override
    public Void call(ConnectToMasterResponsePB r) throws Exception {
      if (!r.getRole().equals(Role.LEADER)) {
        incrementCountAndCheckExhausted();
        return null;
      }
      // We found a leader!
      if (!responseDCalled.compareAndSet(false,  true)) {
        // Someone else already found a leader. This is somewhat unexpected
        // because this means two nodes think they're the leader, but it's
        // not impossible. We'll just ignore it.
        LOG.debug("Callback already invoked, discarding response({}) from {}", r, hostAndPort);
        return null;
      }

      responseD.callback(new ConnectToClusterResponse(hostAndPort, r));
      return null;
    }

    @Override
    public String toString() {
      return "ConnectToMasterCB for " + hostAndPort.toString();
    }
  }

  /**
   * Errback for each ConnectToMaster RPC sent in connectToMaster() above.
   * Stores each exception in 'exceptionsReceived'. Increments 'countResponseReceived': if
   * the count is equal to the number of masters and no one else had called 'responseD' before,
   * pass a {@link NoLeaderFoundException} into 'responseD'; otherwise, do
   * nothing.
   */
  final class ConnectToMasterErrCB implements Callback<Void, Exception> {
    private final HostAndPort hostAndPort;

    public ConnectToMasterErrCB(HostAndPort hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    @Override
    public Void call(Exception e) throws Exception {
      LOG.warn("Error receiving response from {}", hostAndPort, e);
      exceptionsReceived.add(e);
      incrementCountAndCheckExhausted();
      return null;
    }

    @Override
    public String toString() {
      return "ConnectToMasterErrCB for " + hostAndPort.toString();
    }
  }
}
