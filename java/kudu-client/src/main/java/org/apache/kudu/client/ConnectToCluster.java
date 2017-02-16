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
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;
import org.apache.kudu.master.Master.GetTableLocationsResponsePB;
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
  private final Deferred<Master.GetTableLocationsResponsePB> responseD;
  private final int numMasters;

  // Used to avoid calling 'responseD' twice.
  private final AtomicBoolean responseDCalled = new AtomicBoolean(false);

  // Number of responses we've receives: used to tell whether or not we've received
  // errors/replies from all of the masters, or if there are any
  // GetMasterRegistrationRequests still pending.
  private final AtomicInteger countResponsesReceived = new AtomicInteger(0);

  // Exceptions received so far: kept for debugging purposes.
  private final List<Exception> exceptionsReceived =
      Collections.synchronizedList(new ArrayList<Exception>());

  /**
   * Creates an object that holds the state needed to retrieve master table's location.
   * @param masterAddrs Addresses of all master replicas that we want to retrieve the
   *                    registration from.
   * @param responseD Deferred object that will hold the GetTableLocationsResponsePB object for
   *                  the master table.
   */
  ConnectToCluster(List<HostAndPort> masterAddrs) {
    this.masterAddrs = masterAddrs;
    this.responseD = new Deferred<>();
    this.numMasters = masterAddrs.size();
  }

  public Deferred<GetTableLocationsResponsePB> getDeferred() {
    return responseD;
  }

  private static Deferred<ConnectToClusterResponse> getMasterRegistration(
      final TabletClient masterClient, KuduRpc<?> parentRpc,
      long defaultTimeoutMs) {
    // TODO: Handle the situation when multiple in-flight RPCs all want to query the masters,
    // basically reuse in some way the master permits.
    final ConnectToMasterRequest rpc = new ConnectToMasterRequest();
    if (parentRpc != null) {
      rpc.setTimeoutMillis(parentRpc.deadlineTracker.getMillisBeforeDeadline());
      rpc.setParentRpc(parentRpc);
    } else {
      rpc.setTimeoutMillis(defaultTimeoutMs);
    }
    Deferred<ConnectToClusterResponse> d = rpc.getDeferred();
    rpc.attempt++;
    masterClient.sendRpc(rpc);

    // If we are connecting to an older version of Kudu, we'll get a NO_SUCH_METHOD
    // error. In that case, we resend using the older version of the RPC.
    d.addErrback(new Callback<Deferred<ConnectToClusterResponse>, Exception>() {
      @Override
      public Deferred<ConnectToClusterResponse> call(Exception result)
          throws Exception {
        if (result instanceof RpcRemoteException) {
          RpcRemoteException rre = (RpcRemoteException)result;
          if (rre.getErrPB().getCode() == RpcErrorCodePB.ERROR_INVALID_REQUEST &&
              rre.getErrPB().getUnsupportedFeatureFlagsCount() > 0) {
            AsyncKuduClient.LOG.debug("Falling back to GetMasterRegistration() RPC to connect " +
                "to server running Kudu < 1.3.");
            Deferred<ConnectToClusterResponse> newAttempt = rpc.getDeferred();
            assert newAttempt != null;
            rpc.setUseOldMethod();
            masterClient.sendRpc(rpc);
            return newAttempt;
          }
        }
        return Deferred.fromError(result);
      }
    });

    return d;
  }

  /**
   * Retrieve the master registration (see {@link ConnectToClusterResponse}
   * from the leader master.
   *
   * @param masterAddresses the addresses of masters to fetch from
   * @param parentRpc RPC that prompted a master lookup, can be null
   * @param connCache the client's connection cache, used for creating connections
   *                  to masters
   * @param defaultTimeoutMs timeout to use for RPCs if the parentRpc has no timeout
   * @return a Deferred object for the master replica's current registration
   */
  public static Deferred<GetTableLocationsResponsePB> run(
      List<HostAndPort> masterAddresses,
      KuduRpc<?> parentRpc,
      ConnectionCache connCache,
      long defaultTimeoutMs) {
    ConnectToCluster connector = new ConnectToCluster(masterAddresses);

    // Try to connect to each master. The ConnectToCluster instance
    // waits until it gets a good response before firing the returned
    // deferred.
    for (HostAndPort hostAndPort : masterAddresses) {
      Deferred<ConnectToClusterResponse> d;
      TabletClient client = connCache.newMasterClient(hostAndPort);
      if (client == null) {
        String message = "Couldn't resolve this master's address " + hostAndPort.toString();
        LOG.warn(message);
        Status statusIOE = Status.IOError(message);
        d = Deferred.fromError(new NonRecoverableException(statusIOE));
      } else {
        d = getMasterRegistration(client, parentRpc, defaultTimeoutMs);
      }
      d.addCallbacks(connector.callbackForNode(hostAndPort),
          connector.errbackForNode(hostAndPort));
    }
    return connector.responseD;
  }

  /**
   * Creates a callback for a GetMasterRegistrationRequest that was sent to 'hostAndPort'.
   * @see GetMasterRegistrationCB
   * @param hostAndPort Host and part for the RPC we're attaching this to. Host and port must
   *                    be valid.
   * @return The callback object that can be added to the RPC request.
   */
  @VisibleForTesting
  Callback<Void, ConnectToClusterResponse> callbackForNode(HostAndPort hostAndPort) {
    return new GetMasterRegistrationCB(hostAndPort);
  }

  /**
   * Creates an errback for a GetMasterRegistrationRequest that was sent to 'hostAndPort'.
   * @see GetMasterRegistrationErrCB
   * @param hostAndPort Host and port for the RPC we're attaching this to. Used for debugging
   *                    purposes.
   * @return The errback object that can be added to the RPC request.
   */
  @VisibleForTesting
  Callback<Void, Exception> errbackForNode(HostAndPort hostAndPort) {
    return new GetMasterRegistrationErrCB(hostAndPort);
  }

  /**
   * Checks if we've already received a response or an exception from every master that
   * we've sent a GetMasterRegistrationRequest to. If so -- and no leader has been found
   * (that is, 'responseD' was never called) -- pass a {@link NoLeaderFoundException}
   * to responseD.
   */
  private void incrementCountAndCheckExhausted() {
    if (countResponsesReceived.incrementAndGet() == numMasters) {
      if (responseDCalled.compareAndSet(false, true)) {

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
            LOG.warn(String.format(
                "None of the provided masters (%s) is a leader, will retry.",
                allHosts));
            ex = new NoLeaderFoundException(Status.ServiceUnavailable(message));
          } else {
            LOG.warn(String.format(
                "Unable to find the leader master (%s), will retry",
                allHosts));
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
  }

  /**
   * Callback for each GetMasterRegistrationRequest sent in getMasterTableLocations() above.
   * If a request (paired to a specific master) returns a reply that indicates it's a leader,
   * the callback in 'responseD' is invoked with an initialized GetTableLocationResponsePB
   * object containing the leader's RPC address.
   * If the master is not a leader, increment 'countResponsesReceived': if the count equals to
   * the number of masters, pass {@link NoLeaderFoundException} into
   * 'responseD' if no one else had called 'responseD' before; otherwise, do nothing.
   */
  final class GetMasterRegistrationCB implements Callback<Void, ConnectToClusterResponse> {
    private final HostAndPort hostAndPort;

    public GetMasterRegistrationCB(HostAndPort hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    @Override
    public Void call(ConnectToClusterResponse r) throws Exception {
      Master.TabletLocationsPB.ReplicaPB.Builder replicaBuilder =
          Master.TabletLocationsPB.ReplicaPB.newBuilder();

      Master.TSInfoPB.Builder tsInfoBuilder = Master.TSInfoPB.newBuilder()
          .addRpcAddresses(ProtobufHelper.hostAndPortToPB(hostAndPort))
          .setPermanentUuid(ByteString.EMPTY); // required field, but unused for master

      replicaBuilder.setTsInfo(tsInfoBuilder);
      if (r.getRole().equals(Metadata.RaftPeerPB.Role.LEADER)) {
        replicaBuilder.setRole(r.getRole());
        Master.TabletLocationsPB.Builder locationBuilder = Master.TabletLocationsPB.newBuilder();
        locationBuilder.setPartition(
            Common.PartitionPB.newBuilder().setPartitionKeyStart(ByteString.EMPTY)
                                           .setPartitionKeyEnd(ByteString.EMPTY));
        locationBuilder.setTabletId(
            ByteString.copyFromUtf8(AsyncKuduClient.MASTER_TABLE_NAME_PLACEHOLDER));
        locationBuilder.addReplicas(replicaBuilder);
        // No one else has called this before us.
        if (responseDCalled.compareAndSet(false, true)) {
          responseD.callback(
              Master.GetTableLocationsResponsePB.newBuilder().addTabletLocations(
                  locationBuilder.build()).build()
          );
        } else {
          LOG.debug("Callback already invoked, discarding response(" + r.toString() + ") from " +
              hostAndPort.toString());
        }
      } else {
        incrementCountAndCheckExhausted();
      }
      return null;
    }

    @Override
    public String toString() {
      return "get master registration for " + hostAndPort.toString();
    }
  }

  /**
   * Errback for each GetMasterRegistrationRequest sent in getMasterTableLocations() above.
   * Stores each exception in 'exceptionsReceived'. Increments 'countResponseReceived': if
   * the count is equal to the number of masters and no one else had called 'responseD' before,
   * pass a {@link NoLeaderFoundException} into 'responseD'; otherwise, do
   * nothing.
   */
  final class GetMasterRegistrationErrCB implements Callback<Void, Exception> {
    private final HostAndPort hostAndPort;

    public GetMasterRegistrationErrCB(HostAndPort hostAndPort) {
      this.hostAndPort = hostAndPort;
    }

    @Override
    public Void call(Exception e) throws Exception {
      LOG.warn("Error receiving a response from: " + hostAndPort, e);
      exceptionsReceived.add(e);
      incrementCountAndCheckExhausted();
      return null;
    }

    @Override
    public String toString() {
      return "get master registration errback for " + hostAndPort.toString();
    }
  }
}
