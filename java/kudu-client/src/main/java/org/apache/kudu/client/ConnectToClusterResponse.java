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

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;

import org.apache.kudu.Common.PartitionPB;
import org.apache.kudu.master.Master.ConnectToMasterResponsePB;
import org.apache.kudu.master.Master.GetTableLocationsResponsePB;
import org.apache.kudu.master.Master.TSInfoPB;
import org.apache.kudu.master.Master.TabletLocationsPB;
import org.apache.kudu.master.Master.TabletLocationsPB.ReplicaPB;

/**
 * The aggregated response after connecting to a cluster. This stores the
 * identity of the leader master as well as the response from that master.
 */
class ConnectToClusterResponse {
  private static final ByteString FAKE_TABLET_ID = ByteString.copyFromUtf8(
      AsyncKuduClient.MASTER_TABLE_NAME_PLACEHOLDER);

  /** The host and port of the master that is currently leader */
  private final HostAndPort leaderHostAndPort;
  /** The response from that master */
  private final ConnectToMasterResponsePB connectResponse;

  public ConnectToClusterResponse(HostAndPort hostAndPort,
      ConnectToMasterResponsePB connectResponse) {
    super();
    this.leaderHostAndPort = hostAndPort;
    this.connectResponse = connectResponse;
  }

  public ConnectToMasterResponsePB getConnectResponse() {
    return connectResponse;
  }

  public HostAndPort getLeaderHostAndPort() {
    return leaderHostAndPort;
  }

  /**
   * Return the location of the located leader master as if this had been a normal
   * tablet lookup. This is necessary so that we can cache the master location as
   * if it were a tablet.
   */
  public GetTableLocationsResponsePB getAsTableLocations() {
    String fakeUuid = AsyncKuduClient.getFakeMasterUuid(leaderHostAndPort);
    return GetTableLocationsResponsePB.newBuilder()
        .addTabletLocations(TabletLocationsPB.newBuilder()
            .setPartition(PartitionPB.newBuilder()
                .setPartitionKeyStart(ByteString.EMPTY)
                .setPartitionKeyEnd(ByteString.EMPTY))
            .setTabletId(FAKE_TABLET_ID)
            .addReplicas(ReplicaPB.newBuilder()
                .setTsInfo(TSInfoPB.newBuilder()
                    .addRpcAddresses(ProtobufHelper.hostAndPortToPB(leaderHostAndPort))
                    .setPermanentUuid(ByteString.copyFromUtf8(fakeUuid)))
                .setRole(connectResponse.getRole()))).build();
  }
}
