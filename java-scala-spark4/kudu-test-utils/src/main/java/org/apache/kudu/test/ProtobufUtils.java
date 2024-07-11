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

package org.apache.kudu.test;

import com.google.protobuf.ByteString;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProtobufUtils {

  /**
   * Get a PartitionPB with empty start and end keys.
   * @return a fake partition
   */
  public static Common.PartitionPB.Builder getFakePartitionPB() {
    Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
    partition.setPartitionKeyStart(ByteString.EMPTY);
    partition.setPartitionKeyEnd(ByteString.EMPTY);
    return partition;
  }

  /**
   * Get a PartitionPB with specified start and end keys.
   * @param partitionKeyStart start key
   * @param partitionKeyEnd end key
   * @return a fake partition
   */
  public static Common.PartitionPB.Builder getFakePartitionPB(
          byte[] partitionKeyStart, byte[] partitionKeyEnd) {
    Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
    partition.setPartitionKeyStart(ByteString.copyFrom(partitionKeyStart));
    partition.setPartitionKeyEnd(ByteString.copyFrom(partitionKeyEnd));
    return partition;
  }

  /**
   * Create a InternedReplicaPB based on the passed information.
   * @param tsInfoIndex server's index in the TSInfoPB list
   * @param role server's role in the configuration
   * @return a fake InternedReplicaPB
   */
  public static Master.TabletLocationsPB.InternedReplicaPB.Builder getFakeTabletInternedReplicaPB(
      int tsInfoIndex,  Metadata.RaftPeerPB.Role role) {
    Master.TabletLocationsPB.InternedReplicaPB.Builder internedReplicaBuilder =
        Master.TabletLocationsPB.InternedReplicaPB.newBuilder();
    internedReplicaBuilder.setTsInfoIdx(tsInfoIndex);
    internedReplicaBuilder.setRole(role);
    return internedReplicaBuilder;
  }

  /**
   * Create a TSInfoPB based on the passed information.
   * @param uuid server's identifier
   * @param host server's hostname
   * @param port server's port
   * @return a fake TSInfoPB
   */
  public static Master.TSInfoPB.Builder getFakeTSInfoPB(String uuid, String host, int port) {
    Master.TSInfoPB.Builder tsInfoBuilder = Master.TSInfoPB.newBuilder();
    Common.HostPortPB.Builder hostBuilder = Common.HostPortPB.newBuilder();
    hostBuilder.setHost(host);
    hostBuilder.setPort(port);
    tsInfoBuilder.addRpcAddresses(hostBuilder);
    tsInfoBuilder.setPermanentUuid(ByteString.copyFromUtf8(uuid));
    return tsInfoBuilder;
  }
}
