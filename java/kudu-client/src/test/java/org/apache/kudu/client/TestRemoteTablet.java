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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.kudu.test.ProtobufUtils;
import org.junit.Test;

import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;

public class TestRemoteTablet {
  private static final String[] kUuids = { "uuid-0", "uuid-1", "uuid-2" };

  @Test
  public void testLeaderLastRemovedLast() {
    RemoteTablet tablet = getTablet(2);

    // Demote the wrong leader, no-op.
    assertEquals(kUuids[2], tablet.getLeaderServerInfo().getUuid());
    tablet.demoteLeader(kUuids[1]);
    assertEquals(kUuids[2], tablet.getLeaderServerInfo().getUuid());

    // Tablet at server 1 was deleted.
    assertTrue(tablet.removeTabletClient(kUuids[1]));
    assertEquals(kUuids[2], tablet.getLeaderServerInfo().getUuid());

    // Simulate another thread trying to remove 1.
    assertFalse(tablet.removeTabletClient(kUuids[1]));

    // Tablet at server 0 was deleted.
    assertTrue(tablet.removeTabletClient(kUuids[0]));
    assertEquals(kUuids[2], tablet.getLeaderServerInfo().getUuid());

    // Leader was demoted.
    tablet.demoteLeader(kUuids[2]);
    assertNull(tablet.getLeaderServerInfo());

    // Simulate another thread doing the same.
    tablet.demoteLeader(kUuids[2]);
    assertNull(tablet.getLeaderServerInfo());
  }

  @Test
  public void testLeaderLastRemovedFirst() {
    RemoteTablet tablet = getTablet(2);

    // Test we can remove it.
    assertTrue(tablet.removeTabletClient("uuid-2"));
    assertNull(tablet.getLeaderServerInfo());

    // Test demoting it doesn't break anything.
    tablet.demoteLeader("uuid-2");
    assertNull(tablet.getLeaderServerInfo());
  }

  @Test
  public void testLeaderFirst() {
    RemoteTablet tablet = getTablet(0);

    // Test we can remove it.
    assertTrue(tablet.removeTabletClient("uuid-0"));
    assertNull(tablet.getLeaderServerInfo());

    // Test demoting it doesn't break anything.
    tablet.demoteLeader("uuid-0");
    assertNull(tablet.getLeaderServerInfo());

    // Test removing a server with no leader doesn't break.
    assertTrue(tablet.removeTabletClient("uuid-2"));
  }

  @Test
  public void testLocalReplica() {
    RemoteTablet tablet = getTablet(0, 0);

    assertEquals(kUuids[0], tablet.getClosestServerInfo().getUuid());
  }

  @Test
  public void testNoLocalReplica() {
    RemoteTablet tablet = getTablet(0, -1);

    // We just care about getting one back.
    assertNotNull(tablet.getClosestServerInfo().getUuid());
  }

  @Test
  public void testReplicaSelection() {
    RemoteTablet tablet = getTablet(0, 1);

    assertEquals(kUuids[0],
        tablet.getReplicaSelectedServerInfo(ReplicaSelection.LEADER_ONLY).getUuid());
    assertEquals(kUuids[1],
        tablet.getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA).getUuid());
  }

  // AsyncKuduClient has methods like scanNextRows, keepAlive, and closeScanner that rely on
  // RemoteTablet.getReplicaSelectedServerInfo to be deterministic given the same state.
  // This ensures follow up calls are routed to the same server with the scanner open.
  // This test ensures that remains true.
  @Test
  public void testGetReplicaSelectedServerInfoDeterminism() {
    RemoteTablet tabletWithLocal = getTablet(0, 0);
    verifyGetReplicaSelectedServerInfoDeterminism(tabletWithLocal);

    RemoteTablet tabletWithRemote = getTablet(0, -1);
    verifyGetReplicaSelectedServerInfoDeterminism(tabletWithRemote);
  }

  private void verifyGetReplicaSelectedServerInfoDeterminism(RemoteTablet tablet) {
    String init = tablet.getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA).getUuid();
    for (int i = 0; i < 10; i++) {
      String next = tablet.getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA).getUuid();
      assertEquals("getReplicaSelectedServerInfo was not deterministic", init, next);
    }
  }

  @Test
  public void testToString() {
    RemoteTablet tablet = getTablet(0, 1);
    assertEquals("fake tablet@[uuid-0(host:1000)[L],uuid-1(host:1001),uuid-2(host:1002)]",
        tablet.toString());
  }

  private RemoteTablet getTablet(int leaderIndex) {
    return getTablet(leaderIndex, -1);
  }

  static RemoteTablet getTablet(int leaderIndex, int localReplicaIndex) {
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();

    tabletPb.setPartition(ProtobufUtils.getFakePartitionPB());
    tabletPb.setTabletId(ByteString.copyFromUtf8("fake tablet"));
    List<ServerInfo> servers = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      InetAddress addr;
      try {
        if (i == localReplicaIndex) {
          addr = InetAddress.getByName("127.0.0.1");
        } else {
          addr = InetAddress.getByName("1.2.3.4");
        }
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }

      String uuid = kUuids[i];
      servers.add(new ServerInfo(uuid,
                                 new HostAndPort("host", 1000 + i),
                                 addr,
                                 /*location=*/""));
      tabletPb.addReplicas(ProtobufUtils.getFakeTabletReplicaPB(
          uuid, "host", i,
          leaderIndex == i ? Metadata.RaftPeerPB.Role.LEADER : Metadata.RaftPeerPB.Role.FOLLOWER));
    }

    return new RemoteTablet("fake table", tabletPb.build(), servers);
  }
}
