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

import com.google.protobuf.ByteString;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestRemoteTablet {

  @Test
  public void testLeaderLastRemovedLast() {
    RemoteTablet tablet = getTablet(2);

    // Demote the wrong leader, no-op.
    assertEquals("2", tablet.getLeaderUUID());
    tablet.demoteLeader("1");
    assertEquals("2", tablet.getLeaderUUID());

    // Tablet at server 1 was deleted.
    assertTrue(tablet.removeTabletClient("1"));
    assertEquals("2", tablet.getLeaderUUID());

    // Simulate another thread trying to remove 1.
    assertFalse(tablet.removeTabletClient("1"));

    // Tablet at server 0 was deleted.
    assertTrue(tablet.removeTabletClient("0"));
    assertEquals("2", tablet.getLeaderUUID());

    // Leader was demoted.
    tablet.demoteLeader("2");
    assertEquals(null, tablet.getLeaderUUID());

    // Simulate another thread doing the same.
    tablet.demoteLeader("2");
    assertEquals(null, tablet.getLeaderUUID());
  }

  @Test
  public void testLeaderLastRemovedFirst() {
    RemoteTablet tablet = getTablet(2);

    // Test we can remove it.
    assertTrue(tablet.removeTabletClient("2"));
    assertEquals(null, tablet.getLeaderUUID());

    // Test demoting it doesn't break anything.
    tablet.demoteLeader("2");
    assertEquals(null, tablet.getLeaderUUID());
  }

  @Test
  public void testLeaderFirst() {
    RemoteTablet tablet = getTablet(0);

    // Test we can remove it.
    assertTrue(tablet.removeTabletClient("0"));
    assertEquals(null, tablet.getLeaderUUID());

    // Test demoting it doesn't break anything.
    tablet.demoteLeader("0");
    assertEquals(null, tablet.getLeaderUUID());

    // Test removing a server with no leader doesn't break.
    assertTrue(tablet.removeTabletClient("2"));
  }

  @Test
  public void testLocalReplica() {
    RemoteTablet tablet = getTablet(0, 0);

    assertEquals("0", tablet.getClosestUUID());
  }

  @Test
  public void testNoLocalReplica() {
    RemoteTablet tablet = getTablet(0, -1);

    // We just care about getting one back.
    assertNotNull(tablet.getClosestUUID());
  }

  private RemoteTablet getTablet(int leaderIndex) {
    return getTablet(leaderIndex, -1);
  }

  private RemoteTablet getTablet(int leaderIndex, int localReplicaIndex) {
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();

    tabletPb.setPartition(TestUtils.getFakePartitionPB());
    tabletPb.setTabletId(ByteString.copyFromUtf8("fake tablet"));
    List<ServerInfo> servers = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String uuid = i + "";
      servers.add(new ServerInfo(uuid, "host", i, i == localReplicaIndex));
      tabletPb.addReplicas(TestUtils.getFakeTabletReplicaPB(
          uuid, "host", i,
          leaderIndex == i ? Metadata.RaftPeerPB.Role.LEADER : Metadata.RaftPeerPB.Role.FOLLOWER));
    }

    return new RemoteTablet("fake table", tabletPb.build(), servers);
  }
}
