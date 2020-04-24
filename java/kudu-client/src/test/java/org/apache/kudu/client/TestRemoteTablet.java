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

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.test.ProtobufUtils;
import org.apache.kudu.test.junit.RetryRule;

public class TestRemoteTablet {
  private static final String kClientLocation = "/fake-client";
  private static final String kLocation = "/fake-noclient";
  private static final String kNoLocation = "";
  private static final String[] kUuids = { "uuid-0", "uuid-1", "uuid-2" };

  @Rule
  public RetryRule retryRule = new RetryRule();

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
    {
      // Let's examine a tablet where the first UUID is local to the client,
      // but no UUID is in the same location.
      RemoteTablet tablet = getTablet(0, 0, -1);

      // If the client has no location, we should pick the local server.
      assertEquals(kUuids[0], tablet.getClosestServerInfo(kNoLocation).getUuid());

      // NOTE: if the client did have a location, because the test replicas are
      // assigned a different default location, they aren't considered local,
      // so we would select one at random.
    }

    {
      // Let's examine a tablet where the first UUID is local to the client,
      // and the second is in the same location.
      RemoteTablet tablet = getTablet(0, 0, 1);

      // If the client has no location, we should pick the local server.
      assertEquals(kUuids[0], tablet.getClosestServerInfo(kNoLocation).getUuid());

      // If the client does have a location, we should pick the one in the same
      // location.
      assertEquals(kUuids[1], tablet.getClosestServerInfo(kClientLocation).getUuid());
    }

    {
      // Let's examine a tablet where the first UUID is local to the client and
      // is also in the same location.
      RemoteTablet tablet = getTablet(0, 0, 0);

      // If the client has no location, we should pick the local server.
      assertEquals(kUuids[0], tablet.getClosestServerInfo(kNoLocation).getUuid());

      // If the client does have a location, we should pick the one in the same
      // location.
      assertEquals(kUuids[0], tablet.getClosestServerInfo(kClientLocation).getUuid());
    }
  }

  @Test
  public void testNoLocalOrSameLocationReplica() {
    RemoteTablet tablet = getTablet(0, -1, -1);

    // We just care about getting one back.
    assertNotNull(tablet.getClosestServerInfo(kClientLocation).getUuid());
  }

  @Test
  public void testReplicaWithNoValidLocation() {
    RemoteTablet tablet = getTablet(0, 1, 2);

    // Test removing all tablet servers doesn't break.
    for (String uuid : kUuids) {
      assertTrue(tablet.removeTabletClient(uuid));
    }
    assertNull(tablet.getLeaderServerInfo());
    assertNull(tablet.getClosestServerInfo(kNoLocation));
    assertNull(tablet.getClosestServerInfo(kClientLocation));
  }

  @Test
  public void testReplicaSelection() {
    {
      RemoteTablet tablet = getTablet(0, 1, 2);

      // LEADER_ONLY picks the leader even if there's a local replica.
      assertEquals(kUuids[0],
          tablet.getReplicaSelectedServerInfo(ReplicaSelection.LEADER_ONLY, kClientLocation)
              .getUuid());

      // Since there are locations assigned, CLOSEST_REPLICA picks the replica
      // in the same location, even if there's a local one.
      assertEquals(kUuids[2],
          tablet.getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA, kClientLocation)
              .getUuid());
    }

    {
      RemoteTablet tablet = getTablet(0, 1, -1);

      // LEADER_ONLY picks the leader even if there's a local replica.
      assertEquals(kUuids[0],
          tablet.getReplicaSelectedServerInfo(ReplicaSelection.LEADER_ONLY, kClientLocation)
              .getUuid());

      // NOTE: the test replicas are assigned a default location. So, even if
      // there are local replicas, because they are in different locations than
      // the client, with CLOSEST_REPLICA, a replica is chosen at random.
    }

    {
      RemoteTablet tablet = getTablet(0, -1, 1);

      // LEADER_ONLY picks the leader even if there's a replica with the same location.
      assertEquals(kUuids[0],
          tablet.getReplicaSelectedServerInfo(ReplicaSelection.LEADER_ONLY, kClientLocation)
              .getUuid());

      // CLOSEST_REPLICA picks the replica in the same location.
      assertEquals(kUuids[1],
          tablet.getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA, kClientLocation)
              .getUuid());
    }
  }

  // AsyncKuduClient has methods like scanNextRows, keepAlive, and closeScanner that rely on
  // RemoteTablet.getReplicaSelectedServerInfo to be deterministic given the same state.
  // This ensures follow up calls are routed to the same server with the scanner open.
  // This test ensures that remains true.
  @Test
  public void testGetReplicaSelectedServerInfoDeterminism() {
    // There's a local leader replica.
    RemoteTablet tabletWithLocal = getTablet(0, 0, 0);
    verifyGetReplicaSelectedServerInfoDeterminism(tabletWithLocal);

    // There's a leader in the same location as the client.
    RemoteTablet tabletWithSameLocation = getTablet(0, -1, 0);
    verifyGetReplicaSelectedServerInfoDeterminism(tabletWithSameLocation);

    // There's no local replica or replica in the same location.
    RemoteTablet tabletWithRemote = getTablet(0, -1, -1);
    verifyGetReplicaSelectedServerInfoDeterminism(tabletWithRemote);
  }

  private void verifyGetReplicaSelectedServerInfoDeterminism(RemoteTablet tablet) {
    String init = tablet
        .getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA, kClientLocation)
        .getUuid();
    for (int i = 0; i < 10; i++) {
      String next = tablet
          .getReplicaSelectedServerInfo(ReplicaSelection.CLOSEST_REPLICA, kClientLocation)
          .getUuid();
      assertEquals("getReplicaSelectedServerInfo was not deterministic", init, next);
    }
  }

  @Test
  public void testToString() {
    RemoteTablet tablet = getTablet(0, 1, -1);
    assertEquals("fake tablet@[uuid-0(host:1000)[L],uuid-1(host:1001),uuid-2(host:1002)]",
        tablet.toString());
  }

  private RemoteTablet getTablet(int leaderIndex) {
    return getTablet(leaderIndex, -1, -1);
  }

  // Returns a three-replica remote tablet that considers the given indices of
  // replicas to be leader, local to the client, and in the same location.
  static RemoteTablet getTablet(int leaderIndex,
                                int localReplicaIndex,
                                int sameLocationReplicaIndex) {
    Partition partition = ProtobufHelper.pbToPartition(ProtobufUtils.getFakePartitionPB().build());
    List<LocatedTablet.Replica> replicas = new ArrayList<>();
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
      String location = i == sameLocationReplicaIndex ? kClientLocation : kLocation;
      servers.add(new ServerInfo(uuid,
                                 new HostAndPort("host", 1000 + i),
                                 addr,
                                 location));
      Metadata.RaftPeerPB.Role role = leaderIndex == i ? Metadata.RaftPeerPB.Role.LEADER :
                                                         Metadata.RaftPeerPB.Role.FOLLOWER;
      replicas.add(new LocatedTablet.Replica("host", i, role, null));
    }

    return new RemoteTablet("fake table", "fake tablet", partition, replicas, servers);
  }
}
