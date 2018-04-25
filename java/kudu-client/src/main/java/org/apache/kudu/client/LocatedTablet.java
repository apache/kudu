/*
 *
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

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.consensus.Metadata.RaftPeerPB.Role;
import org.apache.kudu.master.Master.TabletLocationsPB.ReplicaPB;

/**
 * Information about the locations of tablets in a Kudu table.
 * This should be treated as immutable data (it does not reflect
 * any updates the client may have heard since being constructed).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocatedTablet {
  private final Partition partition;
  private final byte[] tabletId;

  private final List<Replica> replicas;

  LocatedTablet(RemoteTablet tablet) {
    partition = tablet.getPartition();
    tabletId = tablet.getTabletIdAsBytes();
    replicas = tablet.getReplicas();
  }

  public List<Replica> getReplicas() {
    return replicas;
  }

  public Partition getPartition() {
    return partition;
  }

  /**
   * DEPRECATED: use {@link #getPartition()}
   */
  @Deprecated
  public byte[] getStartKey() {
    return getPartition().getPartitionKeyStart();
  }

  /**
   * DEPRECATED: use {@link #getPartition()}
   */
  @Deprecated()
  public byte[] getEndKey() {
    return getPartition().getPartitionKeyEnd();
  }

  public byte[] getTabletId() {
    return tabletId;
  }

  /**
   * Return the current leader, or null if there is none.
   */
  public Replica getLeaderReplica() {
    return getOneOfRoleOrNull(Role.LEADER);
  }

  /**
   * Return the first occurrence for the given role, or null if there is none.
   */
  private Replica getOneOfRoleOrNull(Role role) {
    for (Replica r : replicas) {
      if (r.getRoleAsEnum().equals(role)) {
        return r;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return Bytes.pretty(tabletId) + " " + partition.toString();
  }

  /**
   * One of the replicas of the tablet.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Replica {
    private final ReplicaPB pb;

    Replica(ReplicaPB pb) {
      this.pb = pb;
    }

    public String getRpcHost() {
      if (pb.getTsInfo().getRpcAddressesList().isEmpty()) {
        return null;
      }
      return pb.getTsInfo().getRpcAddressesList().get(0).getHost();
    }

    public Integer getRpcPort() {
      if (pb.getTsInfo().getRpcAddressesList().isEmpty()) {
        return null;
      }
      return pb.getTsInfo().getRpcAddressesList().get(0).getPort();
    }

    private Role getRoleAsEnum() {
      return pb.getRole();
    }

    public String getRole() {
      return pb.getRole().toString();
    }

    @Override
    public String toString() {
      return pb.toString();
    }
  }
}
