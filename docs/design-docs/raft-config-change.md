<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Kudu Raft configuration change design

For operability and availability reasons, we want to be able to
dynamically change the set of tablet servers that host a given Kudu
tablet. The use cases for this functionality include:

* Replacing a failed tablet server to maintain the desired replication
  factor of tablet data.
* Growing the Kudu cluster over time. This might need "rebalancing" tablet
  locations to even out the load across tablet servers.
* Increasing the replication of one or more tablets of a table if they
  become hot (e.g. in a time series workload, making today’s partitions have a
  higher replication).

## Scope
This document covers the following topics:

* Design and implementation of membership change at the
  consensus level.
* Process for adding / removing a tablet server to / from a running
  tablet configuration.
* Process for moving a tablet replica from one tablet server to
  another.
* Process for restoring availability while attempting to minimize data
  loss after catastrophic failure (permanent failure of a majority of the
  nodes). Since there can be no guarantee or bound on the amount of data that
  may be lost in such a scenario, we only provide a high level approach to
  allow for attempting a manual repair.

## References

[1] [Raft paper](https://raft.github.io/raft.pdf)<br>
[2] [Diego Ongaro's Ph.D. dissertation](https://github.com/ongardie/dissertation#readme)

We reference [2] a lot in this doc.

## Membership change

In Kudu, we change membership following the one-by-one membership change design
[2] from Diego Ongaro’s PhD dissertation. We provide a rough outline of the
one-by-one design as outlined in the dissertation, however this doc is mostly
concerned with the Kudu-specific details and deviations from Raft.

### One-by-one membership change

We can only make one addition or subtraction to the configuration atomically.
Until one such change (i.e. change config transaction) commits or aborts, no
others may be started. This gives us safety guarantees. The proof is outlined
in [2].

### Process for adding a new node to the cluster

This process is executed by a driver, which may be a client program or the
Master. We’ll say the node to be added to the cluster is named `new_node`.

1. Driver initiates execution of Tablet Copy procedure of `new_node` from
   the current leader `copy_source` using an RPC call to the `new_node`.
   Tablet Copy runs to completion, which means all data and logs at the
   time the Tablet Copy was initiated were replicated to `new_node`. Driver
   polls `new_node` for indication that the Tablet Copy process is
   complete.
   <br>
   If the `copy_source` node crashes before Tablet Copy is complete,
   the copy fails and the driver must start the entire process over from
   the beginning. If the driver or `new_node` crashes and the tablet never
   joins the configuration, the Master should eventually delete the abandoned
   tablet replica from `new_node`.
2. Driver invokes the AddServer() RPC call on the leader to add `new_node` as a
   `PRE_FOLLOWER` to the configuration. This is a new role type, which does not
   have voting rights. Replicate this config change through the cluster (does
   not change voting majority). The leader will automatically transition a
   `PRE_FOLLOWER` to a `FOLLOWER` (with voting rights, implying a potential
   majority change) when it detects `new_node` has caught up sufficiently to
   replicate the remaining log entries within an election timeout (see [2]
   section 4.2.1). Several nodes may be in `PRE_FOLLOWER` mode at a given time,
   but when transitioning to `FOLLOWER` the one-by-one rules still apply.
   <br>
   Failure to add the node as a `PRE_FOLLOWER` (usually due to a leader change
   or weakness in the configuration) will require a retry later by the driver.
3. As soon as a replica receives the ConfigChangeRequest it applies the
   configuration change in-memory. It does not wait for commitment to apply the
   change. See rationale in [2] section 4.1.
4. The Tablet Copy session between `new_node` and `copy_source` is
   closed once the config change to transition the node to `PRE_FOLLOWER` has
   been committed. This implies releasing an anchor on the log. Since
   `new_node` is already a member of the configuration receiving log updates,
   it should hold a log anchor on the leader starting at the as-yet
   unreplicated data, so this overlap is safe [TODO: this may not yet be
   implemented, need to check].
5. Eventually the ConfigChangeTransaction is committed and the membership
   change is made durable.

### Config change transaction implementation details

When a config change transaction is received indicating a membership change, we
apply the change as WIP config change without committing it to disk. Consensus
commit of the ChangeConfigTransaction causes us to sync ConsensusMeta to disk
(Raft relies on the log durability but we don’t want to prevent log GC due to
config change entries).

This approach allows us to "roll back" to the last-committed configuration
membership in the case that a change config transaction is aborted and replaced
by the new leader.

### Process for removing a node from the cluster

Removing a given node (let’s call it `doomed_node`) from the cluster follows a
lot of the same rules as adding a node. The procedure is also run by a "driver"
process. Here are the details:

1. Driver invokes a RemoveServer() RPC on the configuration leader indicating
   which server to remove from the configuration.
2. If `doomed_node` is not the configuration leader, the leader pushes the
   membership change through consensus using a ConfigChangeTransaction, with a
   configuration that no longer includes `doomed_node`.
3. If `doomed_node` is the leader, the leader transfers configuration ownership
   to the most up-to-date follower in the configuration using the procedure
   outlined in [2] appendix section 3.10 and returns an RPC reply to the client
   `STEPPING_DOWN`, which means the driver should refresh its meta cache and
   try again later.

### Preventing disruptive servers when removing a member

According to [2] section 4.2.3 we cannot use a "pre-vote check" that does log
matching to prevent disruptive servers, however a pre-vote check that checks
whether the recipient has heard from the leader in the past heartbeat period
should work. An additional benefit to this is that the potential sender will
not continuously increment their term number if the pre-vote check fails. So we
will use such an approach instead of the suggested one.

## Moving a tablet from one server to another

Replacing a tablet server is always done as a series of steps:

1. Add new server, wait for commit.
2. Remove old server, wait for commit.

This may require more design on the Master side. We’ll address that later.

## Restoring availability after catastrophic data loss

In the case of a permanent loss of a majority of a tablet configuration, all
durability and consistency guarantees are lost. Assuming there is at least one
remaining member of the configuration, we may be able to recover some data and
regain configuration availability by replicating the remaining data. However
this is highly dangerous and there is no way back once a manual process such as
this is done.

TODO: This somewhat orthogonal to online configuration changes, maybe move to
another doc.

### Steps:

1. Run a tool to determine the most up-to-date remaining replica.
2. Use Tablet Copy to create additional replicas from the most up-to-date remaining
   replica. Wait for Tablet Copy to complete on all the nodes.
3. Bring all tablet servers hosting the affected tablet offline (TODO: This is
   possible to implement per-tablet but not currently supported)
4. Run tool to rewrite the ConsensusMetadata file per-tablet server to
   forcefully update the configuration membership to add copied
   nodes as followers. TODO: Violates Raft not to append to the log, do we also
   need to do that?
5. Bring the affected tablets / tablet servers back online.
6. Pray?

## Appendix: Idea to add a new member before it has copied all data

The idea here is to take advantage of the fact that nodes can participate in
Raft consensus without actually applying operations to their "state machine"
(database). In other words, a node doesn’t need to have any actual tablet data
on it in order to add useful fault tolerance and latency-leveling properties.
HydraBase calls this mode of follower a "WITNESS".

For example, consider a three node configuration experiencing a failure:

**key**: L = logging, V = voting, E = electable (has up-to-date tablet data), X = down

**t=1**: {LVE} {LVE} {LVE}

Initially, all replicas are logging, voting, and electable. At this
point they can handle a fault of any node.

**t=2**: **{LVE X}** {LVE} {LVE} (majority=2)

If the first replica fails, now we have no further fault tolerance, since the
majority is 2 and only 2 nodes are live. To solve this, we can add a new
replica which is only logging and voting (but would never start an election).
This proceeds in two steps:

**t=3**: {LVE X} {LVE} {LVE} **{LV}** (majority=3)

First, we add the new replica as voting. To add the node, we need a majority of
3/4, so fault tolerance is not improved.

**t=4**: **{L X}** {LVE} {LVE} {LV} (majority = 2, handle 1 fault)

Next, we demote the dead replica from LVE to L, so it no longer participates in
voting. For a server that has just failed, it’s preferable to demote to "L" and
not completely remove from the configuration, because it’s possible (even
likely!) it would actually restart before the new replica has finished
copying. If it does, we have the option of adding it back to the
configuration and cancelling the Tablet Copy.

Because we now have three voting replicas, the majority is 2, so we can handle
a fault of any of the remaining three nodes. After reaching this state, we can
take our time to copy the tablet data to the new replica. At some point, the
new replica has finished copying its data snapshot, and then replays its own
log (as it would during bootstrap) until it is acting like a normal replica.
Once it is a normal replica, it is now allowed to start elections.

**t=5**: {L X} {LVE} {LVE} **{LVE}** (majority = 2, handle 1 fault)

At this point we are fully healed.

### Advantages:

The important advantage to this idea is that, when a node fails, we can very
quickly regain our fault tolerance (on the order of two round-trips in order to
perform two config changes). If we have to wait for the new tablet to bootstrap
and replay all data, it may be tens of minutes or even hours before regaining
fault tolerance.

As an example, consider the case of a four-node cluster, each node having 1TB
of replica data. If a node fails, then its 1TB worth of data must be transferred
among the remaining nodes, so we need to wait for 300+GB of data to transfer,
which could take up to an hour. During that hour, we would have no
latency-leveling on writes unless we did something like the above.

### Disadvantages:

Is this more complex?
