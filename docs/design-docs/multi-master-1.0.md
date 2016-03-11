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

# Multi-master support for Kudu 1.0

## Background

Kudu's design avoids a single point of failure via multiple Kudu masters.
Just as with tablets, master metadata is persisted to disk and replicated
via Raft consensus, and so a deployment of **2N+1** masters can tolerate up
to **N** failures.

By the time Kudu's first beta launched, support for multiple masters had
been implemented but was too fragile to be anything but experimental. The
rest of this document describes the gaps that must be filled before
multi-master support is ready for production, and lays out a plan for how
to fill them.

## Gaps in the master

### Current design

At startup, a master Raft configuration will elect a leader master. The
leader master is responsible for servicing both tserver heartbeats as well
as client requests. The follower masters participate in Raft consensus and
replicate metadata, but are otherwise idle. Any heartbeats or client
requests they receive are rejected.

All persistent master metadata is stored in a single replicated tablet.
Every row in this tablet represents either a table or a tablet. Table
records include unique table identifiers, the table's schema, and other bits
of information. Tablet records include a unique identifier, the tablet's
Raft configuration, and other information.

What master metadata is replicated?

1. Table and tablet existence, via **CreateTable()** and **DeleteTable()**.
   Every new tablet record also includes an initial Raft configuration.
2. Schema changes, via **AlterTable()** and tserver heartbeats.
3. Tablet server Raft configuration changes, via tserver heartbeats. These
   include both the list of peers (may have changed due to
   under-replication) as well as the current leader (may have changed due to
   an election).

Scanning the master tablet for every heartbeat or request would be slow,
so the leader master caches all master metadata in memory. The caches are
only updated after a metadata change is successfully replicated; in this
way they are always consistent with the on-disk tablet. When a new leader
master is elected, it scans the entire master tablet and uses the metadata
to rebuild its in-memory caches.

To understand how the in-memory caches work, let's start with the different
kinds of information that are cached:

1. Tablet server instance. Is uniquely identified by the tserver's UUID and
   includes general metadata about the tserver, how recently this master
   received a heartbeat from the tserver, and proxy objects that the master
   uses to communicate with the tserver.
2. Table instance. Is uniquely identified by the table's ID and includes
   general metadata about the table.
3. Tablet instance. Is uniquely identified by the tablet's ID and includes
   general metadata about the tablet along with its current replicas.

Now, let's describe the various data structures that store this information:

1. Global: map of tserver UUID to tserver instance.
2. Global: map of table ID to table instance. All tables are present, even
   deleted tables.
3. Global: map of table name to table instance. Deleted tables are omitted,
   otherwise a new table could collide with a deleted one.
4. Global: map of tablet ID to tablet instance. All tablets are present,
   even deleted ones.
5. Per-table instance: map of tablet ID to tablet instance. Deleted tablets
   are retained here but are not inserted the next time master metadata is
   reloaded from disk.
6. Per-tablet instance: map of tserver UUID to tablet replica.

With few exceptions (detailed below), these caches behave more or less as
one would expect. For example, a successful **CreateTable()** yields a
table, tablet, and replica instances, all of which are mapped accordingly. A
heartbeat from a tserver may update a tablet's replica map if that tablet
elected a new leader replica. And so on.

All tservers start up with location information for the entire master Raft
configuration, but are only responsible for heartbeating to the leader
master. Prior to the first heartbeat, a tserver must determine which of the
masters is the leader master. After that, the tserver will send heartbeats
to that master until such a time that it fails or steps down, at which point
the tserver must determine the new leader master and the cycle
repeats. Follower masters ignore all heartbeats.

The information communicated in a tserver's heartbeat varies. Normally the
enclosed tablet report is "incremental" in that it only includes tablets
that have undergone Raft configuration or role changes. In rarer
circumstances the tablet report will be "full" and include every
tablet. Importantly, incremental reports are "edge" triggered; that is,
after a tablet is included in an incremental report **N**, it is omitted
from incremental report **N+1**. Full tablet reports are sent when one of
the following conditions is met:

1. The master requests it in the previous heartbeat response, because the
   master is seeing this tserver for the first time.
2. The tserver has just started.

Clients behave much like tservers: they are configured a priori with the
entire master Raft configuration, must always communicate with the leader
master, and will retry their requests until the new leader master is found.

### Known issues

#### KUDU-1358: **CreateTable()** may fail following a master election

One of the aforementioned in-memory caches keeps track of all known tservers
and their "liveness" (i.e. how likely they are to be alive). This cache is
*NOT* rebuilt using persistent master metadata; instead, it is updated
whenever an unknown tserver heartbeats to the leader master.
**CreateTable()** requests use this cache to determine whether a new table
can be satisfied using the current cluster size; if not, the request is
rejected.

Right after a master election, the new leader master may have a cold tserver
cache if it's never seen any heartbeats before. Until an entire heartbeat
interval has elapsed, this cold cache may harm **CreateTable()** requests:

1. If insufficient tservers are known to the master, the request will fail.
2. If not all tservers are known to the master, the cluster may become
   unbalanced as tablets pile up on a particular subset of tservers.

#### KUDU-1353: **AlterTable()** may get stuck

Another in-memory cache that is *NOT* rebuilt using persistent master
metadata is the per-tablet replica map. These maps are used to satisfy
client **GetTableLocations()** and **GetTabletLocations()**
requests. They're also used to determine the leader replica for various
master to tserver RPC requests, such as in response to a client's
**AlterTable()**. Each of these maps is updated during a tserver heartbeat
using a tablet's latest Raft configuration.

If a new leader master is elected and a tserver is already known to it
(perhaps because the master had been leader before), every heartbeat it
receives from that tserver will include an empty tablet report. This is
problematic for the per-tablet replica maps, which will remain empty until
tablets show up in a tablet report.

Empty per-tablet replica maps in an otherwise healthy leader master can
cause a variety of failures:

1. **AlterTable()** requests may time out.
2. **GetTableLocations()** and **GetTabletLocations()** requests may yield
   no replica location information.
3. **DeleteTable()** requests will succeed but won't delete any tablets
   from tservers until those tablets find their way into tablet reports.

#### KUDU-1374: Operations triggered by heartbeats may go unperformed

As described earlier, the inclusion or exclusion of a tablet in an
incremental tablet report is edge-triggered, and may result in a state
changing operation on the tserver, communicated via out-of-band RPC. This
RPC is retried until it is successful. However, if the leader master dies
*after* it is able to respond to the tserver's heartbeat but *before* the
out-of-band RPC is sent, the edge-triggered tablet report may be missed, and
the state changing operation will not be performed until the next time the
tablet is included in a tablet report. As tablet report inclusion criteria
is narrow, operations may be "missed" for quite some time.

These operations include:
1. Some tablet deletions, such as tablets belonging to orphaned tables, or
   tablets whose deletion RPCs were sent and failed during an earlier
   **DeleteTable()** request.
2. Some tablet alters, such as tablets whose alter RPCs were sent and failed
   during an earlier **AlterTable()** request.
3. Config changes sent due to under-replicated tablets.

#### KUDU-495: Masters may abort if replication fails

Some master operations will crash the master when replication fails. That's
because they were implemented with local consensus in mind, wherein a
replication failure is indicative of a disk failure and recovery is
unlikely. With multiple masters and Raft consensus, replication may fail if
the current leader master is no longer the leader master (e.g. it was
partitioned from the rest of its Raft configuration, which promptly elected
a new leader), raising the likelihood of a master crash.

### Unimplemented features

#### Master Raft configuration changes

It's not currently possible to add or remove a master from an active Raft
configuration.

It would be nice to implement this for Kudu 1.0, but it's not a strict
requirement.

#### KUDU-500: allow followers to handle read-only client operations

Currently followers reject any client operations as the expectation is that
clients communicate with the leader master. As a performance optimization,
followers could handle certain "safe" operations (i.e. read-only requests),
but follower masters must do a better job of keeping their in-memory caches
up-to-date before this change is made. Moreover, operations should include
an indication of how stale the follower master's information is allowed to
be for it to be considered acceptable by the client.

It would be nice to implement this for Kudu 1.0, but it's not a strict
requirement.

## Gaps in the clients

(TBD)

TODO: JD says the code that detects the current master was partially removed
from the Java client because it was buggy. Needs further investigation.

## Goals

1. The plan must address all of the aforementioned known issues.
2. If possible, the plan should implement the missing features too.
3. The plan's scope should be minimized, provided that doesn't complicate
   future implementations. In other words, the plan should not force
   backwards incompatibilities down the line.

## Plan

### Heartbeat to all masters

This is probably the most effective way to address KUDU-1358, and, as a side
benefit, helps implement the "verify cluster connectivity" feature. [This
old design document](old-multi-master-heartbeating.md) describes KUDU-1358
and its solutions in more detail.

With this change, tservers no longer need to "follow the leader" as they will
heartbeat to every master. However, a couple things need to change for this
to work correctly:

#### Follower masters must process heartbeats, at least in part

Basically, they're only intended to refresh the master's notion of the
tserver's liveness; table and tablet information is still replicated from
the leader master and should be ignored if found in the heartbeat.

#### All state changes taken by a tserver must be fenced

That is, the master and/or tserver must enforce that all actions take effect
iff they were sent by the master that is currently the leader.

After an exhaustive audit of all master state changes (see appendix A), it
was determined that the current protection mechanisms built into each RPC
are sufficient to provide fencing. The one exception is orphaned replica
deletion done in response to a heartbeat. To protect against that, true
orphans (i.e. tablets for which no persistent record exists) will not be
deleted at all. As the master retains deleted table/tablet metadata in
perpetuity, this should ensure that true orphans appear only under drastic
circumstances, such as a tserver that heartbeats to the wrong cluster.

The following protection mechanisms are here for historical record; they
will not be implemented.

##### Alternative fencing mechanisms

One way to do this is by including the current term in every requested state
change and hearbeat response. Each tserver maintains the current term in
memory, reset whenever a heartbeat response or RPC includes a later term
(thus also serving as a "there is a new leader master" notification). If a
state change request includes an older term, it is rejected. When a tserver
first starts up, it initializes the current term with whatever term is
indicated in the majority of the heartbeat responses. In this way it
can protect itself from a "rogue master" at startup without having to
persist the current term to disk.

An alternative to the above fencing protocol is to ensure that the leader
master replicates via Raft before triggering a state change. It doesn't
matter what is replicated; a successful replication asserts that this master
is still the leader. However, our Raft implementation doesn't currently
allow for replicating no-ops (i.e. log entries that needn't be persisted).
Moreover, this is effectively an implementation of "leader leases" (in that
a successful replication grants the leader a "lease" to remain leader for at
least one Raft replication interval), but one that the rest of Kudu must be
made aware of in order to be fully robust.

### Send full heartbeats to newly elected leader masters

To address KUDU-1374, when a tserver passively detects that there's a new
leader master (i.e. step #2 above), it should send it a full heartbeat. This
will ensure that any heartbeat-triggered actions intended but not taken by
the old leader master are reinitiated by the new one.

### Ensure each logical operation is replicated as one batch

Kudu doesn't yet support atomic multi-row transactions, but all row
operations bound for one tablet and batched into one Write RPC are combined
into one logical transaction. This property is useful for multi-master
support as all master metadata is encapsulated into a single tablet. With
some refactoring, it is possible to ensure that any logical operation
(e.g. creating a table) is encapsulated into a single RPC. Doing so would
obviate the need for "roll forward" repair of partially replicated
operations during metadata load and is necessary to address KUDU-495.

Some repair is still necessary for table-wide operations. These complete on
a tablet by tablet basis and thus it is possible for partially created,
altered, or deleted tables to exist at any point in time. However, the
repair is a natural course of action taken by the leader master:

1. During a tablet report: for example, if the master sees a tablet without
   the latest schema, it'll send that tablet an idempotent alter RPC.
   Further, thanks to the above full heartbeat change, the new leader master
   will have an opportunity to roll forward such tables on master failover.
2. In the background: the master will periodically scan its in-memory state
   looking for tablets that have yet to be reported. If it finds one, it
   will be given a "nudge"; the master will send a create tablet RPC to the
   appropriate tserver. This scanning continues in a new leader master
   following master failover.

All batched logical operations include one table entry and *N* tablet
entries, where *N* is the number of tablets in the table. These entries are
encapsulated in a WriteRequestPB that is replicated by the leader master to
follower masters. When *N* is very large, it is conceivable for the
WriteRequestPB to exceed the maximum size of a Kudu RPC. To determine just
how likely this is, replication RPC sizes were measured in the creation of a
table with 1000 tablets and a simple three-column schema. The results: the
replication RPC clocked in at ~117 KB, a far cry from the 8 MB maximum RPC
size. Thus, a batch-based approach should not be unreasonable for today's
scale targets.

### Remove some unnecessary in-memory caches

To fix KUDU-1353, the per-tablet replica locations could be removed entirely.
The same information is already present in each tablet instance, just not in
an easy to use map form. The only downside is that operations that previously
used the cached locations would need to perform more lookups into the tserver
map, to resolve tserver UUIDs into instances. We think this is a reasonable
trade-off, however, as the tserver map should be hot.

An alternative is to rebuild the per-tablet replica locations on metadata
load, but the outright removal of that cached data is a simpler solution.

The tserver cache could also be rebuilt, but:

1. It will be incomplete, as only the last known RPC address (and none of
   the HTTP addresses) is persisted. Additionally, addressing KUDU-418 may
   require the removal of the last known RPC address from the persisted
   state, at which point there's nothing worth rebuilding.
2. The tserver cache is expected to be warm from the moment any master
   (including followers) starts up due to "Heartbeat to all masters" above.

Note: in-memory caches belonging to a former leader master will, by
definition, contain stale information. These caches could be
cleared following an election, but it shouldn't matter either way as this
master is no longer servicing client requests or tserver heartbeats.

### Ensure strict ordering for all state changes

Master state change operations should adhere to the following contract:

1. Acquire locks on the relevant table and/or tablets. Which locks and
   whether the locks are held for reading or writing depends on the
   operation. For example, **DeleteTable()** acquires locks for writing on
   the table and all of its tablets. Table locks must be acquired before
   tablet locks.
2. Mutate in-memory state belonging to the table and/or tablets. These
   mutations are made via COW so that concurrent readers continue to see
   only "committed" data.
3. Replicate all of the mutations via Raft consensus in a single batch. If
   the replication fails, the overall operation fails, the mutations are
   discarded, and the locks are released.
4. Replication has succeeded; the operation may not fail beyond this point.
5. Commit the mutations. If both table and tablet mutations are committed,
   tablet mutations must come first. Any other in-memory changes must be
   performed now.
6. The success of the operation is now consistent on this master in both
   on-disk state as well as in-memory state.
7. Send RPCs to the tservers (e.g. **DeleteTable()** will now send delete
   tablet RPCs to each tablet in the table). The work done by an RPC must
   take place, either by retrying the RPC until it succeeds, or through some
   other mechanism, such as sending a new RPC in response to a heartbeat.

Generally speaking, this contract is upheld universally. However, a detailed
audit (see appendix B) of the master has revealed a few exceptions:

1. During **CreateTable()**, intermediate table and tablet state is made
   visible prior to replication.
2. During **DeleteTable()**, RPCs are sent prior to the committing of
   mutations.
3. When background scanning for newly created tablets, the logic that
   "replaces" timed out tablets makes the replacements visible prior to
   replication.

To prevent clients from seeing intermediate state and other potential
issues, these operations must be made to adhere to the above contract.

## Appendix A: Fencing audit and discussion

To understand which master operations need to be fenced, we ask the following
key questions:

1. What decisions can a master make unilaterally (i.e. without first
   replicating so as to establish consensus)?
2. When making such decisions, does the master at least consult past replicated
   state first? If it does, would "stale" state yield incorrect decisions?
3. If it doesn't (or can't) consult replicated state, are the external actions
   performed as a result of the decision safe?

We identified the set of potentially problematic external actions as those taken
by the master during tablet reports.

We ruled out ChangeConfig; it is safe due to the use of CAS on the last change
config opid (protects against two leader masters both trying to add a server),
and because if the master somehow added a redundant server, in the worst case
the new replica will be deleted the next time it heartbeats.

That left DeleteReplica, which is called under the following circumstances:

1. When the master can't find any record of the replica's tablet or its table,
   it is deleted.
2. When the persistent state says that the replica (or its table) has been
   deleted, it is deleted.
3. When the persistent state says that the replica is no longer part of the Raft
   config, it is deleted.
4. When the persistent state includes replicas that aren't in the latest Raft
   config, they are deleted.

Like ChangeConfig, cases 3 and 4 are protected with a CAS. Cases 1 and 2 are
not, but 2 falls into category #2 from earlier: if persistent state is consulted
and the decision is made to delete a replica, that decision is correct and
cannot become incorrect (i.e. under no circumstance would a tablet become
"undeleted").

That leaves case 1 as the only instance that needs additional fencing. We could
implement leader leases as described earlier or "current term" checking to
protect against it.

Or, we could 1) continue our current policy of retaining persistent state of
deleted tables/tablets forever, and 2) change the master not to delete
tablets for which it has no records. If we always have the persistent state
for deleted tables, all instances of case 1 become case 2 unless there's
some drastic problem (e.g. tservers are heartbeating to the wrong master),
in which case not deleting the tablets is probably the right thing to do.

## Appendix B: Master operation audit and analysis

The following are detailed audits of how each master operation works today.
The description of each operation is followed by a list of potential
improvements, all of which were already incorporated into the above
plan. These audits may be useful to understanding the plan.

### Creating a new table

#### CreateTable() RPC

1. create table in state UNKNOWN, begin mutation to state PREPARING
2. create tablets in state UNKNOWN, begin mutation to state PREPARING
3. update in-memory maps (table by id/name, tablet by id)
   - new table and tablets are now visible with UNKNOWN state and no
     useful metadata (table schema, name, tablet partitions, etc.)
4. replicate new tablets
5. change table from state PREPARING to RUNNING
6. replicate new table
7. commit mutation for table
   - new table and tables are visible, table with full metadata in RUNNING
     state but tablets still in UNKNOWN state without metadata
8. commit mutations for tablets
   - new table and tablets are visible with full metadata, table in
     RUNNING state, tablets in PREPARING state (without consensus state)
9. wake up bg_tasks thread

Potential improvements:

1. The two replications can be safely combined into one replication
2. Consumers of table and tablet in-memory state must be prepared for
   UNKNOWN intermediate state without metadata, as well as lack of consensus
   state. This can be addressed by:
   1. Adding a new global in-memory unordered_set to "lock" a table's name
      while creating it. When the create is finished (regardless of
      success), the name is unlocked. We could even reuse the tablet
      LockManager for this, as it has no real tablet dependencies
   2. Moving step #3 to after step #7

#### Background task scanning

1. for each tablet t in state PREPARING:
   - change t from state PREPARING to CREATING
2. for each tablet t_old in state CREATING:
   - if t_old timed out:
     1. create tablet t_new in state UNKNOWN, begin mutation to state PREPARING
     2. add t_new to table's tablet_map
        - t_new is now visible when operating on all of table's tablets, but
          is in UNKNOWN state and has no useful metadata
     3. update in-memory tablet_by_id map with t_new
        - t_new is now visible to by tablet_id lookups, still UNKNOWN and
          no useful metadata
     4. change t_old from state CREATING to REPLACED
     5. change t_new from state PREPARING to CREATING
3. for each tablet t in state CREATING:
   - reset t committed_consensus_state:
     1. reset term to min term
     2. reset consensus type to local or distributed
     3. reset index to invalid op index
     4. select peers
4. replicate new and existing tablets
5. if error in replica selection or replication:
   1. update each table's in-memory tablet_map to remove t_new tablets (step 2)
      - t_new tablets still visible to by tablet_id lookups
   2. update tablet_by_id map to remove t_new tablets (step 2)
      - t_new tablets no longer visible
6. send DeleteTablet() RPCs for all t_old tablets (step 2)
7. send CreateTablet() RPCs for all created tablets
8. commit mutations for new and existing tablets
   - replacement tablets from step #2 now have full metadata, all tablets now
     have visible consensus state

Potential improvements:

1. All replication is already atomic; nothing to change here
2. Steps 6 and 7 should probably be reordered after Step 8
3. t_new can expose intermediate state to consumers, much like in
   CreateTable(). Is this safe?
   1. Remove step #5
   2. After step #8 (but before RPCs), insert steps #2b and #2c

### Deleting a table

#### DeleteTable() RPC

1. change table from state RUNNING (or ALTERING) to REMOVED
2. replicate table
3. commit mutation for table
   - new state (REMOVED) is now visible
4. for each tablet t:
   1. Send DeleteTablet() RPC for t
   2. change t from state RUNNING to DELETED
   3. replicate t
   4. commit mutation for t
      - new state (DELETED) is now visible

Potential improvements:

1. Table and tablet replications can be safely combined, provided we invert
   the commit order and make sure rest of master is OK with that
2. DeleteTablet() RPC should come after tablet mutation is committed

### Altering a table

#### AlterTable() RPC

1. if requested change to table name:
   - change name
2. if requested change to table schema:
   1. reset fully_applied_schema with current schema
   2. reset current schema
3. increment schema version
4. increment next column id
5. change table from state RUNNING to ALTERING
6. replicate table
7. commit mutation to table
   - new state (ALTERING), schema, etc. are now visible
8. Send AlterTablet() RPCs for each tablet

No potential improvements

### Heartbeating

#### Heartbeat for tablet t

1. if t fails by_id lookup:
   - send DeleteTablet() RPC for t
2. if t's table does not exist:
   - send DeleteTablet() RPC for t
3. if t is REMOVED or REPLACED, or t's table is DELETED:
   - send DeleteTablet() RPC for t
4. if t is no longer in the list of peers:
   - send DeleteTablet() RPC for t
5. if t CREATING and has leader:
   - change t from CREATING to RUNNING
6. change consensus state
   1. reset committed_consensus_state (if it exists in report)
   2. update tablet replica locations
      - new replica locations now immediately visible
   3. if t is no longer in the list of peers:
      - send DeleteTablet() RPC for t
   4. if tablet is under-replicated
      - send ConfigChange() RPC for t
8. update t
9. commit mutation for t
   - new state and committed consensus state are now visible
10. if t reported schema version isn't latest:
   - send AlterTablet() RPC for t
11. else:
   1. update tablet schema version
      - new schema version is immediately visible
   2. if table is in state ALTERING and all tablets have latest schema version:
      1. clear fully_applied_schema
      2. change table from state ALTERING to RUNNING
      3. replicate table
      4. commit mutation for table
         - new state (RUNNING) and empty fully_applied_schema now visible

No potential improvements. One replication per tablet (as written) is OK
because:

1. If replication fails but master still alive: tserver will retry same
   kind of heartbeat, giving master a chance to replicate again
2. If replication fails and master dies: with proposed "send full heartbeat
   on new leader master" change, failed replications will be retried by new
   leader master
