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

# Kudu Tablet Copy design

Master & Tablet Copy integration with configuration change

## Summary

This document contains information on implementing Tablet Copy in the
context of Kudu's Raft implementation. Details on Raft config change in Kudu
can be found in the [Raft config change design doc](raft-config-change.md).

## Goals

1. Integrate Tablet Copy to allow for copying over "snapshotted" data and
   logs to a tablet replica when logs from the "beginning of time" are no
   longer available for replay.
2. The Master needs to tolerate and facilitate dynamic consensus config change.

## New RPC APIs

### New Master RPC APIs

The Master will expose the following operations to admin users:

1. `AddReplica(tablet_id, TS to_add, Role role)`
2. `RemoveReplica(tablet_id, TS to_remove)`

### New tablet server Tablet Copy RPC

Tablet Copy allows a tablet snapshot to be moved to a new server. A
`StartTabletCopy()` RPC call will be available on each tablet server. When a
leader determines that a follower needs log entries prior to what is available
on the leader side, or when it detects that a follower does not host a given
tablet, it sends the follower an RPC to instruct the follower to initiate
Tablet Copy. Optionally, this callback is made idempotent by passing the
latest OpId in the follower's log as an argument.

### Management of Tablet Copy jobs

Since copying a tablet may involve transferring many GB of data, we
likely need to support operational visibility into ongoing Tablet Copy
jobs, run them on their own thread pool, support cancellation, etc. TBD to
enumerate all of this in detail.

## Design & implementation of Tablet Copy

### Tablet auto-vivification

A leader could cause a tablet to auto-create / auto-vivify itself if it doesn't
already exist by sending a StartTabletCopy RPC to the tablet server. Not
requiring the Master to explicitly invoke a CreateTablet() RPC before adding a
replica to a consensus config makes adding a new peer much simpler to implement
on the Master side.

In addition, allowing the leader to promote a "pre-follower" (`PRE_VOTER`) to
follower (`VOTER`) once its log is sufficiently caught up with the leader's
provides availability benefits, similar to the approach specified in the Raft
dissertation (see Section 4.2.1)

With such a design, to add a new replica to a tablet's consensus config, a
Master server only needs to send an RPC to the leader telling it to add the
given node:

1. Master -> Leader: `AddServer(peer=Follower_new, role=PRE_VOTER)`

The leader will then take care of detecting whether the tablet is out of date
or does not exist, in which cases it must be copied, or whether
it can be caught-up normally.

### Note on deleted tablets

If a replica is subsequently removed from a tablet server, it must be
tombstoned and retain its (now static) persistent Raft state forever. Not doing
this can cause serious split-brain problems due to amnesia, which we define as
losing data that was guaranteed to be persisted according to the consensus
protocol. That situation is described in detail in this [raft-dev mailing list
thread](https://groups.google.com/d/msg/raft-dev/CL1qWP7a_1w/OfHqmbcbIlAJ).

To safely support deletion and Tablet Copy, a tablet will have 4 states
its data can be in: `DOES_NOT_EXIST` (implicit; just the non-existence of state),
`DELETED`, `COPYING`, and `READY`. `DOES_NOT_EXIST` just means a tablet with that name
has never been hosted on the server, `DELETED` means it's tombstoned, `COPYING`
means it's in the process of Tablet Copying, and `READY` means it's in a
normal, consistent state. More details about Tablet Deletion is in a later
section.

### Auto-vivifying Tablet Copy protocol

The Tablet Copy protocol between the leader and follower is as follows.
The leader always starts attempting to heartbeat to the follower:

Leader -> Follower: `AppendEntries(from, to, term, prev_idx, ops[])`

The follower receives the request and responds normally to the heartbeat
request if it is running, and with special responses if it `DOES_NOT_EXIST` or is
`DELETED`. Roughly:

```
AppendEntries(from, to, term, prev_idx, ops[]):
  if (to != self.uuid): return ERR_INVALID_NAME
  if (term < self.current_term): return ERR_INVALID_TERM
  if (self.state == DELETED): return DELETED
  # Otherwise: Normal consensus update / AppendEntries logic
```

If the leader gets back a `DOES_NOT_EXIST` or `DELETED` tablet status, it will
repeatedly attempt to "auto-vivify" the tablet on the follower by sending a
StartTabletCopy RPC to the follower.

On the follower, the `StartTabletCopy` RPC is idempotent w.r.t. repeated RPC
requests from the leader and has logic to create a tablet if it doesn't yet
exist. Roughly:

```
StartTabletCopy(from, to, tablet, current_state, last_opid_in_log = NULL):
  if (to != self.uuid): ERR_INVALID_NAME
  if (this.tablet.state == COPYING): ERR_ALREADY_INPROGRESS
  if (this.tablet.state != current_state): ERR_ILLEGAL_STATE
  if (this.tablet.state == RUNNING):
    DeleteTablet() # Quarantine the tablet data.
  if (this.tablet.state == DELETED || this.tablet.state == DOES_NOT_EXIST):
    CreateTablet(COPYING) # Create tablet in "COPYING" mode.
  if (caller.term < self.term): ERR_BAD_TERM
  if (last_opid_in_log != NULL && != this.log.last_op.id): ERR_ILLEGAL_STATE
  RunTabletCopy() # Download the tablet data.
```

The detailed process, on the follower side, of downloading and replacing the
data is detailed below under "Follower Tablet Copy".

### Tablet directory structure

This section describes a tablet server's directory structure, which looks like:

```
instance
tablet-meta/*<tablet-id>*
consensus-meta/*<tablet-id>*
wals/*<tablet-id>*/
data/
```

The primary indicator of a tablet's existence is the presence of a superblock
file for the tablet in the tablet-meta directory. If that file does not exist,
we have no pointers to tablet data blocks, so we consider the tablet in
`DOES_NOT_EXIST` state. In addition to the superblock, the minimum files needed
to start a tablet are the consensus metadata file (under consensus-meta), the
write-ahead logs (under wals), and the data blocks (under data). Of course,
automatically-generated tablet server-level files like the instance file must
also be in place.

### Tablet deletion

In order to delete a tablet we must permanently retain the Raft metadata to
avoid consensus amnesia bugs. We also temporarily back up (quarantine) the data
for later debugging purposes. Initially, we will provide some tool to manually
remove the quarantined files and their associated data blocks when they are no
longer needed.

**Requirements**

* Save consensus metadata such as current term, vote history, and last log
  entry. This is required by Raft to avoid electing stale nodes as leaders and
  also to force stale leaders to step down.
* Tombstone the tablet (possibly using a flag in the superblock), so we know to
  look for the saved consensus metadata per above.
* Quarantine the old data.

**Implementation**

We can safely implement tablet deletion using the following steps:

1. Shutdown tablet, if running.
2. Create the quarantine directory (QDIR).
3. Copy current SuperBlock to QDIR.
4. Mark SuperBlock as `DELETED` (thus we always roll the delete forward after
   this step); Store the last OpId in the log into a field in the SuperBlock PB
   (we need to know the last OpId in our log if we want to be able to vote in
   elections after being deleted and no longer having our WAL files); Store the
   path to the QDIR in the SuperBlock as well; and finally save and fsync the
   SuperBlock into the tablet metadata directory. Now the tablet is considered
   deleted (at startup, we will ensure this process completed - see "Tablet
   startup" below).
5. Copy the consensus metadata file into QDIR.
6. Move the WAL dir to QDIR. This should be atomic, since at this time we do
   not stripe the WAL. If we were to stripe or multiplex the WAL in the future,
   we could add some kind of tablet-level sequence number, like a generation
   number, that gets incremented at the SuperBlock level when we initiate
   Tablet Copy. That should keep us pointed at the relevant entries.

### Follower Tablet Copy

Tablet Copy copies the data from the remote; merges the new
and old consensus metadata files (if a local one already existed; otherwise the
remote metadata is adopted); and writes a replacement SuperBlock.

1. Rewrite current SuperBlock in `COPYING` state and fsync. If SuperBlock already
   exists, clear the QDIR path field, but not the last_opid field (in case we
   crash at this stage and must delete ourselves at startup, then we can (must)
   retain our knowledge of last_opid in order to be able to vote; see "Tablet
   startup" below).
2. Download & merge consensus metadata file (see "Consensus metadata merge"
   below).
3. Download remote WALs.
4. Download remote blocks.
5. Write replacement SuperBlock in `READY` state and fsync it.
6. Start up the new tablet replica.

### Consensus metadata merge

When downloading the remote consensus metadata file, in order to avoid
consensus amnesia, we must ensure that our term remains monotonic and that our
memory of our votes is not lost. We can't just adopt the remote consensus
metadata, so we must merge it with our own. These rules ensure the consensus
metadata remains valid while merging the remote metadata:

1. Always adopt the highest term when merging consensus metadata.
2. if `remoteTerm <= localTerm`: Retain vote history for `localTerm`, if any.
3. Always adopt the remote's raft cluster membership configuration.

### Tablet startup

There is a simple state machine for maintaining a consistent state when a tablet starts up:

1. if tablet.state is `DELETED` && WAL dir exists:
   Redo steps #5 and #6 in the "Tablet deletion" section above (roll forward).
2. if tablet.state is `COPYING`: Delete self (go back to `DELETED` state).
3. if tablet.state is `READY`: Normal startup.
4. if tablet startup fails:
   Stay offline. Master or admin is responsible for responding.

### Master implementation for adding a new peer

Since the master has only a single idempotent RPC that it has to invoke to add
a new peer to the consensus config, the master does not need to store metadata
about the operations it is conducting or perform retries. The policy (for
example the under-replication policy) will make those decisions and trigger
idempotent actions. The master is constantly evaluating its policies and making
adjustments.

When a master determines that it needs to change a tablet's configuration, it
does the following:

Master sends an `AddServer()` RPC to the leader of the tablet to add a new
replica as a `PRE_VOTER` (the initial implementation may simply add the peer as
a `VOTER`).

1. This `AddServer()` RPC will specify the prior committed raft config for the
   tablet to ensure that the request is idempotent.
2. The master will periodically send such RPCs until it sees that it has
   been successful, for example in fixing an under-replication problem.

**Idempotent config change operations**

To allow the master to make decisions while having potentially stale consensus
config information, we need to add a CompareAndSet-style consistency parameter
to "config change" operations (`AddServer` / `RemoveServer` / `ChangeRole`). We
will add committed_config_id as an optional parameter that identifies the
latest committed consensus config. If the opid doesn't match the currently
committed config on the peer (checked under the RaftConsensus ReplicaState
lock), the RPC returns an error indicating that the caller is out of date. If
the OpId matches, but there is already a pending config change, the request is
also rejected (that behavior is already implemented).

## Other Failure Recovery

### Failure of a disk with consensus metadata is a catastrophic failure

If we lose a disk with consensus metadata or WALs, and would need to copy a
new replica to recover, it may be impossible to do so safely due to unavoidable
consensus amnesia. In such a case, the tablet server must adopt a new UUID and
fully clear all of its data and state:

1. Detect bad disk
2. Shut down / crash
3. Administrator will replace the bad disk(s)
4. Wipe all data on the machine
5. Reassign a new UUID for the tablet server

This precaution ensures that an outdated tablet server cannot ever convince a
server with consensus amnesia to auto-vivify a new tablet, elect the outdated
server leader, and create a parallel-universe consensus group. Of course,
reassigning a server's UUID is only effective at isolating amnesiac servers
from requests intended for their former selves if all RPC calls are tagged with
the UUID of the intended recipient and if that field is always validated. This
ensures that a server refuses to participate with a peer that is calling it by
the incorrect (old) name.

It may be possible to optimize this and avoid having to nuke the whole box when
only one disk fails. Maybe we could rely on RAID for the consensus metadata and
WALs, or we could try to make tablets sticky to specific disks, or do something
else clever. However those options aren't great and this issue is pretty
tricky.

## Future work & optimizations

### Should a server be allowed to vote if it `DOES_NOT_EXIST` or is `DELETED`?

Ideally, yes. There are scenarios that could be constructed where a tablet
config could not make progress (be elected) without this functionality. This is
possible to do as long as we retain consensus metadata indefinitely, which is
required for correctness anyway. However this is not a top priority.

One scenario where a `DELETED` tablet may need to vote to make forward progress
is if a `VOTER` replica falls behind and so starts to Tablet Copy, crashes
in the middle of Tablet Copy, and deletes itself at startup. Once we
implement `PRE_VOTER`, and always catch up as a `PRE_VOTER` before becoming a
`VOTER`, the opportunity for potential problems with `VOTER`s is reduced a lot,
especially around the initial step of adding a server to the cluster, but still
does not address the above scenario.

### Support `AddServer` abort?

Ideally, we would support aborting an in-progress config change request if the
new peer is not responding. This could have some availability benefits. However
this is not a top priority.

There are hacks that can be done as a workaround to roll back a failed config
change, like truncating the log, forcing a leader to be elected to a new term,
etc. that could allow us to set quorum members manually. But it would be better
to have some kind of auto-rollback after a timeout.

### Higher level Master APIs

1. `MoveReplica(tablet_id, TS from, TS to)`
   * Consists of add, then remove for a single tablet and pair of tablet servers.
2. `ChangeVotingReplicaCount(tablet_id, new_count)`
   * Perform series of automatic steps to satisfy the desired voter replication factor.
3. `RetireTabletServer(TS ts)`
   * Move all replicas off of this box.
