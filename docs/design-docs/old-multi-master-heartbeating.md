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

# TabletServer support for multiple masters

## Context

This document was written on November 10th, 2014. The design was never
implemented, but may serve as a useful starting point for future
multi-master design discussions.

## Existing TS to single master flow

Currently a tablet server is provided with an address of a single
master. The tablet server then periodically (default period: 1 second)
sends heartbeats to the master. The heartbeats contain the tablet
server’s node instance, and (optionally) the tablet server’s
registration (hosts and ports) and/or a the server’s tablet report.
If a heartbeat can’t be delivered to the master at a scheduled time,
the tablet server waits, and tries again during the next schedule
time.

### Heartbeats and in-memory state

When the single master receives a heartbeat, it:

1. Checks if the HeartBeat contains TS registration. If there’s
   registration, update a mapping of tablet server UUID to TS
   registration. (This is in-memory only)
2. If HeartBeat contains a tablet report, then the Master updates the
   SysTables/SysTablets entries as needed based on the tablet report (the
   hard state is updated).
3. If a heart-beat came from a TS that has no entry in the in-memory
   state (the UUID to TS registration mapping is empty for that TS), then
   the master requests the TS to (re-)send its registration and a full
   tablet report. **This is needed in order to re-build the in-memory
   state on a master server that has freshly come up.**

In-all, the in-memory state (encapsulated in ts_manager.cc/.h) is not
in critical path of any lookup operation. **However, it plays a role
in the table creation operations**: getting the current number of
tablet servers that are up (to ensure that tablets can be created),
assigning replicas to tablets, and modifying tablets on the tablet
server (pending tasks for create table, delete table, alter table,
etc…).

#### Issue: CreateTable() unavailability on a “fresh” master

As a result, when a single master is restarted, but before it has
received the heartbeats from N servers (where N is the replication
factor), CreateTable() call will fail (as we don’t know whether or not
we have enough TS replicas up to execute the request). Additionally,
until we’ve received heartbeats from enough tablet servers to be able
to assign all tablets, background tablet alteration tasks may remain
in PENDING state for a longer period of time (this, however, is per
the contract for those operations).

## Design alternatives

### Don’t require a minimum server count for CreateTable()
* Don’t refuse CreateTable() requests when the minimum number of
  servers is not available. As is, CreateTable() can return without
  having actually created the table: in this case, the tasks would
  remain as pending until the requisite number of tablet servers have
  come up.

### Send heartbeats to all leaders
* After failover, immediate ability to do things that require
  knowledge of TS liveness:
  * Load balancing
  * Direct clients to up-to-date tablet quorum leader
* Detect connectivity issues between TS / Master Followers before failover
* Disadvantage: Tablet Server has to keep separate thread and state
  for each master server for heart-beating. This is not that bad, we can
  have multiple Heartbeater instances (1 per Master per TS)

### Send heartbeats only to the master, use consensus to replicate.
* Only send to one server
  * If timeout / server down, try next in round-robin fashion
  * Follower Masters redirect to leader
  * Retrying heartbeats is straightforward, there is no case where we
    “give up”.
* Less traffic between TS and Master
* Disadvantage: Logic required to “follow the leader”
* Disadvantage: Either exists a period after failover for which the
  follow does not know which servers host which tablets, thus leading
  to less efficient routing, or need to replicate every heartbeat via
  consensus (this would be slow, especially if logging). Or we have to
  only replicate “snapshots” of the soft state

## Handling multiple masters (“heartbeat to Master quorum” design)

In order to handle multiple masters, tablet servers must support
initialization with multiple master addresses, the ability to
determine which master is the leader (much like this is currently done
on the C++ client), and to be able to send heartbeats to non-leader
masters (to address the “CreateTable() unavailability on fresh master”
issue above). Tablet servers will need to maintain per-master state,
indicating whether that master is currently a leader, and whether they
need to (re-)send their registration (or in the case of the leader, a
full tablet report) to that master.

Master server must be changed to allow non-leader masters to support
handling heartbeats from tablet servers, applying the heartbeat
information only to the in-memory state (i.e., disregarding full
tablet reports) only and not to the CatalogManager (only process
routine heartbeats and TS registration, updating the mapping of live
tablet servers). Authoritative information (the (table, key) ->
tablet, and tablet -> TS mapping) that is in the hot-path of all
clients requests is served from hard state, which is replicated via
the master’s consensus quorum. Client queries (and full tablet reports
from tablet servers) will still be handled only by the leader master.

The heartbeat response from a master server will also indicate whether
or not that master server is the leader. The TS, upon receiving a
response saying a master server is not the leader from the previous
leader, will determine the new leader (just as it does during the
initialization routine) and send full tablet reports (if required) to
that leader.

## Summary of supported remote functionality, by role

| RPC | Leader Master | Follower Master |
| --- | ------------- | --------------- |
| Ping(),<br/>TSHeartbeat() | Yes | Yes |
| ListTables(),<br/>GetTableLocations(),<br/>GetTabletLocations(),<br/>GetTableSchema(),<br/><br/>CreateTable(),<br/>DeleteTable(),<br/>AlterTable() | Yes | **No**<br/><br/>The _List* and Get* calls need cache invalidation upon Update() to work properly on a follower, unless we disable the RWC cache._ |
| ListTabletServers(),<br/>ListMasters(),<br/>GetMasterRegistration() | Yes | Yes |

# Deliverables

1. Extract any code that could be shared between the client, master,
   and tablet server. Namely, this would be the code that handles server
   registration, finding the quorum leader. Presently we have separate
   data structures for the tablet server and master registration, these
   could be unified as “server registration”, allowing code re-use in
   places where it makes sense (provided the newly added abstractions do
   not, by themselves, increase complexity). **Status**: DONE
2. Support initializing a tablet server with multiple masters,
   heartbeating to the leader master, and handling leader master
   failures. NOTE: this doesn’t address the CreateTable() issue
   above. **Status**: DONE.
  1. Test 1: start a cluster, initialize a client, create a table,
     change the leader master, and verify that we can perform a scan on
     the tablet immediately after the new leader master is started.
  2. Test 2: start a cluster, initialize a client, create table A,
     change the leader master, sleep for a period sufficient for the
     tablet servers to send their heartbeats to the master, and verify
     that we can delete table A and create table B.
3. Address the CreateTable() issue above: support sending heartbeats
   to all of the masters (not just the leader).
  1. Test: like “Test 2” above, but without waiting for the heartbeats
     to go through.
