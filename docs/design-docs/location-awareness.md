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

Kudu Location Awareness
=======================

Requirements
============

Motivation
----------

Kudu is designed to tolerate independent failures of single nodes.
Tablets are normally replicated at least 3 times via Raft consensus,
remain available (possibly with a short interruption) when any single
replica becomes unavailable, and automatically re-replicate data if a
replica is unavailable for too long. However, there can be correlated
failures of multiple nodes when e.g. an entire rack loses power or an
AWS availability zone has its fiber severed. Kudu has matured to the
point where users require features that allow them to prepare for
correlated node failures.

Use Cases
---------

Below are a few use cases involving Kudu location awareness policies.
The motivation is to clarify on the big picture and build a high-level
proposal, so we could be on the same page in terms of our expectations.
Hopefully, having these will help to iterate on the set of requirements
for the MVP that supports location awareness policies.

### 1 Required: Rack-aware placement of replicas to survive per-rack hardware failures

Having servers in multiple racks, place tablet's data across racks so
that if a network switch/power supply on one rack fails, the tablet's
data is still available for reading and writing.

### 2 Required: Keeping up-to-date inter-datacenter backup replicas

That’s to not lose data in case of a failure of all hardware in a single
datacenter/zone. Running servers in multiple datacenters/zones, place
data in different datacenters/zones so that if all servers in one
datacenter/zone fail, the tablet's data is still available for reading
and writing.  For this use case, it's assumed that the latencies between
datacenters are negligible.

### 3 Strict locality constraints

Having servers in multiple datacenters/zones, place table's data across
servers only in a particular datacenter/zone (something like GDPR-alike
constraint), not allowing replicas to be placed in any other
zone/datacenter even in case of re-balancing.                          
                                             

### 4 Strict inter-datacenter locality constraints and intra-datacenter rack-awareness

In case of deployment with multi-datacenter or multi-zone setup, it’s
necessary to have affinity for some tables (e.g., stick them to
particular DC or AZ).  But within that DC, all tablets of those tables
should remain available if a single rack fails.  Basically, that’s
superposition of cases 1 and 3.

Requirements
------------

-   Allow administrators to label nodes with locations.

-   Support location awareness for clients as well, if labeled.

-   Support location-aware replica placement policies that allow
    administrators to control where replicas of a table are placed based
    on a policy set for the cluster as whole. The supported policies
    must be at least expressive enough to handle the following use case
    (but may be more expressive):

-   Ensure that no location contains a majority of the replicas if there
    are more than 2 locations.

-   Support tablets with replicas split between locations that have
    reliable, low-latency connectivity between them, e.g. between AWS
    availability zones in the same region.

-   Diagnostics and warnings should be able to indicate when the
    connection between locations is not reliable or has high latency.

-   Support location-aware clients so at minimum a location-aware client
    performing a CLOSEST\_REPLICA scan will read from the same location if
    possible.
-   Enhance the rebalancing tool to preserve placement policies if they
    hold and to move replicas to enforce placement policies if they are
    violated.
-   Provide other appropriate guides and tools to administer and support
    the features needed to fulfill the above requirements.

-   Documentation on setting up location awareness, covering at least
    how to achieve availability in the face of a single location failure
-   CLI tools to observe location labels and policies

Non-requirements
----------------

-   Support tablets with replicas distributed globally, or to locations
    that do not have high-quality, low-latency connections.
-   Support a distinguished “location-aware durability” case where a
    tablet is durable but not available if a single location is
    permanently lost. This can be achieved by splitting replicas evenly
    across two locations with an even replication factor, for example.
-   Support location-awareness for tablet leadership. For example,
    require that 2 out of 3 replicas be placed in location “us-east-1”
    and the tablet leader be one of the two replicas in that location.
-   Support performance- or regulation-motivated placement policies at
    higher granularity.

-   E.g. “place all replicas of tablets for range partition ‘EUROPE-PII’
    in location ‘eu-1’”

-   Optimize the total data transfer between replicas in order to
    minimize the potential costs of splitting tablets across locations,
    e.g. AWS charging for transfer between availability zones.

-   For example, optimizing tablet copy to copy from replicas in the
    same location as the destination replica as opposed to copying from
    the leader.

Kudu Location Awareness - Design
================================

Proposal: “Hew to HDFS”
-----------------------

This proposal fulfills the requirements with a design that resembles
HDFS’s rack awareness features as much as possible. As Kudu tablets
requires a majority of replicas to be available while HDFS blocks
require only one, there are significant differences.

### Node labeling

Kudu will adopt HDFS’s method of node labeling. The master will gain a
new flag \`--location\_mapping\_cmd\` whose value will be an executable
script. The script should accept one or more hostnames or IP addresses and
return, for each hostname or IP addresses provided, a location. A
location is a /-separated string that begins with ‘/’, like a Unix path,
and whose characters in the /-separated components are limited to those
from the set [a-zA-Z0-9\_-.].

The  /-separation signals that hierarchical semantics may be introduced
later.

Whenever a tablet server registers or re-registers with the master, the
master will assign it a location by calling the script with the hostname
or IP of the server. The assigned location will be cached and the cached
location will be used until the tablet server
re-registers, in which case the master
will re-resolve the tablet server’s location using the script. The
leader master will use location information to place new replicas, both
when creating a table and when re-replicating.

### Placement policies

Recall the one tablet replica placement policy required of any design:

-   If there are more than 2 locations, ensure that no location contains
    a majority of the replicas.

This design proposes a placement policy for tablet creation and
re-replication that makes a best-effort to comply with this requirement
but will violate it in order to maintain full re-replication. This is
the same enforcement as HDFS.

Additionally, this design has one more policy that it makes a best
effort to satisfy:

-   Ensure that, when there are two locations, no more than floor(r / 2)
    + 1 replica of a replication factor r tablet are placed in a single
    location.

These two policies will apply to every table in the cluster and will not
be configurable. Future implementations may introduce configurable
placement policies for the cluster or sub-elements of the cluster like
tables, or may introduce different strictness semantics or guarantees
around enforcement.

#### Definitions

-   A location is available if at least one tablet server in the
    location is available, according to the master.
-   If the number of tablet servers in a location is N and the total
    number of replicas in the location is R, the load or total load of
    the location is defined as R / N. For a table T with R replicas in
    the location, the table load or load for table T is defined as R /
    N.
-   The skew between two locations is defined as the absolute value of
    the difference between their loads. Table skew between locations is
    defined analogously.
-   The skew of a set of locations is defined as the maximum skew
    between any pair of locations. The table skew of a set of locations
    is defined analogously.
-   A set of locations is balanced if no replica can be moved between
    locations without either violating the placement policy or
    increasing the skew. Table-wise balance is defined analogously.

Note that the definitions of skew and balance generalize the definitions
used in the original rebalancer design, if we identify a location
consisting of a single tablet server with its tablet server.

#### Tablet Creation

1.  On tablet creation, the master will choose locations for the
    replicas by repeatedly using the power of two choices, starting from
    a pool of all available locations. The load factor used to make the
    binary choice in the algorithm will be the load of the location.
    When a location is picked it is removed from the pool. If there are
    still replicas remaining after all locations have been used, all
    available locations will be put back into the pool except those
    which cannot possibly accept a replica because every tablet server
    in the location already hosts a replica.
2.  Within a location, replicas will be placed using the power of two
    choices algorithm among the tablet servers.

If it is possible to conform to the placement policies given the number
of tablet servers in each location, then this policy clearly produces an
arrangement that complies with the policy.

The new placement algorithm generalizes the original by making it have
two stages: choosing locations, then choosing replicas within location.
The original algorithm can be recovered by considering all tablet
servers to be in a single location. This algorithm also generalizes in
an obvious way to more levels of location hierarchy.

#### Re-replication

Re-replication will be a special case of the above algorithm, where we
are picking a tablet server for a single replica, given that we have
already picked the locations for the other replicas.

### Location-aware clients

When the client connects to the cluster, it will include the hostname or
IP of the machine it is running on. The master will assign it a location
and return that to the client. It will also return location information
to the client about tablet servers. The client will use this information
to select a server to read from in CLOSEST\_REPLICA mode. It will prefer a
local server, as it does now, and then after that prefer servers in the
same location (with ties by random choice). This procedure generalizes
to a hierarchical scheme: the client would prefer servers having the
longest suffix of common locations in the /-separated string.

### Rebalancing

This design assumes that preserving and enforcing the placement policy
is higher priority than balancing. Therefore, the rebalancer will
attempt to conform the cluster with the placement policy and, only
afterwards, attempt to balance the cluster while maintaining the
placement policy.

#### Rebalancing: background

Recall that previously a cluster was considered balanced if the maximum
skew between any pair of tablet servers was 0 or 1. Table balance was
defined analogously. Currently, the rebalancer uses a two-stage greedy
algorithm. It is guaranteed to balance every table and the cluster as
long as replica moves succeed. The thrust of the algorithm is as
follows:

-   For each table:

-   Order the tablet servers by load for the table.
-   While the table is not balanced:

-   Move a replica from the most loaded server to the least

-   All tables are now balanced. Balance the cluster:

-   While the cluster is not balanced

-   Move a replica from the most loaded server to the least

One key reason this algorithm works is that it is always possible to
move a replica from the most loaded server to the least loaded. However,
this is not true when balancing replicas between locations. Consider a
cluster with 3 locations {A, B, C}. A has 100 tablet servers in it while
B and C have 2 each. If tablet creation results in replica counts {A -\>
2, B -\> 2, C -\> 1}, B has load 2 while A has load 1/50, but every
replica move from B to A violates the placement policy. So, it is not
always possible to move a replica from the most loaded location to the
least loaded.

Yet, it is possible to characterize the situations when a move is
impossible. Suppose now we have two locations A and B. A consists of S
tablet servers hosting M replicas of a tablet X among them. B consists
of T tablet servers hosting N replica of a tablet X among them. Suppose
the replication factor of the tablet is R. There are two reasons why a
move of a replica of tablet X from A to B could be illegal:

1.  A tablet server may never host more than one replica of the same
    tablet. If this principle forbids a move, then N = T. Because of the
    placement policy, this means that T \<= floor(R / 2), so 2T \<= R.
    This can only happen if the replication factor is big compared to
    the number of servers in the smallest location.
2.  Moving a replica from A to B would violate the placement policy.
    This can plausibly happen for a single tablet. However, for this to
    be true for every replica in the table, every tablet would need to
    have floor(R / 2) replicas in location B, so the load of B for the
    table is N / T =  floor(R)/2, the maximum value of load if the
    placement policy holds. But then we wouldn’t be moving from A to
    this location if the goal is to reduce the skew. A similar argument
    applies to moves looking at the total load of the location as well.

\#1 is not expected to be a problem in the common case. Almost all
tablets are 3x replicated, and 3x replication precludes multiple
replicas in the same location anyway. Allowing for 5x replication, a
location would need to have 2 or less servers to be vulnerable.
Therefore, in the common case, it should be possible to move a replica
from the most loaded location to the least loaded, and a similar greedy
algorithm will produce good results.

#### Rebalancing stage 1: enforce placement policy

The rebalancer will first attempt to reestablish the placement policy if
it has been violated. It will find a set of replicas whose movement to
new locations will bring the tablet in compliance with the placement
policy. It will mark all of these tablets with the REPLACE attribute,
and then wait for the master to place them in new locations. If there is
no such set of replica then this step is skipped. This is nice and
simple as it concentrates responsibility for the placement policy in the
master’s replica selection algorithm. It’s suboptimal since the current
master replica selection algorithm is not tuned to keep the cluster
well-balanced in many situations. This is planned to be improved by
later rebalancer work.

#### Rebalancing stage 2: rebalance

The rebalancer will balance hierarchically: first balancing locations,
then balancing within locations.

1.  For each table:

1.  Order the locations by load for that table
2.  While there are still legal moves

1.  Move from the most loaded locations to the least loaded

1.  Within the location, move from the most loaded tablet server to the
    least loaded, to reduce the within-location phase’s work

3.  Once legal moves are exhausted, remove the most loaded location from
    consideration, so the second-most-loaded becomes the most loaded.
    Return to b.

2.  Repeat the above steps for replicas of the cluster as a whole.
3.  Within each location, rebalance using the original rebalancing
    algorithm, as if the location were a cluster of its own.

I don’t think this algorithm is guaranteed to produce a balanced cluster
in all cases, but in cases where the replication factor is small
compared to the size of all of the locations it should produce an
optimal outcome. In fact, there may exist a better balancing algorithm,
but it’s not worth spending a lot of time devising, implementing, and
supporting it as we will almost surely eventually adopt a very different
stochastic gradient descent sort of algorithm one rebalancing is built
into the master and repurposed to run constantly in the background.
Furthermore, I think this algorithm will produce decent results in odd
cases of high replication factors and small locations, where either the
placement policy cannot be complied with at all or where \#1 from above
could limit rebalancing. We may make small tweaks to the algorithm for
these cases if experiment or experience indicates it is necessary.

### Tools and documentation

#### kudu tool (excluding rebalancing)

The ksck tool will be location-aware: it will list tablet server
locations and highlight tablets that violate placement policies, at the
warning level. Additionally, the kudu tserver list tool will be able to
list locations.
