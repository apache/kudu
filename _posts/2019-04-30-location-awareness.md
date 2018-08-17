---
layout: post
title: "Location Awareness in Kudu"
author: Alexey Serbin
---

This post is about location awareness in Kudu. It gives an overview
of the following:
- principles of the design
- restrictions of the current implementation
- potential future enhancements and extensions

<!--more-->

Introduction
==========
Kudu supports location awareness starting with the 1.9.0 release. The
initial implementation of location awareness in Kudu is built to satisfy the
following requirement:

- In a Kudu cluster consisting of multiple servers spread over several racks,
  place the replicas of a tablet in such a way that the tablet stays available
  even if all the servers in a single rack become unavailable.

A rack failure can occur when a hardware component shared among servers in the
rack, such as a network switch or power supply, fails. More generally,
replace 'rack' with any other aggregation of nodes (e.g., chassis, site,
cloud availability zone, etc.) where some or all nodes in an aggregate become
unavailable in case of a failure. This even applies to a datacenter if the
network latency between datacenters is low. This is why we call the feature
_location awareness_ and not _rack awareness_.

Locations in Kudu
==========
In Kudu, a location is defined by a string that begins with a slash (`/`) and
consists of slash-separated tokens each of which contains only characters from
the set `[a-zA-Z0-9_-.]`. The components of the location string hierarchy
should correspond to the physical or cloud-defined hierarchy of the deployed
cluster, e.g. `/data-center-0/rack-09` or `/region-0/availability-zone-01`.

The design choice of using hierarchical paths for location strings is
partially influenced by HDFS. The intention was to make it possible using
the same locations as for existing HDFS nodes, because it's common to deploy
Kudu alongside HDFS. In addition, the hierarchical structure of location
strings allows for interpretation of those in terms of common ancestry and
relative proximity. As of now, Kudu does not exploit the hierarchical
structure of the location except for the client's logic to find the closest
tablet server. However, we plan to leverage the hierarchical structure
in future releases.

Defining and assigning locations
==========
Kudu masters assign locations to tablet servers and clients.

Every Kudu master runs the location assignment procedure to assign a location
to a tablet server when it registers. To determine the location for a tablet
server, the master invokes an executable that takes the IP address or hostname
of the tablet server and outputs the corresponding location string for the
specified IP address or hostname. If the executable exits with non-zero exit
status, that's interpreted as an error and masters add corresponding error
message about that into their logs. In case of tablet server registrations
such outcome is deemed as a registration failure and the corresponding tablet
server is not added into the master's registry. The latter renders the tablet
server unusable to Kudu clients since non-registered tablet servers are not
discoverable to Kudu clients via `GetTableLocations` RPC.

The master associates the produced location string with the registered tablet
server and keeps it until the tablet server re-registers, which only occurs
if the master or tablet server restarts. Masters use the assigned location
information internally to make replica placement decisions, trying to place
replicas evenly across locations and to keep tablets available in case all
tablet servers in a single location fail (see
[the design document](https://s.apache.org/location-awareness-design)
for details). In addition, masters provide connected clients with
the information on the client's assigned location, so the clients can make
informed decisions when they attempt to read from the closest tablet server.
Kudu tablet servers themselves are location agnostic, at least for now,
so the assigned location is not reported back to a registered tablet server.

The location-aware placement policy for tablet replicas in Kudu
==========
While placing replicas of tablets in location-aware cluster, Kudu uses a best
effort approach to adhere to the following principle:
- Spread replicas across locations so that the failure of tablet servers
  in one location does not make tablets unavailable.

That's referred to as the _replica placement policy_ or just _placement policy_.
In Kudu, both the initial placement of tablet replicas and the automatic
re-replication are governed by that policy. As of now, that's the only
replica placement policy available in Kudu. The placement policy isn't
customizable and doesn't have any configurable parameters.

Automatic re-replication and placement policy
==========
By design, keeping the target replication factor for tablets has higher
priority than conforming to the replica placement policy. In other words,
when bringing up tablet replicas to replace failed ones, Kudu uses a best-effort
approach with regard to conforming to the constraints of the placement policy.
Essentially, that means that if there isn't a way to place a replica to conform
with the placement policy, the system places the replica anyway. The resulting
violation of the placement policy can be addressed later on when unreachable
tablet servers become available again or the misconfiguration is addressed.
As of now, to fix the resulting placement policy violations it's necessary
to run the CLI rebalancer tool manually (see below for details),
but in future releases that might be done [automatically in background](
https://issues.apache.org/jira/browse/KUDU-2780).

An example of location-aware rebalancing
==========
This section illustrates what happens during each phase of the location-aware
rebalancing process.

In the diagrams below, the larger outer boxes denote locations, and the
smaller inner ones denote tablet servers. As for the real-world objects behind
locations in this example, one might think of server racks with a shared power
supply or a shared network switch. It's assumed that no more than one tablet
server is run at each node (i.e. machine) in a rack.

The first phase of the rebalancing process is about detecting violations and
reinstating the placement policy in the cluster. In the diagram below, there
are three locations defined: `/L0`, `/L1`, `/L2`. Each location has two tablet
servers. Table `A` has the replication factor of three (RF=3) and consists of
four tablets: `A0`, `A1`, `A2`, `A3`. Table `B` has replication factor of five
(RF=5) and consists of three tablets: `B0`, `B1`, `B2`.

The distribution of the replicas for tablet `A0` violates the placement policy.
Why? Because replicas `A0.0` and `A0.1` constitute the majority of replicas
(two out of three) and reside in the same location `/L0`.

```
         /L0                     /L1                    /L2
+-------------------+   +-------------------+  +-------------------+
|   TS0      TS1    |   |   TS2      TS3    |  |   TS4      TS5    |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
| | A0.0 | | A0.1 | |   | | A0.2 | |      | |  | |      | |      | |
| |      | | A1.0 | |   | | A1.1 | |      | |  | | A1.2 | |      | |
| |      | | A2.0 | |   | | A2.1 | |      | |  | | A2.2 | |      | |
| |      | | A3.0 | |   | | A3.1 | |      | |  | | A3.2 | |      | |
| | B0.0 | | B0.1 | |   | | B0.2 | | B0.3 | |  | | B0.4 | |      | |
| | B1.0 | | B1.1 | |   | | B1.2 | | B1.3 | |  | | B1.4 | |      | |
| | B2.0 | | B2.1 | |   | | B2.2 | | B2.3 | |  | | B2.4 | |      | |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
+-------------------+   +-------------------+  +-------------------+
```

The location-aware rebalancer should initiate movement either of `T0.0` or
`T0.1` from `/L0` to other location, so the resulting replica distribution would
_not_ contain the majority of replicas in any single location. In addition to
that, the rebalancer tool tries to evenly spread the load across all locations
and tablet servers within each location. The latter narrows down the list
of the candidate replicas to move: `A0.1` is the best candidate to move from
location `/L0`, so location `/L0` would not contain the majority of replicas
for tablet `A0`. The same principle dictates the target location and the target
tablet server to receive `A0.1`: that should be tablet server `TS5` in the
location `/L2`. The result distribution of the tablet replicas after the move
is represented in the diagram below.

```
         /L0                     /L1                    /L2
+-------------------+   +-------------------+  +-------------------+
|   TS0      TS1    |   |   TS2      TS3    |  |   TS4      TS5    |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
| | A0.0 | |      | |   | | A0.2 | |      | |  | |      | | A0.1 | |
| |      | | A1.0 | |   | | A1.1 | |      | |  | | A1.2 | |      | |
| |      | | A2.0 | |   | | A2.1 | |      | |  | | A2.2 | |      | |
| |      | | A3.0 | |   | | A3.1 | |      | |  | | A3.2 | |      | |
| | B0.0 | | B0.1 | |   | | B0.2 | | B0.3 | |  | | B0.4 | |      | |
| | B1.0 | | B1.1 | |   | | B1.2 | | B1.3 | |  | | B1.4 | |      | |
| | B2.0 | | B2.1 | |   | | B2.2 | | B2.3 | |  | | B2.4 | |      | |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
+-------------------+   +-------------------+  +-------------------+
```

The second phase of the location-aware rebalancing is about moving tablet
replicas across locations to make the locations' load more balanced. For the
number `S` of tablet servers in a location and the total number `R` of replicas
in the location, the _load of the location_ is defined as `R/S`.

At this stage all violations of the placement policy are already rectified. The
rebalancer tool doesn't attempt to make any moves which would violate the
placement policy.

The load of the locations in the diagram above:
- `/L0`: 1/5
- `/L1`: 1/5
- `/L2`: 2/7

A possible distribution of the tablet replicas after the second phase is
represented below. The result load of the locations:
- `/L0`: 2/9
- `/L1`: 2/9
- `/L2`: 2/9

```
         /L0                     /L1                    /L2
+-------------------+   +-------------------+  +-------------------+
|   TS0      TS1    |   |   TS2      TS3    |  |   TS4      TS5    |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
| | A0.0 | |      | |   | | A0.2 | |      | |  | |      | | A0.1 | |
| |      | | A1.0 | |   | | A1.1 | |      | |  | | A1.2 | |      | |
| |      | | A2.0 | |   | | A2.1 | |      | |  | | A2.2 | |      | |
| |      | | A3.0 | |   | | A3.1 | |      | |  | | A3.2 | |      | |
| | B0.0 | |      | |   | | B0.2 | | B0.3 | |  | | B0.4 | | B0.1 | |
| | B1.0 | | B1.1 | |   | |      | | B1.3 | |  | | B1.4 | | B2.2 | |
| | B2.0 | | B2.1 | |   | | B2.2 | | B2.3 | |  | | B2.4 | |      | |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
+-------------------+   +-------------------+  +-------------------+
```

The third phase of the location-aware rebalancing is about moving tablet
replicas within each location to make the distribution of replicas even,
both per-table and per-server.

See below for a possible replicas' distribution in the example scenario
after the third phase of the location-aware rebalancing successfully completes.

```
         /L0                     /L1                    /L2
+-------------------+   +-------------------+  +-------------------+
|   TS0      TS1    |   |   TS2      TS3    |  |   TS4      TS5    |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
| | A0.0 | |      | |   | |      | | A0.2 | |  | |      | | A0.1 | |
| |      | | A1.0 | |   | | A1.1 | |      | |  | | A1.2 | |      | |
| |      | | A2.0 | |   | | A2.1 | |      | |  | | A2.2 | |      | |
| |      | | A3.0 | |   | | A3.1 | |      | |  | | A3.2 | |      | |
| | B0.0 | |      | |   | | B0.2 | | B0.3 | |  | | B0.4 | | B0.1 | |
| | B1.0 | | B1.1 | |   | |      | | B1.3 | |  | | B1.4 | | B1.2 | |
| | B2.0 | | B2.1 | |   | | B2.2 | | B2.3 | |  | |      | | B2.4 | |
| +------+ +------+ |   | +------+ +------+ |  | +------+ +------+ |
+-------------------+   +-------------------+  +-------------------+
```

How to make a Kudu cluster location-aware
==========
To make a Kudu cluster location-aware, it's necessary to set the
`--location_mapping_cmd` flag for Kudu master(s) and make the corresponding
executable (binary or a script) available at the nodes where Kudu masters run.
In case of multiple masters, it's important to make sure that the location
mappings stay the same regardless of the node where the location assignment
command is running.

It's recommended to have at least three locations defined in a Kudu
cluster so that no location contains a majority of tablet replicas.
With two locations or less it's not possible to spread replicas
of tablets with replication factor of three and higher such that no location
contains a majority of replicas.

For example, running a Kudu cluster in a single datacenter `dc0`, assign
location `/dc0/rack0` to tablet servers running at machines in the rack `rack0`,
`/dc0/rack1` to tablet servers running at machines in the rack `rack1`,
and `/dc0/rack2` to tablet servers running at machines in the rack `rack2`.
In a similar way, when running in cloud, assign location `/regionA/az0`
to tablet servers running in availability zone `az0` of region `regionA`,
and `/regionA/az1` to tablet servers running in zone `az1` of the same region.

An example of location assignment script for Kudu
==========
```
#!/bin/sh
#
# It's assumed a Kudu cluster consists of nodes with IPv4 addresses in the
# private 192.168.100.0/32 subnet. The nodes are hosted in racks, where
# each rack can contain at most 32 nodes. This results in 8 locations,
# one location per rack.
#
# This example script maps IP addresses into locations assuming that RPC
# endpoints of tablet servers are specified via IPv4 addresses. If tablet
# servers' RPC endpoints are specified using DNS hostnames (and that's how
# it's done by default), the script should consume DNS hostname instead of
# an IP address as an input parameter. Check the `--rpc_bind_addresses` and
# `--rpc_advertised_addresses` command line flags of kudu-tserver for details.
#
# DISCLAIMER:
#   This is an example Bourne shell script for Kudu location assignment. Please
#   note it's just a toy script created with illustrative-only purpose.
#   The error handling and the input validation are minimalistic. Also, the
#   network topology choice, supportability and capacity planning aspects of
#   this script might be sub-optimal if applied as-is for real-world use cases.

set -e

if [ $# -ne 1 ]; then
  echo "usage: $0 <ip_address>"
  exit 1
fi

ip_address=$1
shift

suffix=${ip_address##192.168.100.}
if [ -z "${suffix##*.*}" ]; then
  # An IP address from a non-controlled subnet: maps into the 'other' location.
  echo "/other"
  exit 0
fi

# The mapping of the IP addresses
if [ -z "$suffix" -o $suffix -lt 0 -o $suffix -gt 255 ]; then
  echo "ERROR: '$ip_address' is not a valid IPv4 address"
  exit 2
fi

if [ $suffix -eq 0 -o $suffix -eq 255 ]; then
  echo "ERROR: '$ip_address' is a 0xffffff00 IPv4 subnet address"
  exit 3
fi

if [ $suffix -lt 32 ]; then
  echo "/dc0/rack00"
elif [ $suffix -ge 32 -a $suffix -lt 64 ]; then
  echo "/dc0/rack01"
elif [ $suffix -ge 64 -a $suffix -lt 96 ]; then
  echo "/dc0/rack02"
elif [ $suffix -ge 96 -a $suffix -lt 128 ]; then
  echo "/dc0/rack03"
elif [ $suffix -ge 128 -a $suffix -lt 160 ]; then
  echo "/dc0/rack04"
elif [ $suffix -ge 160 -a $suffix -lt 192 ]; then
  echo "/dc0/rack05"
elif [ $suffix -ge 192 -a $suffix -lt 224 ]; then
  echo "/dc0/rack06"
else
  echo "/dc0/rack07"
fi
```

Reinstating the placement policy in a location-aware Kudu cluster
==========
As explained earlier, even if the initial placement of tablet replicas conforms
to the placement policy, the cluster might get to a point where there are not
enough tablet servers to place a new or a replacement replica. Ideally, such
situations should be handled automatically: once there are enough tablet servers
in the cluster or the misconfiguration is fixed, the placement policy should
be reinstated. Currently, it's possible to reinstate the placement policy using
the `kudu` CLI tool:

`sudo -u kudu kudu cluster rebalance <master_rpc_endpoints>`

In the first phase, the location-aware rebalancing process tries to
reestablish the placement policy. If that's not possible, the tool
terminates. Use the `--disable_policy_fixer` flag to skip this phase and
continue to the cross-location rebalancing phase.

The second phase is cross-location rebalancing, i.e. moving tablet replicas
between different locations in attempt to spread tablet replicas among
locations evenly, equalizing the loads of locations throughout the cluster.
If the benefits of spreading the load among locations do not justify the cost
of the cross-location replica movement, the tool can be instructed to skip the
second phase of the location-aware rebalancing. Use the
`--disable_cross_location_rebalancing` command line flag for that.

The third phase is intra-location rebalancing, i.e. balancing the distribution
of tablet replicas within each location as if each location is a cluster on its
own. Use the `--disable_intra_location_rebalancing` flag to skip this phase.

Future work
==========
Having a CLI tool to reinstate placement policy is nice, but it would be great
to run the location-aware rebalancing in background, automatically reinstating
the placement policy and making tablet replica distribution even
across a Kudu cluster.

In addition to that, there is a idea to make it possible to have
multiple customizable placement policies in the system. As of now, there is
a request to implement so-called 'table pinning', i.e. make it possible
to specify placement policy where replicas of tablets of particular tables
are placed only at nodes within the specified locations. The table pinning
request is tracked by KUDU-2604 in Apache JIRA, see
[KUDU-2604](https://issues.apache.org/jira/browse/KUDU-2604).

References
==========
\[1\] Location awareness in Kudu: [design document](
https://github.com/apache/kudu/blob/master/docs/design-docs/location-awareness.md)

\[2\] A proposal for Kudu tablet server labeling: [KUDU-2604](
https://issues.apache.org/jira/browse/KUDU-2604)

\[3\] Further improvement: [automatic cluster rebalancing](
https://issues.apache.org/jira/browse/KUDU-2780).
