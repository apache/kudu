---
layout: post
title: "Using Raft Consensus on a Single Node"
author: Mike Percy
---

As Kudu marches toward its 1.0 release, which will include support for
multi-master operation, we are working on removing old code that is no longer
needed. One such piece of code is called LocalConsensus. Once LocalConsensus is
removed, we will be using Raft consensus even on Kudu tables that have a
replication factor of 1.

<!--more-->

Using Raft consensus in single-node cases is important for multi-master
support because it will allow people to dynamically increase their Kudu
cluster's existing master server replication factor from 1 to many (3 or 5 are
typical).

The Consensus interface
=======================

In Kudu, the
[Consensus](https://github.com/apache/incubator-kudu/blob/branch-0.9.x/src/kudu/consensus/consensus.h)
interface was created as an abstraction to allow us to build the plumbing
around how a consensus implementation would interact with the underlying
tablet. We were able to build out this "scaffolding" long before our Raft
implementation was complete.

The Consensus API has the following main responsibilities:

1. Support acting as a Raft `LEADER` and replicate writes to a local
   write-ahead log (WAL) as well as followers in the Raft configuration. For
   each operation written to the leader, a Raft implementation must keep track
   of how many nodes have written a copy of the operation being replicated, and
   whether or not that constitutes a majority. Once a majority of the nodes
   have written a copy of the data, it is considered committed.
2. Support acting as a Raft `FOLLOWER` by accepting writes from the leader and
   preparing them to be eventually committed.
3. Support voting in and initiating leader elections.
4. Support participating in and initiating configuration changes (such as going
   from a replication factor of 3 to 4).

The first implementation of the Consensus interface was called LocalConsensus.
LocalConsensus only supported acting as a leader of a single-node configuration
(hence the name "local"). It could not replicate to followers, participate in
elections, or change configurations. These limitations have led us to
[remove](https://gerrit.cloudera.org/3350) LocalConsensus from the code base
entirely.

Because Kudu has a full-featured Raft implementation, Kudu's RaftConsensus
supports all of the above functions of the Consensus interface.

Using a Single-node Raft configuration
======================================

A common question on the Raft mailing lists is: "Is it even possible to use
Raft on a single node?" The answer is yes.

Fundamentally, Raft works by first electing a leader that is responsible for
replicating write operations to the other members of the configuration. In
order to elect a leader, Raft requires a (strict) majority of the voters to
vote "yes" in an election. When there is only a single eligible node in the
configuration, there is no chance of losing the election. Raft specifies that
when starting an election, a node must first vote for itself and then contact
the rest of the voters to tally their votes. If there is only a single node, no
communication is required and an election succeeds instantaneously.

So, when does it make sense to use Raft for a single node?

It makes sense to do this when you want to allow growing the replication factor
in the future. This is something that Kudu needs to support. When deploying
Kudu, someone may wish to test it out with limited resources in a small
environment. Eventually, they may wish to transition that cluster to be a
staging or production environment, which would typically require the fault
tolerance achievable with multi-node Raft. Without a consensus implementation
that supports configuration changes, there would be no way to gracefully
support this. Because single-node Raft supports dynamically adding an
additional node to its configuration, it is possible to go from one replica to
2 and then 3 replicas and end up with a fault-tolerant cluster without
incurring downtime.

More about Raft
===============

To learn more about how Kudu uses Raft consensus, you may find the relevant
[design docs](https://github.com/apache/incubator-kudu/blob/master/docs/design-docs/README.md)
interesting. In the future, we may also post more articles on the Kudu blog
about how Kudu uses Raft to achieve fault tolerance.

To learn more about the Raft protocol itself, please see the [Raft consensus
home page](https://raft.github.io/). The design of Kudu's Raft implementation
is based on the extended protocol described in Diego Ongaro's Ph.D.
dissertation, which you can find linked from the above web site.
