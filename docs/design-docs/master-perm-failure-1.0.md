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

# Permanent failure handling of masters for Kudu 1.0

## Background

Kudu's 1.0 release includes various improvements to multi-master support so that
it can be used in production safely. The original release plan emphasized using
multiple masters for high availability in the event of transient failures, but
unfortunately didn't talk much (if at all) about permanent failures. This
document compares transient and permanent failures, and provides a design for
addressing the latter.

### Transient failures

Kudu's handling of transient master failures is best illustrated with an
example. Assume we have a healthy Raft configuration consisting of three
masters. If one master suffers a transient failure and is offline for a short
time before returning, there's no harm. If the failed node was a follower, the
leader can still replicate to a majority of nodes. If the leader itself failed,
a majority of nodes can still elect a new leader. No new machinery is needed to
support this; all of the code has been written and tested with the caveat that
there are some bugs that we have been squashing over the past few months.

### Permanent failures

What's missing, however, is handling for permanent failures. If a node dies and
is not coming back, we need to replace it with a healthy one ASAP. If we don't,
a second failure (transient or otherwise) will lead to a loss of availability.

## Design proposal for handling permanent failures

In practice, the most straight-forward approach to handling any permanent
failure is to extend Raft configuration change support to the master; currently
it’s only possible to do it in the tserver. However, this just isn't possible
given time constraints. Therefore, we will use a DNS-dependent alternative.

Here is the algorithm:

1. Base state:
   1. There's a healthy Raft configuration of three nodes: **A**, **B**, and
      **C**.
   2. **A** is the leader.
   3. The value of **--master_addresses** (the master-only gflag describing the
      locations of the entire master Raft configuration) is {**A**, **B**,
      **C**} on each node.
   4. Each of **A**, **B**, and **C** are DNS cnames.
   5. The value of **--tserver_master_address** (the tserver-only gflag
      describing the locations of the masters) on each tserver is {**A**, **B**,
      **C**}
2. **C** dies, permanently. If **A** dies, the directions below are the same,
   except replace **A** with whichever node was elected the new leader.
3. Make sure **C** is completely dead and cannot come back to life. If possible,
   destroy its on-disk master state.
4. Find a replacement machine **D**.
5. Modify DNS records such that **D** assumes **C**'s cname.
6. Invoke new command line tool on **D** that uses remote bootstrap to copy
   master state from **A** to **D**.
7. Start a new master on **D**. It should use the same value of
   **--master_addresses** as used by the other masters.

In order to implement this design, we'll need to make the following changes:

1. Make remote bootstrap available for masters (currently it's tserver-only).
2. Implement new remote bootstrap "client" command line tool.

## Migration from single-master deployments

While not exactly related to failure handling, the remote bootstrap
modifications described above can be used to ease migration from a single master
deployment to a multi-master one. Since migration is a rare and singular event
in the lifetime of a cluster, it is assumed that a temporary loss of
availability during the migration is acceptable.

Here is the algorithm:

1. There exists a healthy single-node master deployment called **A**.
2. Find new master machines, creating DNS cnames for all of them. Create a DNS
   cname for **A** too, if it's not already a cname. Note: the total number of
   masters must be odd. To figure out how many masters there should be, consider
   that **N** failures can be tolerated by a deployment of **2N+1** masters.
3. Stop the master running on **A**.
4. Invoke new command line tool to format a filesystem on each new master node.
5. Invoke new command line tool to print the filesystem uuid on each master node
   and on existing master node **A**. Record these UUIDs.
6. Invoke new command line tool on **A** to rewrite the on-disk consensus
   metadata (cmeta) file describing the Raft configuration. Provide the uuid and
   cname for each new master node as well as for **A**.
7. Start the master running on **A**.
8. Invoke remote bootstrap "client" tool from above on each new node to copy
   **A**'s master state onto new node. These invocations can be done in parallel
   to speed up the process, though in practice master state is quite small.
9. Start the master on each new node.

In order to implement this design, we'll need the following additional changes:

1. Implement new command line tool to format filesystems.
2. Implement new command line tool to print filesystem uuids.
3. Implement new command line tool to rewrite cmeta files.
