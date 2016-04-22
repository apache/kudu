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

# Design Docs

This directory holds Kudu design documents. These documents are typically
written from a point-of-time view, and do not necessarily represent the current
state of the system. They are useful for learning why design decisions were
made.

| Document | Component(s) | Discussion |
| -------- | ------------ | ---------- |
| [Scan optimization and partition pruning](scan-optimization-partition-pruning.md) | Client, Tablet | [gerrit](http://gerrit.cloudera.org:8080/2149) |
| [CFile format](cfile.md) | Tablet | N/A |
| [Codegen API and impl. details](codegen.md) | Server | N/A |
| [Consensus design](consensus.md) | Consensus | N/A |
| [Raft config change design](raft-config-change.md) | Consensus | N/A |
| [Tablet Copy design](raft-tablet-copy.md) | Consensus | N/A |
| [Master design](master.md) | Master | N/A |
| [RPC design and impl. details](rpc.md) | RPC | N/A |
| [Tablet design, impl. details and comparison to other systems](tablet.md) | Tablet | N/A |
| [Tablet compaction design and impl.](compaction.md) | Tablet | N/A |
| [Tablet compaction policy](compaction-policy.md) | Tablet | N/A |
| [Schema change design](schema-change.md) | Master, Tablet | N/A |
| [Maintenance operation scheduling](triggering-maintenance-ops.md) | Master, Tablet Server | N/A |
| [C++ client design and impl. details](cpp-client.md) | Client | N/A |
| [(old) Heartbeating between tservers and multiple masters](old-multi-master-heartbeating.md) | Master | [gerrit](http://gerrit.cloudera.org:8080/2495) |
| [Scan Token API](scan-tokens.md) | Client | [gerrit](http://gerrit.cloudera.org:8080/2443) |
| [Full multi-master support for Kudu 1.0](multi-master-1.0.md) | Master, Client | [gerrit](http://gerrit.cloudera.org:8080/2527) |
| [Non-covering Range Partitions](non-covering-range-partitions.md) | Master, Client | [gerrit](http://gerrit.cloudera.org:8080/2772) |
| [Permanent failure handling of masters for Kudu 1.0](master-perm-failure-1.0.md) | Master | |
| [RPC Retry/Failover semantics](rpc-retry-and-failover.md) | Client/TS/Master | [gerrit](http://gerrit.cloudera.org:8080/2642) |
| [Tablet history garbage collection](tablet-history-gc.md) | Tablet | [gerrit](https://gerrit.cloudera.org/2853) |
| [Documentation Style Guide](doc-style-guide.adoc) | Documentation | |
