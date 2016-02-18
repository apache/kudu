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
| [Scan Optimization & Partition Pruning](scan-optimization-partition-pruning.md) | Client, Tablet | [gerrit](http://gerrit.cloudera.org:8080/#/c/2149/) |
| [CFile format](cfile.md) | Tablet | N/A |
| [Codegen API and impl. details](codegen.md) | Server | N/A |
| [Consensus design](consensus.md) | Consensus | N/A |
| [Master design](master.md) | Master | N/A |
| [RPC design and impl. details](rpc.md) | RPC | N/A |
| [Tablet design, impl. details and comparison to other systems](tablet.md) | Tablet | N/A |
| [C++ client design and impl. details](cpp-client.md) | Client | N/A |
