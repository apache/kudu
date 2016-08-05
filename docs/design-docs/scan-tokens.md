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

# Scan Token API

## Motivation

Most distributed compute frameworks that integrate with Kudu need the ability to
split Kudu tables into physical sections so that computation can be distributed
and parallelized. Additionally, these frameworks often want to take advantage of
data locality by executing tasks on or close to the physical machines that hold
the data they are working on.

Kudu should have a well defined API that all compute integrations can use to
implement parallel scanning with locality hints. This API should be amenable to
serialization so that individual scanner tasks may be shipped to remote task
executors.

## Design

Kudu will provide a client API that takes a scan description (e.g. table name,
projected columns, fault tolerance, snapshot timestamp, lower and upper primary
key bounds, predicates, etc.) and returns a sequence of scan tokens. For
example:

```java
ScanTokenBuilder builder = client.newScanTokenBuilder();
builder.setProjectedColumnNames(ImmutableList.of("col1", "col2"));
List<ScanToken> tokens = builder.build();
```

Scan tokens may be used to create a scanner over a single tablet. Additionally,
scan tokens have a well defined, but opaque to the client, serialization format
so that tokens may be serialized and deserialized by the compute framework, and
even passed between processes using different Kudu client versions or
implementations (JVM vs. C++). Continuing the previous example:

```java
byte[] serializedToken = tokens.get(0).serialize();

// later, possibly in a different process

KuduScanner scanner = ScanToken.deserializeIntoScanner(serializedToken, client);
```

Along with the serializable scan token, the API will provide a location hint
containing the replicas hosting the data. This will be done via the existing
replica location APIs (`org.apache.kudu.client.LocatedTablet` in the Java client, and
`std::vector<KuduTabletServer*>` in the C++ client).

Initially, the scan token API should support creating a single token per tablet
in the table (less tablets which may be pruned, if the client supports pruning,
see the [partition pruning design doc](scan-optimization-partition-pruning.md)).
Internally, limiting a token to a single tablet should be done by including
partition key limits in the token, and setting those limits on the scanner when
deserializing the token. Alternatively, the tablet ID could be directly included
in the token, but this may have unintended consequences if tablet splitting
features are added to Kudu. A token could be created before a split event, with
the resulting scan happening after the split. By setting tablet limits through
partition key bounds instead of tablet IDs, it is clear that the scanner should
retrieve results from all of the child tablets.

Eventually, the scan token API should allow applications to further split scan
tokens so that inter-tablet parallelism can be acheived. Splitting tokens may be
achieved by assigning the child tokens non-overlapping sections of the primary
key range. Even without the token splitting feature built in to the API,
applications can simulate the effect by building multiple sets of scan tokens
using non-overlapping sets of primary key bounds. However, it is likely that in
the future Kudu will be able to choose a more optimal primary key split point
than the application, perhaps through an internal tablet statistics API.
Additionally, having the API built in to the Kudu client further decreases the
effort required to write high performance integrations for Kudu.
