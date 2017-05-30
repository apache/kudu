---
layout: post
title: Consistency in Apache Kudu, Part 1
author: David Alves
---
In this series of short blog posts we will introduce Kudu’s consistency model,
its design and ultimate goals, current features, and next steps.
On the way, we’ll shed some light on the more relevant components and how they
fit together.

In Part 1 of the series (this one), we’ll cover motivation and design trade-offs, the end goals and
the current status.

<!--more-->

## What is “consistency” and why is it relevant?

In order to cope with ever increasing data volumes, modern storage systems like Kudu have to support
many concurrent users while coordinating requests across many machines, each with many threads executing
work at the same time. However, application developers shouldn’t have to understand the internal
details of how these systems implement this parallel, distributed, execution in order to write
correct applications. _Consistency in the context of parallel, distributed systems roughly
refers to how the system behaves in comparison to a single-machine, single-thread system_. In a
single-threaded, single-machine storage system operations happen one-at-a-time, in a clearly
defined order, making correct applications easy to code and reason about. A developer writing an
application against such a system doesn’t have to care about how simultaneous operations interact
or about ordering anomalies, so the code is simpler, but more importantly, cognitive load is greatly
reduced, freeing focus for the application logic itself.

While such a simple system is definitely possible to build, it wouldn’t be able to cope with very
large amounts of data. In order to deal with big data volumes and write throughputs modern storage
systems like Kudu are designed to be distributed, storing and processing data across many machines
and cores. This means that many things happen simultaneously in the same and different machines,
that there are more moving parts and thus more oportunity for mis-orderings and for components
to fail. How far systems like Kudu go (or don’t go) in emulating the simple single-threaded, single-machine
system a distributed, parallel setting where failures are common is roughly what is referred to
as how _“consistent”_ the system is.

_Consistency_ as a term is somewhat overloaded in the distributed systems and database communities,
there are many different models, properties, different names for the same concept, and often
different concepts under the same name. This post is not meant to introduce these concepts
as there are excellent references already available elsewhere (we recommend Kyle Kinsbury's excellent
series of blog posts on the matter, like [this one](https://aphyr.com/posts/313-strong-consistency-models)).
Throughout this and follow-up posts we’ll refer to consistency loosely as the __C__ in __CAP__[1]
in some cases and as the __I__ in __ACID__[2] in others; we’ll try to be specific when relevant.

## Design decisions, trade-offs and motivation

Consistency is essentially about ordering and ordering usually has a cost. Distributed storage
system design must choose to prioritize some properties over others according to the target use
cases. That is, trade-offs must be made or, borrowing a term from economics, there is
“no free lunch”. Different systems choose different trade-off points; for instance, systems inspired by _Dynamo_[3], usually favor availability in the consistency/availability
trade-off: by allowing a write to a data item to succeed even when a majority (or even all) of the
replicas serving that data item are unreachable, Dynamo’s design is minimizing insertion errors and
insert latency (related to availability) at the cost having to perform extra work for value
reconciliation on reads and possibly returning stale or disordered values (related to consistency).
On the other end of the spectrum, traditional DBMS design is often driven by the need to support
transactions of arbitrary complexity while providing the users stronger, predictable, semantics,
favoring consistency at the cost of scalability and availability.

Kudu’s overarching goal is to enable _fast analytic workloads over large amounts of mutable_ data,
 meaning it was designed to perform fast scans over large volumes of data stored in many servers.
In practical terms this means that, when given a choice, more often than not, we opted for the
design that would enable Kudu to have faster scan performance (i.e. favoring reads even if it meant pushing
a bit more work to the path that mutates data, i.e. writes). This does not mean that the write path
was not a concern altogether. In fact, modern storage systems like _Google’s Spanner_[4]
global-scale database demonstrate that, with the right set of trade-offs, it is possible to have strong
consistency semantics with write latencies and overall availability that are adequate for most use
cases (e.g. Spanner achieves 5 9's of availability). For the write path, we often made similar choices in Kudu.

Another important aspect that directed our design decisions is the type of _write workload_ we targeted.
Traditionally, analytical storage systems target periodic bulk write workloads and a continuous
stream of analytical scans. This design is often problematic in that it forces users to have to
build complex pipelines where data is accumulated in one place for later loading into the storage
 system. Moreover, beyond the architectural complexity, this kind of design usually also
means that the data that is available for analytics is not the most recent. In Kudu we aimed for
enabling continuous ingest, i.e. having a continuous stream of small writes, obviating the need to
assemble a pipeline for data accumulation/loading and allowing analytical scans to have access to
the most recent data. Another important aspect of the write workloads that we targeted in Kudu is
that they are append-mostly, i.e. most insert new values into the table, with a smaller percentage
updating currently existing values. Both the average write size and the data distribution influence
the design of the write path, as we’ll see in the following sections.

One last concern we had in mind is that different users have different needs when it comes to
consistency semantics, particularly as it applies to an analytical storage system like Kudu. For
some users consistency isn’t a primary concern, they just want fast scans, and the ability to
update/insert/delete values without needing to build a complex pipeline. For example, many machine
learning models are mostly insensitive to data recency or ordering so, when using Kudu to store data that
will be used to train such a model, consistency is often not as primary a concern as read/write performance is.
 In other cases consistency is a much higher priority. For example, when using Kudu to
store transaction data for fraud analysis it might be important to capture if events are causally
related. Fraudulent transactions might be characterized by a specific sequence of events and when
retrieving that data it might be important for the scan result to reflect that sequence. Kudu’s
design allows users to make a trade-off between consistency and performance at scan time. That is,
users can choose to have stronger consistency semantics for scans at the penalty of latency and
throughput or they can choose to weaken the consistency semantics for an extra performance boost.

### Note

<blockquote>
<p>Kudu currently lacks support for atomic multi-row mutation operations (i.e. mutation
operations to more than one row in the same or different tablets, planned as a future feature).
So, when discussing writes, we’ll be talking about the consistency semantics of single row mutations.
In this context we’ll discuss Kudu’s properties more from a key/value store standpoint. On the
other hand Kudu is an analytical storage engine so, for the read path, we’ll also discuss the
semantics of large (multi-row) scans. This moves the discussion more into the field of traditional
DBMSs. These ingredients make for a non-traditional discussion that is not exactly apples-to-apples
with what the reader might be familiar with, but our hope is that it still provides valuable, or
at least interesting, insight.</p>
</blockquote>

## Consistency options in Kudu

Consistency, as well as other properties, are underpinned in Kudu by the concept of a _timestamp_.
In follow-up posts we'll look into detail how these are assigned and how they are assembled. For now
it's sufficient to know that a timestamp is a single, usually large, number that has some mapping
to wall time. Each mutation of a Kudu row is tagged with one such timestamp. Globally, these timestamps
form a partial order over all the rows with the particularity that causally related mutations (e.g.
a write mutation that is the result of the value obtained from a previous read) may be required to
have increasing timestamps, depending on the user's choices.

Row mutations performed by a single client _instance_ are guaranteed to have increasing timestamps
thus reflecting their potential causal relationship. This property is always enforced. However
there are two major _"knobs"_ that are available to the user to make performance trade-offs, the
`Read` mode, and the `External Consistency` mode (see [here](https://kudu.apache.org/docs/transaction_semantics.html)
for more information on how to use the relevant APIs).

The first and most important knob, the `Read` mode, pertains to what is the guaranteed recency of
data resulting from scans. Since Kudu uses replication for availability and fault-tolerance, there
are always multiple replicas of any data item.
Not all replicas must be up-to-date so if the user cares about recency, e.g. if the user requires
that any data read includes all previously written data _from a single client instance_ then it must
choose the `READ_AT_SNAPSHOT` read mode. With this mode enabled the client is guaranteed to observe
 __"READ YOUR OWN WRITES"__ semantics, i.e. scans from a client will always include all previous mutations
performed by that client. Note that this property is local to a single client instance, not a global
property.

The second "knob", the `External Consistency` mode, defines the semantics of how reads and writes
are performed across multiple client instances. By default, `External Consistency` is set to
 `CLIENT_PROPAGATED`, meaning it's up to the user to coordinate a set of _timestamp tokens_ with clients (even
across different machines) if they are performing writes/reads that are somehow causally linked.
If done correctly this enables __STRICT SERIALIZABILITY__[5], i.e. __LINEARIZABILITY__[6] and
__SERIALIZABILITY__[7] at the same time, at the cost of having the user coordinate the timestamp
tokens across clients (a survey of the meaning of these, and other definitions can be found
[here](http://www.ics.forth.gr/tech-reports/2013/2013.TR439_Survey_on_Consistency_Conditions.pdf)).
The alternative setting for `External Consistency` is to have it set to
`COMMIT_WAIT` (experimental), which guarantees the same properties through a different means, by
implementing Google Spanner's _TrueTime_. This comes at the cost of higher latency (depending on how
tightly synchronized the system clocks of the various tablet servers are), but doesn't require users
to propagate timestamps programmatically.

## Next up

In following posts we'll look into the several components of Kudu's architecture that come together
to enable the consistency semantics introduced in the previous section, including:

- Transactions and the Transaction Driver
- Concurrent execution with Multi-Version Concurrency Control
- Exactly-Once semantics with Replay Cache
- Replication, Crash Recovery with Consensus and the Write-Ahead-Log
- Time keeping and timestamp assignment

## References

[[1]](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.24.3690&rep=rep1&type=pdf): Armando Fox and Eric A. Brewer. 1999. Harvest, Yield, and Scalable Tolerant Systems. In Proceedings of the The Seventh Workshop on Hot Topics in Operating Systems (HOTOS '99). IEEE Computer Society, Washington, DC, USA.

[[2]](https://en.wikipedia.org/wiki/ACID): ACID - Wikipedia entry

[[3]](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf): Giuseppe DeCandia, Deniz Hastorun, Madan Jampani, Gunavardhan Kakulapati, Avinash Lakshman, Alex Pilchin, Swaminathan Sivasubramanian, Peter Vosshall, and Werner Vogels. 2007. Dynamo: amazon's highly available key-value store. In Proceedings of twenty-first ACM SIGOPS symposium on Operating systems principles (SOSP '07). ACM, New York, NY, USA.

[[4]](https://research.google.com/archive/spanner-osdi2012.pdf): James C. Corbett, Jeffrey Dean, Michael Epstein, Andrew Fikes, Christopher Frost, J. J. Furman, Sanjay Ghemawat, Andrey Gubarev, Christopher Heiser, Peter Hochschild, Wilson Hsieh, Sebastian Kanthak, Eugene Kogan, Hongyi Li, Alexander Lloyd, Sergey Melnik, David Mwaura, David Nagle, Sean Quinlan, Rajesh Rao, Lindsay Rolig, Yasushi Saito, Michal Szymaniak, Christopher Taylor, Ruth Wang, and Dale Woodford. 2012. Spanner: Google's globally-distributed database. In Proceedings of the 10th USENIX conference on Operating Systems Design and Implementation (OSDI'12). USENIX Association, Berkeley, CA, USA.

[[5]](https://pdfs.semanticscholar.org/fafa/ebf830bc900bccc5e4fd508fd592f5581cbe.pdf): Gifford, David K. Information storage in a decentralized computer system. Diss. Stanford University, 1981.

[[6]](http://www.doc.ic.ac.uk/~gbd10/aw590/Linearizability%20-%20A%20Correctness%20Condition%20for%20Concurrent%20Objects.pdf): Herlihy, Maurice P., and Jeannette M. Wing. "Linearizability: A correctness condition for concurrent objects." ACM Transactions on Programming Languages and Systems (TOPLAS) 12.3 (1990): 463-492.

[[7]](http://www.dtic.mil/get-tr-doc/pdf?AD=ADA078414): Papadimitriou, Christos H. "The serializability of concurrent database updates." Journal of the ACM (JACM) 26.4 (1979): 631-653.