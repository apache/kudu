---
layout: post
title: Apache Kudu (incubating) Weekly Update May 16, 2016
author: Todd Lipcon
---
Welcome to the ninth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Development and code reviews continued on Sameer Abhyankar's patch which
  adds support for pushing down ['IN' predicates](http://gerrit.cloudera.org:8080/#/c/2986/)
  to the Kudu tablet servers.

* Todd Lipcon and Binglin Chang have been continuing to work on improving throughput
  for a high throughput random-read use case. Initial profiling indicated that the
  RPC system was a bottleneck, and patches have started to land which improve
  the throughput:

  The largest bottleneck was in the queue which transfers RPC calls from the
  libev "reactor" threads which perform network IO to the "worker" threads
  which service the actual requests. Binglin borrowed some ideas from Facebook's
  [folly](https://github.com/facebook/folly) library, and implemented an
  [improved queue](http://gerrit.cloudera.org:8080/#/c/2938/)
  which reduces context switches and lock contention while also
  improving CPU cache locality of the worker threads.

  Todd identified that the hash function used to map connections to reactor
  threads was poor, resulting in uneven load distribution across cores.
  A [simple patch to change the hashcode implementation](http://gerrit.cloudera.org:8080/#/c/2939/)
  improved the distribution substantially.

  With just these patches, an RPC stress benchmark was improved from about 202K RPCs/second
  to 768K RPCs/second on a 24-core machine. Further improvements are in flight
  and under review this week.

* Zhen Zhang is continuing to focus on adding more visibility into
  performance and resource usage by adding the ability to propagate various
  per-operation metrics from the server side back to the client. His latest patch
  under review [exposes scanner cache hit rate metrics](http://gerrit.cloudera.org:8080/#/c/3013/)
  to the client.

* Todd Lipcon and Sarah Jelinek continue to make progress on the
  implementation of a persistent-memory backed block cache.
  This week a [substantial refactor to the block cache interface](http://gerrit.cloudera.org:8080/#/c/2957/)
  was committed in preparation for the [NVM cache itself](http://gerrit.cloudera.org:8080/#/c/2593/).

* Congratulations to Will Berkeley, a new contributor who has been
  contributing small fixes and improvements such as
  [exposing table partitioning information in the master web UI](http://gerrit.cloudera.org:8080/#/c/3022/).
  Thanks, Will!

* David Alves has been continuing to make progress towards his implementation of
  the [Replay Cache](http://gerrit.cloudera.org:8080/#/c/2642/).
  This week, he refactored and cleaned up much of the client code involving
  error handling and retrying write operations, in preparation to inserting
  unique identifiers for these and other operations.

* Chris George has continued to work on the Spark DataSource implementation.
  In particular, work is progressing on support for [inserting and updating
  rows via Spark](http://gerrit.cloudera.org:8080/#/c/2992/).

* Todd Lipcon and Mike Percy both committed improvements which will help speed up
  startup. Measurements on a cluster where each node stores a few TB of data
  showed a 3x improvement in startup time.

## Upcoming talks and meetups

* Mladen Kovacevi will be presenting Kudu at the
  [Big Data Montreal](http://www.meetup.com/Big-Data-Montreal/events/230879277/?eventId=230879277)
  meetup.
