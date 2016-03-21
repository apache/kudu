---
layout: post
title: Apache Kudu (incubating) Weekly Update March 21, 2016
author: Todd Lipcon
---
Kudu is a fast-moving young open source project, and we've heard from a few
members of the community that it can be difficult to keep track of what's
going on day-to-day. A typical month comprises 80-100 individual patches
committed and hundreds of code review and discussion
emails. So, inspired by similar weekly newsletters like
[LLVM Weekly](http://llvmweekly.org/) and [LWN's weekly kernel coverage](http://lwn.net/Kernel/)
we're going to experiment with our own weekly newsletter covering
recent development and Kudu-related news.
<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## News and articles from around the web

* Apache Kudu (incubating) 0.7.1 was released last week, and this week
Cloudera announced that its
[Kudu 0.7.1 binaries](http://www.cloudera.com/documentation/betas/kudu/latest/topics/kudu_installation.html)
are now available.
* The Ibis open source project [released version 0.7.0](http://blog.ibis-project.org/release-0.7/)
which includes support for querying Kudu. [An earlier post](http://blog.ibis-project.org/kudu-impala-ibis/)
shows an example of the Ibis-Kudu integration in action.

## Development discussions and code in progress

* Since the first public release, Kudu has had some experimental support for high-availability
master processes. Adar Dembo has started some work identifying the current gaps that make it
experimental and outlining some plans to stabilize it enough for reliable production use.
This week he posted a [design doc](http://gerrit.cloudera.org:8080/#/c/2527/) as well as
some [analysis of CatalogManager code paths](http://gerrit.cloudera.org:8080/#/c/2583/).

* David Alves kicked off a [discussion about version numbers for the Python client](http://markmail.org/message/p4njqwkls7di7aoa).
The discussion trailed off with some general agreement that the Python client would
keep to its own version numbering scheme for the time being, rather than being tied
to specific Kudu versions.

* Binglin Chang's patch for [per-tablet write quotas](http://gerrit.cloudera.org:8080/#/c/2327/)
and associated throttling passed review and should be committed soon. This adds a very basic
throttling mechanism that can provide for some basic isolation and capacity planning for
multi-tenant clusters. It will be considered an experimental feature in the 0.8.0 release.

* Dan Burkert finished off some patches originally prototyped by Todd Lipcon which add
two types of "feature flags" for Kudu's RPC system. Feature flags are a mechanism by which
Kudu clients and servers can expose to one another what specific features they support,
enabling better cross-version compatibility. The [first type of feature flag](http://gerrit.cloudera.org:8080/#/c/2238/)
enumerates the features supported by the RPC system itself. The [second type](http://gerrit.cloudera.org:8080/#/c/2239/)
are scoped to an individual call, and can be used by a client to assert that the server
is new enough to properly interpret requests. The commit messages and code documentation
included in these patches give example use cases.

* Dan also finished off most of a patch series implementing improved partition pruning
support. This will help reduce the number of servers that a client must contact when
scanning a table with predicates (e.g. by eliminating tablets from a hash-partitioned
table when the scanner specifies an equality predicate on the hash-distributed column).
More details can be found in the
[partition-pruning design doc](https://github.com/apache/incubator-kudu/blob/master/docs/design-docs/scan-optimization-partition-pruning.md)
as well as the patches ([1](http://gerrit.cloudera.org:8080/#/c/2137/),
[2](http://gerrit.cloudera.org:8080/#/c/2413/),
[3](http://gerrit.cloudera.org:8080/#/c/2138/)).

## Upcoming talks and meetups

* Dan Burkert will be presenting his work on a
[Rust language client library for Kudu](https://github.com/danburkert/kudu-rs)
at the [Rust Bay Area](http://www.meetup.com/Rust-Bay-Area/events/229107276/) meetup.
