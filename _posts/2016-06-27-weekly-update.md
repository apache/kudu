---
layout: post
title: Apache Kudu (incubating) Weekly Update June 27, 2016
author: Todd Lipcon
---
Welcome to the fifteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

## Development discussions and code in progress

* Todd Lipcon diagnosed and fixed a [tricky bug](https://gerrit.cloudera.org/3445)
  which could cause Kudu servers to crash under load. It turned out that the bug
  was in a synchronization profiling code path related to the tcmalloc allocator.
  This allocator is used in release builds, but can't be used in instrumented builds
  such as
  [AddressSanitizer](http://clang.llvm.org/docs/AddressSanitizer.html) or
  [ThreadSanitizer](http://clang.llvm.org/docs/ThreadSanitizer.html). This made it particularly difficult
  to catch. The bug fix will be released in the upcoming 0.9.1 release.

* Todd also finished and committed a fix for [KUDU-1469](https://issues.apache.org/jira/browse/KUDU-1469),
  a bug in which Kudu's implementation of Raft consensus could get "stuck" not making
  progress replicating operations for a tablet. See the
  [new integration test case](https://gerrit.cloudera.org/#/c/3228/7/src/kudu/integration-tests/raft_consensus-itest.cc)
  for more details on this bug.

* Mike Percy finished implementing and committed a feature which allows
  [reserving disk space for non-Kudu processes](https://gerrit.cloudera.org/#/c/3135/).
  This feature causes Kudu to stop allocating new data blocks on a
  disk if it is within a user-specified threshold of being full, preventing
  possible crashes and allowing for safer collocation of Kudu with other processes
  on a cluster.

* Will Berkeley finished implementing [KUDU-1398](https://issues.apache.org/jira/browse/KUDU-1398),
  a new optimization which reduces the amount of disk space used by
  indexing structures in Kudu's internal storage format. This should
  improve storage efficiency for workloads with large keys, and can
  also improve write performance by increasing the number of index
  entries which can fit in a given amount of cache memory.

* David Alves has completed posting a patch series that implements
  exactly-once RPC semantics. The design, as mentioned in previous
  blog posts, is described in a [design document](https://gerrit.cloudera.org/#/c/2642/)
  and the patches can be found in a 10-patch series starting with
  [gerrit #3190](https://gerrit.cloudera.org/#/c/3190/).

* Dan Burkert is continuing working on adding support for
  [tables with range partitions that don't cover the entire key
  space](https://github.com/apache/incubator-kudu/blob/master/docs/design-docs/non-covering-range-partitions.md).
  This past week, he focused on adding [support in the the Java client](https://gerrit.cloudera.org/#/c/3388/)
  which also necessitated some serious [refactoring](https://gerrit.cloudera.org/#/c/3477/). These patches
  are now under review.

* Congratulations to Andrew Wong, a new contributor who committed his
  first patches this week. Andrew [improved the build docs for OSX](https://gerrit.cloudera.org/#/c/3424/)
  and also fixed a [crash if the user forgot to specify the master address
  in some command line tools](https://gerrit.cloudera.org/#/c/3486/).
  Thanks, Andrew!

## Project news

* The Apache Kudu web site has finished migrating to Apache Software Foundation infrastructure.
  The site can now be found at [kudu.incubator.apache.org](http://kudu.incubator.apache.org/).
  Existing links will automatically redirect.

* A Kudu 0.9.1 release candidate was posted and passed a
  [release vote](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201606.mbox/%3CCADY20s6%3D%2BnKNgvx%3DG_pKupQGiH%2B9ToS53LqExBwWM6vLp-ns9A%40mail.gmail.com%3E)
  by the Kudu Podling PMC (PPMC).
  The release candidate will now be voted upon by the Apache Incubator PMC. If all goes well, we
  can expect a release late this week. The release fixes a few critical bugs discovered in 0.9.0.

* Chris Mattmann, one of Kudu's mentors from the Apache Incubator,
  started a [discussion](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201606.mbox/%3CAD4A858D-403D-4E74-A4F4-DE2F08FB761E%40jpl.nasa.gov%3E)
  about the project's graduation to a top-level project (TLP).
  Initial responses seem to be positive, so the next step will
  be to work on a draft resolution and various stages of
  voting.


## On the Kudu blog

* Adar Dembo published a post detailing his recent work on
  [master fault tolerance in Kudu 1.0](http://kudu.apache.org/2016/06/24/multi-master-1-0-0.html).


Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
