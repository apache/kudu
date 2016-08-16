---
layout: post
title: Apache Kudu Weekly Update August 16th, 2016
author: Todd Lipcon
---
Welcome to the twentieth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu project.

<!--more-->

## Project news

* The first release candidate for the 0.10.0 is [now available](
  http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201608.mbox/%3CCADY20s7U5jVpozFg3L%3DDz2%2B4AenGineJvH96A_HAM12biDjPJA%40mail.gmail.com%3E)

  Community developers and users are encouraged to download the source
  tarball and vote on the release.

  For information on what's new, check out the
  [release notes](https://github.com/apache/kudu/blob/master/docs/release_notes.adoc#rn_0.10.0).
  *Note:* some links from these in-progress release notes will not be live until the
  release itself is published.

## Development discussions and code in progress

* Will Berkeley spent some time working on the Spark integration this week
  to add support for UPSERT as well as other operations.
  Dan Burkert pitched in a bit with some [suggestions](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201608.mbox/%3CCALo2W-XBoSz9cbhXi81ipubrAYgqyDiEeHz-ys8sPAshfcik6w%40mail.gmail.com%3E)
  which were then integrated in a [patch](https://gerrit.cloudera.org/#/c/3871/)
  provided by Will.

  After some reviews by Dan, Chris George, and Ram Mettu, the patch was committed
  in time for the upcoming 0.10.0 release.

* Dan Burkert also completed work for the new [manual partitioning APIs](https://gerrit.cloudera.org/#/c/3854/)
  in the Java client. After finishing up the basic implementation, Dan also made some
  cleanups to the related APIs in both the [Java](https://gerrit.cloudera.org/#/c/3958/)
  and [C++](https://gerrit.cloudera.org/#/c/3882/) clients.

  Dan and Misty Stanley-Jones also collaborated to finish the
  [documentation](https://gerrit.cloudera.org/#/c/3796/)
  for this new feature.

* Adar Dembo worked on some tooling to allow users to migrate their Kudu clusters
  from a single-master configuration to a multi-master one. Along the way, he
  started building some common infrastructure for command-line tooling.

  Since Kudu's initial release, it has included separate binaries for different
  administrative or operational tools (e.g. `kudu-ts-cli`, `kudu-ksck`, `kudu-fs_dump`,
  `log-dump`, etc). Despite having similar usage, these tools don't share much code,
  and the separate statically linked binaries make the Kudu packages take more disk
  space than strictly necessary.

  Adar's work has introduced a new top-level `kudu` binary which exposes a set of subcommands,
  much like the `git` and `docker` binaries with which readers may be familiar.
  For example, a new tool he has built for dumping peer identifiers from a tablet's
  consensus metadata is triggered using `kudu tablet cmeta print_replica_uuids`.

  This new tool will be available in the upcoming 0.10.0 release; however, migration
  of the existing tools to the new infrastructure has not yet been completed. We
  expect that by Kudu 1.0, the old tools will be removed in favor of more subcommands
  of the `kudu` tool.

* Todd Lipcon picked up the work started by David Alves in July to provide
  ["exactly-once" semantics](https://gerrit.cloudera.org/#/c/2642/) for write operations.
  Todd carried the patch series through review and also completed integration of the
  feature into the Kudu server processes.

  After testing the feature for several days on a large cluster under load,
  the team decided to enable this new feature by default in Kudu 0.10.0.

* Mike Percy resumed working on garbage collection of [past versions of
  updated and deleted rows](https://gerrit.cloudera.org/#/c/2853/). His [main
  patch for the feature](https://gerrit.cloudera.org/#/c/3076/) went through
  several rounds of review and testing, but unfortunately missed the cut-off
  for 0.10.0.

* Alexey Serbin's work to add doxygen-based documentation for the C++ Client API
  was [committed](https://gerrit.cloudera.org/#/c/3840/) this week. These
  docs will be published as part of the 0.10.0 release.

* Alexey also continued work on implementing the `AUTO_FLUSH_BACKGROUND` write
  mode for the C++ client. This feature makes it easier to implement high-throughput
  ingest using the C++ API by automatically handling the batching and flushing of writes
  based on a configurable buffer size.

  Alexey's [patch](https://gerrit.cloudera.org/#/c/3952/) has received several
  rounds of review and looks likely to be committed soon. Detailed performance testing
  will follow.

* Congratulations to Ram Mettu for committing his first patch to Kudu this week!
  Ram fixed a [bug in handling Alter Table with TIMESTAMP columns](
  https://issues.apache.org/jira/browse/KUDU-1522).

## Upcoming talks

* Mike Percy will be speaking about Kudu this Wednesday at the
  [Denver Cloudera User Group](http://www.meetup.com/Denver-Cloudera-User-Group/events/232782782/)
  and on Thursday at the
  [Boulder/Denver Big Data Meetup](http://www.meetup.com/Boulder-Denver-Big-Data/events/232056701/).
  If you're based in the Boulder/Denver area, be sure not to miss these talks!

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
