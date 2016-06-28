---
layout: post
title: Apache Kudu (incubating) Weekly Update April 11, 2016
author: Todd Lipcon
---
Welcome to the fourth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Project news

* The first release candidate for the 0.8.0 release was voted upon last week by
  the Kudu project members as well as the Apache Incubator PMC. The vote passed
  on the first try, so this release candidate will is now the official 0.8.0
  release.

  The [release notes](http://kudu.apache.org/releases/0.8.0/docs/release_notes.html)
  summarize the new features, improvements, and important bug fixes. The
  release itself and its documentation can be found at the
  [Kudu 0.8.0 release](http://kudu.apache.org/releases/0.8.0/) page.

  Thank you to the 11 developers who contributed the 172 bug fixes and
  improvements for this release!

* Congratulations to Binglin Chang who was elected as a new committer and
  Podling Project Management Committee (PPMC) member this past week.
  Binglin has contributed many bug reports and fixes and new features,
  and has also given several presentations about Kudu. He is also the
  first community member to run a Kudu cluster in a real production workload.
  Thanks for your contributions, Binglin!

## Development discussions and code in progress

* Darren Hoo reported an issue on the mailing list where his Kudu daemons were
  occasionally crashing in a workload involving large string cell values on the
  order of tens of KB. Todd Lipcon investigated the issue and provided a [bug
  fix](http://gerrit.cloudera.org:8080/2725).

  It turns out that this bug has been present since the third week of Kudu
  development in 2012. Given that the bug persisted so long, yet it was
  relatively easy to reproduce, it highlights that Kudu needs further
  testing for use cases with larger cell values. In particular, large
  primary keys or updates containing large cells would trigger this problem.

* Activity picked back up on the [design document for the Replay Cache feature](http://gerrit.cloudera.org:8080/#/c/2642/).
  This feature's goal is to allow more transparent failovers between Kudu
  servers and allow operations to be retried without affecting the semantics
  seen by clients.

  As an example, consider the case when an 'INSERT' operation of a single
  row times out. Upon receiving a timeout response, the client does not know
  whether the time out occurred before the insert was processed, or after.
  The client may then retry the operation, perhaps on a different replica.
  If the original operation was in fact successfully applied before the
  timeout occurred, the client might receive an erroneous "row already exists"
  error.

  The "replay cache" feature, currently being designed by David Alves, is
  meant to avoid this issue: each server will keep a relatively small cache
  of recently applied operations so that, if a retry arrives, it can
  correctly respond with the original result rather than applying it a second
  time.

* Binglin Chang reported an interesting bug where [two of three replicas were
  online, but neither one was being elected as leader](https://issues.apache.org/jira/browse/KUDU-1391).
  The issue occurred on his 70-node cluster after accidentally shutting down
  about half of the servers for ten minutes. Upon recovery, one out of the
  thousands of tablets got stuck in this situation. Mike Percy and Todd Lipcon
  have been discussing the issue on the JIRA and seem to be converging on
  some action items to prevent it from reoccurring.

* Benjamin Kim revived an old thread about running [Spark on Kudu](http://markmail.org/thread/mznrulrh3o4625ei).
  Users on the thread are piping up to write about their Spark use cases.
  It seems likely that, given this interest, some Spark-on-Kudu improvements
  will be happening in the near term. If you are interested in contributing,
  please pipe up on the mailing list.
