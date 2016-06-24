---
layout: post
title: Apache Kudu (incubating) Weekly Update June 21, 2016
author: Jean-Daniel Cryans
---
Welcome to the fourteenth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

## Development discussions and code in progress

* Dan Burkert posted a series of patches to [add support in the Java client](https://gerrit.cloudera.org/#/c/3388/)
  for non-covering range partitions. At the same time he improved how that client locates tables by
  leveraging the tablets cache.

* In the context of making multi-master reliable in 1.0, Adar Dembo posted a [design document](https://gerrit.cloudera.org/#/c/3393/)
  on how to handle permanent master failures. Currently the master's code is missing some features
  like `remote bootstrap` which makes it possible for a new replica to download a snapshot of the data
  from the leader replica.

* Tsuyoshi Ozawa refreshed [a patch](https://gerrit.cloudera.org/#/c/2162/) posted in February that
  makes it easier to get started contributing to Kudu by providing a Dockerfile with the right
  environment.

## On the blog

* Mike Percy [wrote](http://getkudu.io/2016/06/17/raft-consensus-single-node.html) about how Kudu
  uses Raft consensus on a single node, and some changes we're making as Kudu is getting more mature.

Want to learn more about a specific topic from this blog post? Shoot an email to the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweet at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.
