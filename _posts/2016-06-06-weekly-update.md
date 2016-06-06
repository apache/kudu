---
layout: post
title: Apache Kudu (incubating) Weekly Update June 6, 2016
author: Jean-Daniel Cryans
---
Welcome to the twelfth edition of the Kudu Weekly Update. This weekly blog post
covers ongoing development and news in the Apache Kudu (incubating) project.

<!--more-->

If you find this post useful, please let us know by emailing the
[kudu-user mailing list](mailto:user@kudu.incubator.apache.org) or
tweeting at [@ApacheKudu](https://twitter.com/ApacheKudu). Similarly, if you're
aware of some Kudu news we missed, let us know so we can cover it in
a future post.

## Development discussions and code in progress

* Jean-Daniel Cryans, put up [0.9.0 RC1](http://mail-archives.apache.org/mod_mbox/incubator-kudu-dev/201606.mbox/%3CCAGpTDNduoQM0ktuZc1eW1XeXCcXhvPGftJ%3DLRB8Er5c2dZptvw%40mail.gmail.com%3E)
  for vote on the dev mailing list and it passed. The Incubator PMC (IPMC) will also need
  to vote on it before it can officially be released.

* Mike Percy is working on removing LocalConsensus which is currently used for
  single node Kudu deployments. We will instead use the Raft consensus implementation
  with a replication factor of 1. This is to simplify development since we need to maintain two
  consensus implementations. It will also provide a way to migrate from single node to multi-node
  deployments. See the discussion in this [dev thread](http://mail-archives.apache.org/mod_mbox/kudu-dev/201605.mbox/%3CCADXBggeE6RUYchv5fa=J2geHGE8Mw4SOeoi=LjXjdfmYYSqyhQ@mail.gmail.com%3E).

* Zhen Zhang got a patch in for [KUDU-1444](https://issues.apache.org/jira/browse/KUDU-1444)
  that adds resources usage monitoring to scanners in the C++ client. In the future this could
  be leveraged by systems like Impala to augment the query profiles.

* Longer term efforts for 1.0 are making good progress. Dan Burkert [added support](https://gerrit.cloudera.org/#/c/3255/)
  in the C++ client for non-covering range partitioned tables, and David Alves has a few
  patches in for the [Replay Cache](https://gerrit.cloudera.org/#/c/2642/).
