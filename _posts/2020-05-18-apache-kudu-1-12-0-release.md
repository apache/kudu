---
layout: post
title: Apache Kudu 1.12.0 released
author: Hao Hao
---

The Apache Kudu team is happy to announce the release of Kudu 1.12.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

- Kudu now supports native fine-grained authorization via integration with
  Apache Ranger. Kudu may now enforce access control policies defined for
  Kudu tables and columns stored in Ranger. See the
  [authorization documentation](/releases/1.12.0/docs/security.html#fine_grained_authz)
  for more details.
- Kudu’s web UI now supports proxying via Apache Knox. Kudu may be deployed
  in a firewalled state behind a Knox Gateway which will forward HTTP requests
  and responses between clients and the Kudu web UI.
- Kudu’s web UI now supports HTTP keep-alive. Operations that access multiple
  URLs will now reuse a single HTTP connection, improving their performance.
- The `kudu tserver quiesce` tool is added to quiesce tablet servers. While a
  tablet server is quiescing, it will stop hosting tablet leaders and stop
  serving new scan requests. This can be used to orchestrate a rolling restart
  without stopping on-going Kudu workloads.
- Introduced `auto` time source for HybridClock timestamps. With
  `--time_source=auto` in AWS and GCE cloud environments, Kudu masters and
  tablet servers use the built-in NTP client synchronized with dedicated NTP
  servers available via host-only networks. With `--time_source=auto` in
  environments other than AWS/GCE, Kudu masters and tablet servers rely on
  their local machine's clock synchronized by NTP. The default setting for
  the HybridClock time source (`--time_source=system`) is backward-compatible,
  requiring the local machine's clock to be synchronized by the kernel's NTP
  discipline.
- The `kudu cluster rebalance` tool now supports moving replicas away from
  specific tablet servers by supplying the `--ignored_tservers` and
  `--move_replicas_from_ignored_tservers` arguments (see
  [KUDU-2914](https://issues.apache.org/jira/browse/KUDU-2914) for more
  details).
- Write Ahead Log file segments and index chunks are now managed by Kudu’s file
  cache. With that, all long-lived file descriptors used by Kudu are managed by
  the file cache, and there’s no longer a need for capacity planning of file
  descriptor usage.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.12.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.12.0, follow these steps:

- Download the Kudu [1.12.0 source release](/releases/1.12.0)
- Follow the instructions in the documentation to build Kudu [1.12.0 from
  source](/releases/1.12.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.12.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
