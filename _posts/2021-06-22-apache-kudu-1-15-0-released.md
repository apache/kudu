---
layout: post
title: Apache Kudu 1.15.0 Released
author: Bankim Bhavsar
---

The Apache Kudu team is happy to announce the release of Kudu 1.15.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

* Kudu now experimentally supports multi-row transactions. Currently only `INSERT` and
  `INSERT_IGNORE` operations are supported.
  See [here](https://github.com/apache/kudu/blob/master/docs/design-docs/transactions.adoc) for a
  design overview of this feature.

* Kudu now supports Raft configuration change for Kudu masters and CLI tools for orchestrating
  addition and removal of masters in a Kudu cluster. These tools substantially simplify the process
  of migrating to multiple masters, recovering a dead master and removing masters from a Kudu
  cluster. For detailed steps, see the latest administration documentation. This feature is evolving
  and the steps to add, remove and recover masters may change in the future.
  See [KUDU-2181](https://issues.apache.org/jira/browse/KUDU-2181) for details.

* Kudu now supports table comments directly on Kudu tables which are automatically synchronized
  when the Hive Metastore integration is enabled. These comments can be added at table creation time
  and changed via table alteration.

* Kudu now experimentally supports per-table size limits based on leader disk space usage or number
  of rows. When generating new authorization tokens, Masters will now consider the size limits and
  strip tokens of `INSERT` and `UPDATE` privileges if either limit is reached. To enable this
  feature, set the `--enable_table_write_limit` master flag; adjust the `--table_disk_size_limit`
  and `--table_row_count_limit` flags as desired or use the `kudu table set_limit` tool to set
  limits per table.

* It is now possible to change the Kerberos Service Principal Name using the `--principal` flag. The
  default SPN is still `kudu/_HOST`. Clients connecting to a cluster using a non-default SPN must
  set the `sasl_protocol_name` or `saslProtocolName` to match the SPN base
  (i.e. “kudu” if the SPN is “kudu/_HOST”) in the client builder or the Kudu CLI.
  See [KUDU-1884](https://issues.apache.org/jira/browse/KUDU-1884) for details.

* Kudu RPC now supports TLSv1.3.  Kudu servers and clients automatically negotiate TLSv1.3 for Kudu
  RPC if OpenSSL (or Java runtime correspondingly) on each side supports TLSv1.3.
  If necessary, use the newly introduced flag `--rpc_tls_ciphersuites` to customize TLSv1.3-specific
  cipher suites at the server side.
  See [KUDU-2871](https://issues.apache.org/jira/browse/KUDU-2871) for details.

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.15.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.15.0, follow these steps:

- Download the Kudu [1.15.0 source release](/releases/1.15.0)
- Follow the instructions in the documentation to build Kudu [1.15.0 from
  source](/releases/1.15.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, Flume sink, and other Java integrations are published to the ASF
Maven repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.15.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu), including for AArch64-based
architectures (ARM).
