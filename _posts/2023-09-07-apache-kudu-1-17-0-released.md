---
layout: post
title: Apache Kudu 1.17.0 Released
author: Yingchun Lai
---

The Apache Kudu team is happy to announce the release of Kudu 1.17.0!

The new release adds several new features and improvements, including the
following:

<!--more-->

* Kudu now supports encrypting data at rest. Kudu supports `AES-128-CTR`, `AES-192-CTR`, and
  `AES-256-CTR` ciphers to encrypt data, supports Apache Ranger KMS and Apache Hadoop KMS. See
  [Data at rest](https://kudu.apache.org/docs/security.html#_data_at_rest) for more details.

* Kudu now supports range-specific hash schemas for tables. It's now possible to add ranges with
  their own unique hash schema independent of the table-wide hash schema. This can be done at table
  creation time and while altering the table. It’s controlled by the `--enable_per_range_hash_schemas`
  master flag which is enabled by default (see
  [KUDU-2671](https://issues.apache.org/jira/browse/KUDU-2671)).

* Kudu now supports soft-deleted tables. Kudu keeps a soft-deleted table aside for a period of time
  (a.k.a. reservation), not purging the data yet.  The table can be restored/recalled back before its
  reservation expires.  The reservation period can be customized via Kudu client API upon
  soft-deleting the table.  The default reservation period is controlled by the
  `--default_deleted_table_reserve_seconds` master's flag.
  NOTE: As of Kudu 1.17 release, the soft-delete functionality is not supported when HMS integration
  is enabled, but this should be addressed in a future release (see
  [KUDU-3326](https://issues.apache.org/jira/browse/KUDU-3326)).

* Introduced `Auto-Incrementing` column. An auto-incrementing column is populated on the server side
  with a monotonically increasing counter. The counter is local to every tablet, i.e. each tablet has
  a separate auto incrementing counter (see
  [KUDU-1945](https://issues.apache.org/jira/browse/KUDU-1945)).

* Kudu now supports experimental non-unique primary key. When a table with non-unique primary key is
  created, an `Auto-Incrementing` column named `auto_incrementing_id` is added automatically to the
  table as the key column. The non-unique key columns and the `Auto-Incrementing` column together form
  the effective primary key (see [KUDU-1945](https://issues.apache.org/jira/browse/KUDU-1945)).

* Introduced `Immutable` column. It's useful to represent a semantically constant entity (see
  [KUDU-3353](https://issues.apache.org/jira/browse/KUDU-3353)).

* An experimental feature is added to Kudu that allows it to automatically rebalance tablet leader
  replicas among tablet servers. The background task can be enabled by setting the
  `--auto_leader_rebalancing_enabled` flag on the Kudu masters. By default, the flag is set to 'false'
  (see [KUDU-3390](https://issues.apache.org/jira/browse/KUDU-3390)).

* Introduced an experimental feature: authentication of Kudu client applications to Kudu servers
  using JSON Web Tokens (JWT).  The JWT-based authentication can be used as an alternative to Kerberos
  authentication for Kudu applications running at edge nodes where configuring Kerberos might be
  cumbersome.  Similar to Kerberos credentials, a JWT is considered a primary client's credentials.
  The server-side capability of JWT-based authentication is controlled by the
  `--enable_jwt_token_auth` flag (set 'false' by default).  When the flat set to 'true', a Kudu server
  is capable of authenticating Kudu clients using the JWT provided by the client during RPC connection
  negotiation.  From its side, a Kudu client authenticates a Kudu server by verifying its TLS
  certificate.  For the latter to succeed, the client should use Kudu client API to add the cluster's
  IPKI CA certificate into the list of trusted certificates.

* The C++ client scan token builder can now create multiple tokens per tablet. So, it's now possible
  to dynamically scale the set of readers/scanners fetching data from a Kudu table in parallel. To use
  this functionality, use the newly introduced `SetSplitSizeBytes()` method of the Kudu client API to
  specify how many bytes of data each token should scan
  (see [KUDU-3393](https://issues.apache.org/jira/browse/KUDU-3393)).

* Kudu's default replica placement algorithm is now range and table aware to prevent hotspotting
  unlike the old power of two choices algorithm. New replicas from the same range are spread evenly
  across available tablet servers, the table the range belongs to is used as a tiebreaker (see
  [KUDU-3476](https://issues.apache.org/jira/browse/KUDU-3476)).

* Reduce the memory consumption if there are frequent alter schema operations for tablet servers
  (see [KUDU-3197](https://issues.apache.org/jira/browse/KUDU-3197)).

* Reduce the memory consumption by implementing memory budgeting for performing RowSet merge
  compactions (i.e. CompactRowSetsOp maintenance operations). Several flags have been introduced,
  while the `--rowset_compaction_memory_estimate_enabled` flag indicates whether to check for
  available memory necessary to run CompactRowSetsOp maintenance operations (see
  [KUDU-3406](https://issues.apache.org/jira/browse/KUDU-3406)).

The above is just a list of the highlights, for a more complete list of new
features, improvements and fixes please refer to the [release
notes](/releases/1.17.0/docs/release_notes.html).

The Apache Kudu project only publishes source code releases. To build Kudu
1.17.0, follow these steps:

- Download the Kudu [1.17.0 source release](/releases/1.17.0)
- Follow the instructions in the documentation to build Kudu [1.17.0 from
  source](/releases/1.17.0/docs/installation.html#build_from_source)

For your convenience, binary JAR files for the Kudu Java client library, Spark
DataSource, and other Java integrations are published to the ASF Maven
repository and are [now
available](https://search.maven.org/search?q=g:org.apache.kudu%20AND%20v:1.17.0).

The Python client source is also available on
[PyPI](https://pypi.org/project/kudu-python/).

Additionally, experimental Docker images are published to
[Docker Hub](https://hub.docker.com/r/apache/kudu).
