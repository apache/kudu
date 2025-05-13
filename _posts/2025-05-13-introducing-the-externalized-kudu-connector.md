---
layout: post
title:  "Introducing the Externalized Kudu Connector"
author: Ferenc Csaky
---

Note: This is a cross-post from the Apache Flink blog [Introducing the Externalized Kudu Connector](https://flink.apache.org/2025/04/30/introducing-the-externalized-kudu-connector/)

We are pleased to announce the revival of a connector that makes it possible for Flink to interact with [Apache Kudu](https://kudu.apache.org/).
The original connector existed as part of the [Apache Bahir](https://bahir.apache.org/#home) project, which was moved into the attic.
Despite this, we saw interest to keep the Kudu connector updated, hence the community agreed to externalize it as a standalone connector in accordance with the current connector development model.
For more information about the externalization process, see [FLIP-439](https://cwiki.apache.org/confluence/display/FLINK/FLIP-439%3A+Externalize+Kudu+Connector+from+Bahir).

<!--more-->

# Highlights

- The connector is built on the already existing Apache Bahir code.
- The existing DataStream connector is updated to Sink V2 API.
- New DataStream Source API connector implementation.
- The Table API source and sink connectors are now using the new Schema stack.
- The first released connector version is *2.0.0*, and it supports *Flink 1.19*, and *1.20*.

# DataStream Source API

The Source API implementation is a net new addition to the externalized connector, and it brings some interesting features.
Although Kudu itself is a bounded source, the Kudu Source implementation supports to configure boundedness, and can run in `CONTINUOUS_UNBOUNDED` mode.
In `CONTINUOUS_UNBOUNDED` mode, the source operates similarly to a Change Data Capture (CDC) system.
When the job starts, it takes a snapshot of the source table and records the snapshot timestamp.
After that, it performs periodic differential scans, capturing only the changes that occurred since the last scan.
The frequency of these scans is determined by the `.setDiscoveryPeriod(Duration)` setting.
The following example demonstrates how to stream data from a Kudu table, capturing updates every one minute.

```java
KuduSource<Row> source =
        KuduSource.<Row>builder()
                .setTableInfo(...)
                .setReaderConfig(...)
                .setRowResultConverter(new RowResultRowConverter())
                .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                .setDiscoveryPeriod(Duration.ofMinutes(1))
                .build();
```

For more details and examples, see the [DataStream connector documentation](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/datastream/kudu/)

# Table API Catalog

The connector includes a catalog implementation designed to manage metadata for your Kudu setup and facilitate table operations.
With the Kudu catalog, you can access all existing Kudu tables directly through Flink SQL queries.
Such catalog can be defined in Flink SQL, as part of the Java application, or via a YAML catalog descriptor as well.
The below example shows a minimal example in Filnk SQL.

```sql
CREATE CATALOG my_kudu_catalog WITH(
    'type' = 'kudu',
    'masters' = 'localhost:7051',
    'default-database' = 'default_database'
);

USE CATALOG my_kudu_catalog;
```

For other Table API related topics and examples, see the [Table API connector documentation](https://nightlies.apache.org/flink/flink-docs-lts/docs/connectors/table/kudu/)

# Release Notes

## Sub-task
* [[FLINK-34929](https://issues.apache.org/jira/browse/FLINK-34929)] - Create "flink-connector-kudu" repository
* [[FLINK-34930](https://issues.apache.org/jira/browse/FLINK-34930)] - Move existing Kudu connector code from Bahir repo to dedicated repo
* [[FLINK-34931](https://issues.apache.org/jira/browse/FLINK-34931)] - Update Kudu DataStream connector to use Sink V2
* [[FLINK-35114](https://issues.apache.org/jira/browse/FLINK-35114)] - Remove old Table API implementations, update Schema stack
* [[FLINK-35350](https://issues.apache.org/jira/browse/FLINK-35350)] - Add documentation for Kudu connector
* [[FLINK-37389](https://issues.apache.org/jira/browse/FLINK-37389)] - Add "flink-sql-connector-kudu" module

## New Feature
* [[FLINK-36855](https://issues.apache.org/jira/browse/FLINK-36855)] - Implement Source API in Kudu connector
* [[FLINK-37527](https://issues.apache.org/jira/browse/FLINK-37527)] - Add `KuduSource` documentation
* [[FLINK-37664](https://issues.apache.org/jira/browse/FLINK-37664)] - Integrate Kudu connector docs

## Improvement
* [[FLINK-36839](https://issues.apache.org/jira/browse/FLINK-36839)] - Update Kudu version to 1.17.1
* [[FLINK-37190](https://issues.apache.org/jira/browse/FLINK-37190)] - Make Kudu `FlushMode` configurable in Flink SQL
* [[FLINK-37230](https://issues.apache.org/jira/browse/FLINK-37230)] - Consolidate Kudu connector table options
* [[FLINK-37237](https://issues.apache.org/jira/browse/FLINK-37237)] - Improve Kudu table creation based on Flink SQL `CREATE TABLE`

# List of Contributors

Ferenc Csaky, Martijn Visser, Marton Greber

# Resources

Connector GitHub repository: [https://github.com/apache/flink-connector-kudu](https://github.com/apache/flink-connector-kudu)

Maven Central link: [https://central.sonatype.com/artifact/org.apache.flink/flink-connector-kudu](https://central.sonatype.com/artifact/org.apache.flink/flink-connector-kudu)
