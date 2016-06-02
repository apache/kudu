---
layout: post
title: "Default Partitioning Changes Coming in Kudu 0.9"
author: Dan Burkert
---

The upcoming Apache Kudu (incubating) 0.9 release is changing the default
partitioning configuration for new tables. This post will introduce the change,
explain the motivations, and show examples of how code can be updated to work
with the new release.

<!--more-->

The most common source of frustration with new Kudu users is the default
partitioning behavior when creating new tables. If partitioning is not
specified, the Kudu client prior to 0.9 creates tables with a _single tablet_.
Single tablet tables are a Kudu anti-pattern, since they are unable to get the
scalability benefit of distributing data across the cluster, and instead keep
all data on a single machine.

Unfortunately, automatically choosing a better default partitioning
configuration for new tables is not simple. In most cases, hash partitioning on
the primary key is a better default, but this approach can have its own
drawbacks. In particular, it is not clear how many buckets should be used for
the new table.

Since there is no bullet-proof default and changing the partitioning
configuration after table creation is impossible, [we
decided](https://lists.apache.org/thread.html/ca8972620839109334493424a1022fc08c77c315d9d623f5caaa815f@1463699013@%3Cuser.kudu.apache.org%3E)
to remove the default altogether. Removing the default is a backwards
incompatible change, so it must be done before the 1.0 release. If we later find
a better way to create a default partitioning configuration, it should be
possible to adopt it in a backwards compatible way. The result of removing the
default is that new tables created with the 0.9 client must specify a
partitioning configuration, or table creation will fail. You can still create a
table with a single tablet, but it must be configured explicitly. These changes
only affect new table creation; existing tables, including tables created with
default partitioning before the 0.9 release, will continue to work.

In most cases updating existing code to explicitly set a partitioning
configuration should be simple. The examples below add hash partitioning, but
you can also specify range partitioning or a combination of range and hash
partitioning. See the [schema design
guide](http://getkudu.io/docs/schema_design.html#data-distribution) for more
advanced configurations.

C++ Client
==========

With the C++ client, creating a new table with hash partitions is as simple as
calling `KuduTableCreator:add_hash_partitions` with the columns to hash and the
number of buckets to use:

```cpp
unique_ptr<KuduTableCreator> table_creator(my_client->NewTableCreator());
Status create_status = table_creator->table_name("my-table")
                                     .schema(my_schema)
                                     .add_hash_partitions({ "key_column_a", "key_column_b" }, 16)
                                     .Create();
if (!create_status.ok() { /* handle error */ }
```

Java Client
===========

And similarly, in Java:

```java
List<String> hashColumns = new ArrayList<>();
hashColumns.add("key_column_a");
hashColumn.add("key_column_b");
CreateTableOptions options = new CreateTableOptions().addHashPartitions(hashColumns, 16);
myClient.createTable("my-table", my_schema, options);
```

In the examples above, if the hash partition configuration is omitted the create
table operation will fail with the error `Table partitioning must be specified
using setRangePartitionColumns or addHashPartitions`. In the Java client this
manifests as a thrown `IllegalArgumentException`, while in the C++ client it is
returned as a `Status::InvalidArgument`.

Impala
======

When creating Kudu tables with Impala, the formerly optional `DISTRIBUTE BY`
clause is now required:

```SQL
CREATE TABLE my_table (key_column_a STRING, key_column_b STRING, other_column STRING)
DISTRIBUTE BY HASH (key_column_a, key_column_b) INTO 16 BUCKETS
TBLPROPERTIES(
    'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
    'kudu.table_name' = 'my_table',
    'kudu.master_addresses' = 'kudu-master.example.com:7051',
    'kudu.key_columns' = 'key_column_a,key_column_b'
);
```
