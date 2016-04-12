# Non-covering Range Partitions

## Background & Motivation

This document assumes advanced knowledge of Kudu partitioning, see the
[schema design guide](http://getkudu.io/docs/schema_design.html) and the
[partition pruning design doc](scan-optimization-partition-pruning.md) for more
background.

Currently, Kudu tables create a set of tablets during creation according to the
partition schema of the table. The partition schema can specify range
partitioning with split rows, hash partitioning with a set number of buckets, or
a combination of both. Once the table is created, the set of tablets is fixed,
and individual tablets may not be added or dropped.

Range partitioned tables are required to have tablets which cover the entire
range of possible keys. This is achieved by specifying the split points between
partitions; it is impossible to supply an upper or lower bound, or create a
table with gaps in the partition space.

The limitation that tablets are fixed upon table creation and that the entire
range must be covered by tablets presents difficulties for two important
use-cases.

First, for partitioning by a factor which continuously increases, such as time,
range partitioning is currently infeasible. It is impossible to create enough
partitions upfront to serve future needs, and if it were, the partitions would
mostly sit idle, wasting resources. The ability to add partitions on demand is
necessary for this use-case. Additionally, dropping range partitions enables
'moving window' tables which add new partitions and drop old partitions as
necessary to hold the latest interval of data.

Second, Kudu's capability for partitioning a table on categorical data such as
sales region or product line is quite limited. With support for non-covering
range partitions, a separate range partition can be created per categorical
value. New categories can be added and old categories removed by adding or
removing the corresponding range partition. This feature is often called `LIST`
partitioning in other analytic databases.

This document proposes adding non-covering range partitions to Kudu, as well as
the ability to add and drop range partitions. The proposal only extends the
capability of range partitioning; hash partitioning semantics will not change.

It is worth noting the effect of the new range partitioning features on a table
with a combination of hash and range partitioning. When adding a range
partition, a new tablet will be created for every hash bucket value. In the case
of a table with multiple hash components, the number of new tablets will be the
product of the hash bucket counts. Each tablet will be assigned a unique
combination of the new range partition and a hash bucket per hash component.
Similarly, when dropping a range partition on a hash partitioned table, all
tablets for that range across all hash buckets will be dropped.

## Step 1: Create Table with Non-covering Range Partitions

The Kudu client table creator will add an option for adding a bounded partition
range to the table options. Zero or more non-overlapping range bounds may be
provided. If zero range bounds are provided, then the table defaults to no range
bounds, just as it does currently. When one or more range bounds are provided,
tablets will only be created for range values inside the bounds. Range bounds
will be specified as a pair of partial rows, the first being an inclusive lower
bound, and the second being an exclusive upper bound. Either row may be null, in
which case that end of the range is unbounded. Split rows are allowed to be
specified, but they must fall into one of the specified range bounds, and will
serve to split the bound into separate ranges.

The new range bound option is sufficiently flexible to express any partitioning
strategy achievable with split rows, but split rows will remain in the API to
retain backwards compatibility. Split rows could be forbidden when range bounds
are also specified, but schema designers may find it useful to be able to use
both options. For example, a pseudo-SQL statement for creating a table holding
sales data for years 2011 through 2015, with a partition per year:

```sql
CREATE TABLE sales_by_year (year INT32, sale_id INT32, amount INT32)
PRIMARY KEY (sale_id, year)
DISTRIBUTE BY RANGE (year)
              RANGE BOUND ((2011), (2016))
              SPLIT ROWS ((2012), (2013), (2014), (2015));
```

An example categorical table:

```sql
CREATE TABLE sales_by_region (region STRING, sale_id INT32, amount INT32)
PRIMARY KEY (sale_id, region)
DISTRIBUTE BY RANGE (region)
              RANGE BOUND (("North America"), ("North America\0")),
              RANGE BOUND (("Europe"), ("Europe\0")),
              RANGE BOUND (("Asia"), ("Asia\0"));
```

### Writes

Adding support for non-covering range partitions will create a new failure
scenario in the client, in which the application attempts to write a row in a
non-existent partition. In this scenario the client should use the existing row
errors mechanism to report the error (which may not be available until
flushing). A new error status type of `TabletNotFound` will be used.

### Scans

Non-existent range partitions will be ignored by the client during scans. For
example, if the client attempts to scan the `sales_by_region` table above, the
scanner will skip over the range partition gaps between the range bounds. If the
client limits the scan to a non-existent range partition through either
predicates or primary key bounds, then no results will be returned.

### Handling Non-existence of Range Partitions

The Kudu client learns of tablet locations by sending a
`GetTableLocationsRequestPB` to the Kudu master. The message includes an
optional partition key range to limit the result set. The master can only return
tablet locations for tablets which exist, so the response should not include
results for non-covered range partitions.

Kudu clients internally use a meta cache to keep a view of the tablet servers
and tablets in the cluster. The internal client meta cache should handle
receiving tablet location results from masters which include gaps in the range
partitions. The existing lookup functionality (e.g.
`MetaCache::LookupTabletByKey`) should return a `TabletNotFound` status when a
lookup on a non-covering partition key range is attempted. A new method for
looking up the next tablet at or after a partition key (e.g.
`MetaCache::LookupTabletByKeyOrNext`) should be added for scans which only need
to find the next tablet.

If the client table locations lookup request specifies a partition key which
falls in a non-covered range partition, then the master should include the
tablet directly preceding the non-covered range, if it exists. This will enable
the client to get a better view of what tablets exist.

## Step 2: Add Range Partition

To support adding range partitions to existing tables, a new alter table step
type `AlterTableRequestPB::ADD_RANGE_PARTITION` will be added. This RPC will
cause the master to create new tablets for the range partition in its persisted
metadata and contact tablet servers in order to create the necessary tablet
replicas.

## Step 3: Drop Range Partition

Removing range partitions will require a new alter table step type,
`AlterTableRequestPB::DROP_RANGE_PARTITION`. This RPC will act as the mirror to
the add range partition alter table command.

Dropping tablets requires that clients invalidate stale meta cache entries. The
existing cache invalidation mechanism used when tablets are moved can be reused
for this purpose. When an operation attempts to contact a no-longer existent
tablet at a cached location, the tablet server will return a
`TabletServerErrorPB::TABLET_NOT_FOUND` error, which causes the meta cache to
evict its entry for the tablet. Subsequently, the meta cache will request the
new location from the master, and the response will contain no tablet for the
partition key range.

### Transactional Considerations when Dropping Range Partitions

The drop range partition mechanism proposed above works outside of Kudu's
hybrid-time based MVCC transactions. Consequently, when a scan is executing
concurrently with a drop range partition command, the scan's results may not be
repeatable or coherent. For example, if dropping the range partition results in
dropping many tablets (due to hash partitioning), the scanner may read results
from some, but not all of the tablets. The scanner may even be part way through
scanning a tablet as it is dropped. Setting a snapshot timestamp has no effect
on this kind of inconsistency. When using a query execution system such as
Impala or Spark which pre-plan scans and may arbitrarily retry scan tasks,
retried tasks may see different results even when a snapshot timestamp has been
specified. Recognizing a range partition being dropped while scanning may be
possible in a single client, but it is not possible when a higher level
execution engine is retrying scans in different contexts. It is not clear what
should happen in the single client context when scans encounter concurrently
dropped range partitions.

## Alternatives & Future Work

### Client-side Caching of Range Partition Non-existence

The proposed changes to the client meta cache are a straightforward extension of
the existing meta cache functionality, but they expose an edge case around
handling non-existent range partitionss. In particular, every operation on a
non-existent range partition will cause the meta cache to request tablet
location information from the master. This includes easy to avoid cases such as
the application continually writing to non-existent range partitions, but it
also includes ordinary scans that are not limited to a contiguous subset of the
range partitions.

To lessen the performance impact to the client and avoid swamping the master
with unnecessary tablet location requests, the meta cache could be augmented to
remember the non-existence of tablets for a configurable timeout, say 60
seconds. This would cause clients to skip new range partitions during scans and
fail to write to new range partitions for up to 60 seconds after they are
created, but it would significantly reduce the number of tablet lookups for
non-existent range partitions.

### Transactional Range Partition Add and Drop

Adding and dropping range partitions can be made transactional by assigning
timestamps to add and drop events, and using the snapshot timestamp of
operations to decide what tablets to expose. This would make adding and dropping
range partitions similar to the existing row update mechanism in Kudu, including
delaying cleaning up deleted tablets until after a TTL period. Master tablet
location responses would be extended to include the current and past states of
the tablet (live or deleted), and the associated timestamp.

### Add Range Partitions On Demand

Instead of requiring users to specify range partitions bounds and splits upfront
during table creation, Kudu could instead create range partitions on demand as
rows are inserted. The user would specify a range partition interval width, and
every write into a non-existent range partition interval would cause a range
partition to be added.

### Default Tablet

Kudu could have an option to create tables with non-covering range partitions
with a default tablet. This tablet will absorb any writes which would otherwise
fail with a `TabletNotFound` error. This feature would not be compatible with
the 'Add Range Partitions On Demand' feature.
