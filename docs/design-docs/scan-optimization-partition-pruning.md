# Scan Optimization & Partition Pruning

## Background

Kudu has a flexible partitioning design that allows rows to be distributed among
tablets through a combination of hash and range partitioning. The design allows
operators to have control over data locality in order to optimize for the
expected workload.

Currently, Kudu does not take full advantage of partition information when
executing scans. This results in missed opportunities to 'prune', or skip
tablets during a scan based on the scan's predicates and the tablet's hash
bucket and range assignments. This remainder of this design doc will detail the
specific opportunities we can take advantage of to prune partitions, provide an
overview of how we will accomplish this is the clients and on the server, and
provide some alternatives for discussion.

### Sample Schemas

The following sections will reference two example table schema:

```sql
CREATE TABLE 'machine_metrics'
(STRING host, STRING metric, TIMESTAMP time, DOUBLE value)
PRIMARY KEY (host, metric, time)
DISTRIBUTE BY
  HASH (host, metric) INTO 2 BUCKETS
  RANGE (time) SPLIT ROWS [(1451606400000)];
```

which the following tablets:

```
A: bucket(host, metric) = 0, range(time) = [(min), (1451606400000))
B: bucket(host, metric) = 0, range(time) = [(1451606400000), (max))
C: bucket(host, metric) = 1, range(time) = [(min), (1451606400000))
D: bucket(host, metric) = 1, range(time) = [(1451606400000), (max))
```

and

```sql
CREATE TABLE 'user_clicks'
(INT64 user_id, INT64 target_id, INT64 click_id)
PRIMARY KEY (user_id, target_id, click_id)
DISTRIBUTE BY RANGE (user_id, target_id) SPLIT ROWS [(1000, 1000)];
```

with the following tablets:

```
A: range(user_id) = [(min, min), (1000, 1000))
B: range(user_id) = [(1000, 1000), (max, max))
```

## Scan Constraints

Kudu has two mechanisms for limiting and filtering scan results: column
predicates and primary key bounds. The mechanisms can be used to express
different constraints on the scan, but there is some overlap in the constraints
they can represent.

Predicate and primary key bound constraints should be considered when evaluating
optimization and pruning opportunities, but for most pruning opportunities it is
easier to consider only one or the other. In order to simplify the pruning logic
and capture the most pruning opportunities, the Kudu client shares constraints
between the column predicates and primary key bounds when possible. This is done
by 'lifting' implicit predicates from the primary key bounds into the predicate
set, and 'pushing' predicate constraints into the primary key bounds.

For example, the following query:

```sql
SELECT * FROM 'user_clicks'
WHERE primary_key >= (500, 500)
  AND primary_key < (500, 750);
```

can lift the following constraints into the predicate set:

```sql
user_id = 500
target_id >= 500
target_id < 750
```

As an example of pushing predicates into the primary key bounds, the following
query:

```sql
SELECT * FROM 'user_clicks'
WHERE user_id = 500
  AND target_id < 700;
```

will result in the following primary key bounds:

```sql
primary_key >= (500, min)
primary_key  < (500, 700)
```

## Pruning Opportunities

### Range Pruning

If the table is range partitioned with split rows and the scan contains
predicates over a prefix subset of the range columns, then the scan may be able
to prune tablets based on those predicates. For example, the query:

```sql
SELECT * FROM 'machine_metrics'
WHERE timestamp < 500;
```

can prune tablets `B` and `D`.

### Primary Key Pruning

When a table is range partitioned on a prefix of the primary key columns (like
`user_clicks` but unlike `machine_metrics`), a special form of the Range Pruning
optimization becomes available. Instead of pruning based on the scan predicates,
the tablet's range bounds can be compared to the scan's upper and lower primary
key bounds. Since the upper and lower primary key bounds are always at least as
constrained as the predicates when the range columns are a primary key prefix,
the primary key bounds may provide additional pruning opportunities. For
example:

```sql
SELECT * FROM 'user_clicks'
WHERE primary_key >= (500, 0)
  AND primary_key  < (1000, 500);
```

Allows the following predicates to be lifted from the primary key range:

```sql
user_id >= 500
user_id  < 1001
```

The scan can be satisfied entirely by tablet `A`, but the lifted predicates are
unable to prune `B`. By using the primary key bounds, tablet `B` can be pruned.

### Hash Bucket Pruning

If a scan specifies equality predicates on all columns in a hash component, then
the scan may prune all tablets which do not fall in the corresponding bucket.
For example:

```sql
-- Allows hash bucket pruning
SELECT * from 'machine_metrics'
WHERE host = "host001.example.com"
  AND metric = "load-avg-1min";

-- Does not allow hash bucket pruning
SELECT * from 't'
WHERE host = "host001.example.com";
```

## Tablet Lookup Optimization

In order for the client to prune tablets, it must look up the tablet partition
information from the master. In order to limit these lookups for queries which
only touch a small portion of the table, predicates and primary key bounds are
pushed into partition key bounds in a manner similar to how predicates are
pushed into the primary key bounds. The partition key bounds limit the set of
tablets that are looked up from the master and evaluated for pruning. This can
limit the high upfront cost of looking up the entire tablet metadata set for a
new client which is performing a scan that is constrained to a few rows.

## Implementation

We will implement partition pruning in the client and on the server so that in
all cases the minimum amount of work must be done to satisify queries.
Duplicating the work on the server is not strictly necessary, but it is a
low-overhead operation in comparison with accessing disk, and it allows for
client implementations which don't implement the optimizations to benefit.

In the client, the scan's range, hash bucket, and partition key constraints will
be evaluated once per scan. As the scan progresses through the set of tablets in
the partition key range, each tablet's partition information will be compared
against the range bounds and hash buckets to determine whether pruning can
occur. Pruned tablets never need to be contacted (though their partition
information needs to be looked up from the master).

The server will go one step further by adding the tablet's primary key bounds to
the scan spec during scan initialization, which may provide additional pruning
opportunities in the case that the tablet's primary key bounds are more
constrained than the scan primary key bounds. The server will immediately return
an empty result if its partition can be pruned.

## Alternatives

### Prune based on tablet primary key bounds in the client

We could have clients add the tablet's upper and lower primary key bounds and
re-run the range bounds and hash bucket analysis for each tablet in the scan. I
propose that we do not do this, since in practice it will require copying
multiple data structures on the client for each tablet, and is not expected to
yield better pruning oppurtunities often.  On the server, it is a lighter weight
operation since the original scan predicate does not need to be copied (because
it can be mutated in place), and most of the optimization steps are already
happening anyway.

### Prune Tablets by Partition Key Interval Set

The method of pruning tablets described above is 'lazy' in that it retrieves
partition metadata for each tablet in the partition key range, and then compares
the partition against the precomputed hash bucket and range constraints to
determine if the tablet should be pruned.

As an alternative, the client could create a set of partition key ranges from
the hash bucket and range constraints. Taking the intersection of this interval
set with the interval set of tablet partition key ranges would yield exactly the
set of tablets necessary to satisfy the scan.
