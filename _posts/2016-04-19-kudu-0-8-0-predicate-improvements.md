---
layout: post
title: Predicate Improvements in Kudu 0.8
author: Dan Burkert
---

The recently released Kudu version 0.8 ships with a host of new improvements to
scan predicates. Performance and usability have been improved, especially for
tables taking advantage of [advanced partitioning
options](http://getkudu.io/docs/schema_design.html#data-distribution).

<!--more-->

## Scan Optimizations in the Server and C++ Client

The server and C++ client have gotten more sophisticated in how they handle and
optimize scan constraints. Constraints specified in the predicates and lower
and upper bound primary keys are better unified, resulting in more predicates
being pushed into primary key bounds, which can turn full table scans with
predicates into much more efficient bounded scans.

Additionally, the server and C++ client now recognize more opportunities to
prune entire tablets during scans. For example, for the following schema and
query Kudu will now be able to skip scanning 15 out of the 16 tablets in the
table:

{% highlight sql %}
-- create a table with 16 tablets
CREATE TABLE users (id INT64, name STRING, address STRING)
DISTRIBUTE BY HASH (id) INTO 16 BUCKETS;

-- scan over a single tablet
SELECT id, name, address FROM users
WHERE id = 876932;
{% endhighlight %}

For a deeper look at the newly implemented scan and partition pruning
optimizations, see the associated [design
document](https://github.com/apache/incubator-kudu/blob/master/docs/design-docs/scan-optimization-partition-pruning.md).
These optimizations will eventually be incorporated into the Java client as
well, but until that time they are still used on the server side for scans
initiated by Java clients. If you would like to help with this effort, let us
know on the [JIRA issue](https://issues.apache.org/jira/browse/KUDU-1065).

## Redesigned Predicate API in the Java Client

The Java client has a new way to express scan predicates: the
[`KuduPredicate`](http://getkudu.io/apidocs/org/kududb/client/KuduPredicate.html).
The API matches the corresponding C++ API more closely, and adds support for
specifying exclusive, as well as inclusive, range predicates. The existing
[`ColumnRangePredicate`](http://getkudu.io/apidocs/org/kududb/client/ColumnRangePredicate.html)
API has been deprecated, and will be removed soon. Example of transitioning from
the old to new API:

{% highlight java %}
ColumnSchema myIntColumnSchema = ...;
KuduScanner.KuduScannerBuilder scannerBuilder = ...;

// Old predicate API
ColumnRangePredicate predicate = new ColumnRangePredicate(myIntColumnSchema);
predicate.setLowerBound(20);
scannerBuilder.addColumnRangePredicate(predicate);

// New predicate API
scannerBuilder.newPredicate(
    KuduPredicate.newComparisonPredicate(myIntColumnSchema, ComparisonOp.GREATER_EQUAL, 20));
{% endhighlight %}

## Under the Covers Changes

The scan optimizations in the server and C++ client, and the new `KuduPredicate`
API in the Java client are made possible by an overhaul of how predicates are
handled internally. A new protobuf message type,
[`ColumnPredicatePB`](https://github.com/apache/incubator-kudu/blob/master/src/kudu/common/common.proto#L273)
has been introduced, and will allow more column predicate types to be introduced
in the future. If you are interested in contributing to Kudu but don't know
where to start, consider adding a new predicate type; for example the `IS NULL`,
`IS NOT NULL`, `IN`, and `LIKE` predicates types are currently not implemented.
