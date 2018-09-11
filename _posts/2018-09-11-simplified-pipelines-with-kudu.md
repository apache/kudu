---
layout: post
title: Simplified Data Pipelines with Kudu
author: Mac Noland
---

I’ve been working with Hadoop now for over seven years and fortunately, or unfortunately, have run
across a lot of structured data use cases.  What we, at [phData](https://phdata.io/), have found is
that end users are typically comfortable with tabular data and prefer to access their data in a
structured manner using tables.
<!--more-->

When working on new structured data projects, the first question we always get from non-Hadoop
followers is, _“how do I update or delete a record?”_  The second question we get is, _“when adding
records, why don’t they show up in Impala right away?”_  For those of us who have worked with HDFS
and Impala on HDFS for years, these are simple questions to answer, but hard ones to explain.

The pre-Kudu years were filled with 100’s (or 1000’s) of self-join views (or materialization jobs)
and compaction jobs, along with scheduled jobs to refresh Impala cache periodically so new records
show up.  And while doable, for 10,000’s of tables, this basically became a distraction from solving
real business problems.

With the introduction of Kudu, mixing record level updates, deletes, and inserts, while supporting
large scans, are now something we can sustainably manage at scale.  HBase is very good at record
level updates, deletes and inserts, but doesn’t scale well for analytic use cases that often do full
table scans. Moreover, for streaming use cases, changes are available in near real-time.  End users,
accustomed to having to _”wait”_ for their data, can now consume the data as it arrives in their
table.

A common data ingest pattern where Kudu becomes necessary is change data capture (CDC).  That is,
capturing the inserts, updates, hard deletes, and streaming them into Kudu where they can be applied
immediately.  Pre-Kudu this pipeline was very tedious to implement.  Now with tools like
[StreamSets](https://streamsets.com/), you can get up and running in a few hours.

A second common workflow is near real-time analytics.  We’ve streamed data off mining trucks,
oil wells, manufacturing lines, and needed to make that data available to end users immediately.  No
longer do we need to batch up writes, flush to HDFS and then refresh cache in Impala.  As mentioned
before, with Kudu, the data are available as soon as it lands.  This has been a significant 
enhancement for end users, who previously had to _”wait”_ for data.

In summary, Kudu has made a tremendous impact in removing the operational distractions of merging in
changes, and refreshing the cache of downstream consumers.  This now allows data engineers
and users to focus on solving business problems, rather than being bothered by the tediousness of
the backend.
