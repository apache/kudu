<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Tablet history garbage collection

This document describes the removal of what we call "ancient history" from the
tablet. As described in the [Kudu Tablet design doc](tablet.md), Kudu stores
the history of changes over time in order to support scans at a particular
snapshot. Eventually, to reclaim disk space, we want to stop storing that
history.

## Ancient history mark

The ancient history mark is a HybridTime timestamp prior to which history is
considered "ancient". Ancient data is made unavailable and removed from disk.
The _ancient history mark_ is defined by a property called the "tablet history
max age". This property is configured with a _gflag_ called
`--tablet_history_default_max_age_sec`, which at the time of writing defaults
to 15 minutes. In the future, we may allow this to be specified on a per-table
basis.

## Consistency and read visibility

Attempts to open a snapshot scan prior to the ancient history mark will be
rejected. Since blocks are refcounted, scanners that have already opened the
blocks in a long-running scan will not be affected if garbage collection
unlinks an open block that has become ancient history.

Query engines that may perform many long-running scans at a single snapshot,
such as Apache Impala and Apache Spark, may end up too far in the past and error
out if a scan query runs for too long. We still need to come up with a way to
reliably prevent that. The current workaround is to tune the ancient history
mark high enough to avoid most such errors.

## Removing old delta history

In an update-heavy workload, many REDO records (and after compaction, many UNDO
records) tend to accumulate per row. Removing old UNDO records takes place
during the normal operation of several background maintenance tasks:

1. The CompactRowSetsOp tablet maintenance task.
   This task runs a merging compaction, which merges multiple rowsets into one
   rowset. See also "Merging compactions" below.
2. The MajorDeltaCompactionOp tablet maintenance task.
   This task chooses a DiskRowSet with a large number of REDO deltas and
   converts its REDO deltas to UNDO deltas. Any mutations older than the
   ancient history mark are dropped during this process.
3. The UndoDeltaBlockGCOp tablet maintenance task.
   This task deletes UNDO delta blocks that are older than the ancient history
   mark. It runs periodically and is fairly lightweight because it only deletes
   blocks. Therefore it does not cause write amplification (other than the I/O
   required to record the block deletion and rewrite the tablet superblock).

## Removing old deleted rows

### Merging compactions

When a merging compaction is run, two or more DiskRowSets are merged into one
and their constituent row ids are reassigned. During this process, if a row
marked as "deleted" has a deletion timestamp prior to the ancient history mark,
that row will be skipped when writing the new rowset. This process will
permanently remove all traces of the row and the space will be reclaimed from
disk.

### Row GC maintenance task

In cases where a merging tablet compaction is not run, we still want to remove
deleted rows. We could implement a maintenance task specifically for GCing
deleted rows. However, at the time of writing, this is not implemented because
it appears to be prone to causing write amplification.

UndoDeltaBlockGCOp, in contrast, only removes delta history associated with a
row. If a row is deleted, UndoDeltaBlockGCOp will not remove the row's base
data or the final REDO record indicating its deletion.
