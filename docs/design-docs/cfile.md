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

# CFile

CFile is an on-disk columnar storage format which holds data and associated
B-Tree indexes. Within a [DiskRowSet](tablet.md) each column and DeltaFile will
map to a single CFile. In addition, the DiskRowSet's bloomfilter will be stored
in a CFile, and if the table has a compound primary key, then the associated
ad-hoc index will be stored in a separate CFile.

Despite the name, a CFile does not necessarily correspond one-to-one with a file
in the filesystem. The mapping of CFiles to the filesystem is determined by the
[BlockManager](../../src/kudu/fs/block_manager.h) abstraction. A CFile is
written to a single BlockManager Block (not to be confused with cblocks, which
are internal to CFiles, discussed below).

A CFile is composed of a header, a variable number of cblocks, and a footer.
There are a few different types of cblocks, each with a different format: data
cblocks, nullable data cblocks, index cblocks, and dictionary cblocks.

Data cblocks consist of a sequence of consecutive values. Each value is assigned
an ordinal index, or offset, which is unique within the CFile. Thus, a cblock
can be identified within a CFile by the range of ordinal indexes it contains;
for instance, the first three cblocks of a CFile might have the ordinal index
ranges `[0, 55]`, `[56, 132]`, and `[133, 511]`.

In order to support efficient seeks to an arbitrary ordinal index within the
CFile, a positional index may be included which contains a mapping of starting
ordinal index to data cblock. See [CFile Index](#cfile-index) for more details.

For CFiles which contain data in sorted order (for example, a CFile containing
the Primary Key column), a value index may be created. The value index supports
efficiently seeking to an arbitrary value in the CFile. See
[CFile Index](#cfile-index) for more details.

## File Format

The CFile header, cblocks, and footer are written consecutively to the file
without padding.

### Header Layout

| field | width (bytes) | notes |
| --- | --- | --- |
| magic | 8 | [see below](#versioning) |
| length | 4 | 32-bit unsigned length of the following Protobuf message |
| message | variable | encoded [`CFileHeaderPB`][cfile.proto] Protobuf message |
| checksum | 4 | optional CRC-32 checksum of magic, length, and message |

### Footer Layout

| field | width (bytes) | notes |
| --- | --- | --- |
| checksum | 4 | optional CRC-32 checksum of message, magic, and length |
| magic | 8 | [see below](#versioning) |
| message | variable | encoded [`CFileFooterPB`][cfile.proto] Protobuf message |
| length | 4 | unsigned 32-bit length of the preceding Protobuf message |

### Versioning

The CFile header and footer each contain a 'magic' string which indicates the
CFile's version.

| version | string |
| --- | --- |
| CFile v1 | `kuducfil` |
| CFile v2 | `kuducfl2` |

CFile v2 was introduced in Kudu 1.3 in order to make a breaking change in the
CFile format. At the same time, two sets of feature flags were introduced to the
footer in order to make more granular forwards compatibility possible in the
future. See commit [`f82cf6918c`] for details.

### Data cblock Layout

| field | width (bytes) | notes |
| --- | --- | --- |
| data | variable | [encoded](#data-encodings) data values |
| checksum | 4 | optional CRC-32 checksum of the data |

If the CFile is configured to use compression then the encoded data is
compressed before the checksum is appended.

### Nullable Data cblock Layout

Columns marked as nullable in the schema are stored in a nullable cblock, which
includes a bitmap which tracks which rows (ordinal offsets) are null and not
null.

| field | width (bytes) | notes |
| --- | --- | --- |
| value count | variable | [LEB128] encoded count of values |
| bitmap length | variable | [LEB128] encoded length of the following null bitmap |
| null bitmap | variable | [RLE] encoded bitmap |
| data | variable | encoded non-null data values |
| checksum | 4 | optional CRC-32 checksum of the value count, bitmap length, null bitmap, and data |

If the CFile is configured to use compression then the value count, bitmap
length, null bitmap, and encoded data are compressed before the checksum is
appended.

### Checksums

Checksums can be optionally written and verified.

When checksums for the header, data, and footer are written in the CFile, the
`incompatible_features` bitset in the CFileFooterPB message is used. A
"checksum" bit is set to ensure the reader knows if checksums exist.

When reading a CFile the footer should be read first to find if the file
contains checksums. If the `incompatible_features` bitset indicates checksums
exist, the reader can optionally validate them against the read data.

## Data Encodings

cblock data is encoded before being stored. If compression is enabled, then the
cblock is encoded first, and then compressed.

Data cblocks are best-effort size limited to `--cfile-default-block-size` bytes, at
which point a new cblock will be added to the CFile.

### Plain Encoding

The simplest encoding type is plain encoding, which stores values in their
natural representation.

Plain encoding for fixed-width (integer) types consists of the little-endian
values, followed by a trailer containing two little-endian `uint32`s: the count
of values, and the ordinal position of the cblock.

Plain encoding for BINARY or STRING (variable-width) columns is laid out as
follows:

| ordinal position | little-endian `uint32` |
| --- | --- |
| num values | little-endian `uint32` |
| offsets position | little-endian `uint32` |
| values | sequential values with no padding |
| offsets | [group-varint frame-of-reference](#group-varint-frame-of-reference-encoding) encoded value offsets |

### Dictionary Encoding

[Dictionary encoding](dictionary-encoding) may be used for BINARY or STRING
columns. All dictionary encoded cblocks in a CFile share the same dictionary.
If the dictionary becomes full, subsequent cblocks in the CFile switch to plain
encoding. The dictionary is stored as a plain encoded binary cblock, and the
data codewords are stored as [bitshuffle encoded](#bitshuffle-encoding)
`uint32`s.

### Prefix Encoding

Currently used for `BINARY` and `STRING` data cblocks. This is based on the
encoding used by LevelDB for its data blocks, more or less.

Starts with a header of four [Group Varint] encoded `uint32`s:

| field |
| --- |
| number of elements |
| ordinal position |
| restart interval |
| unused |

Followed by prefix-compressed values. Each value is stored relative to the value
preceding it using the following format:

| field | type |
| --- | --- |
| `shared_bytes` | `varint32` |
| `unshared_bytes` | `varint32` |
| `delta` | `char[unshared_bytes]` |

Periodically there will be a "restart point" which is necessary for faster
binary searching. At a "restart point", `shared_bytes` is 0 but otherwise the
encoding is the same.

At the end of the cblock is a trailer with the offsets of the restart points:

| field | type |
| --- | --- |
| `restart_points` | `uint32[num_restarts]` |
| `num_restarts` | `uint32` |

The restart points are offsets relative to the start of the cblock, including
the header.

### Run-length Encoding

Integer and bool types may be [run-length encoded](RLE), which has a simply
layout: the number of values and ordinal position of the cblock as little-endian
`uint32`s, followed by the run-length encoded data. The encoder will
automatically fall back to a bit-packed (literal) encoding if the data is not
conducive to run-length encoding.

### Bitshuffle Encoding

Integer types may be [bitshuffle](bitshuffle) encoded. Bitshuffle-encoded
cblocks are automatically LZ4 compressed, so additional compression is not
recommended.

### Group Varint Frame-of-Reference Encoding

Used internally for `UINT32` blocks. Kudu does not support unsigned integer
columns, so this encoding is not used for column data.

Starts with a header of four [group-varint](group-varint) encoded `uint32`s:

| field |
| --- |
| number of elements |
| minimum element |
| ordinal position |
| unused |

The ordinal position is the ordinal position of the first item in the file. For
example, the first data cblock in the file has ordinal position 0. If that cblock
had 400 values, then the second data cblock would have ordinal position
400.

Followed by the actual data, each set of 4 integers using group-varint. The last
group is padded with 0s. Each integer is relative to the min element in the
header.

## CFile Index

CFiles may optionally include a positional (ordinal) index and value index.
Positional indexes are used to satisfy queries such as: "seek to the data cblock
containing the Nth entry in this CFile". Value indexes are used to satisfy
queries such as: "seek to the data cblock containing `123` in this CFile". Value
indexes are only present in CFiles which contain data stored in sorted order
(e.g. the primary key column).

Ordinal and value indices are organized as a [B-Tree] of index cblocks. As data
cblocks are written, entries are appended to the end of a leaf index cblock.
When a leaf index cblock reaches the configured index cblock size, it is added
to another index cblock higher up the tree, and a new leaf is started.  If the
intermediate index cblock fills, it will start a new intermediate cblock and
spill into an even higher-layer internal cblock.

For example:

```
                      [Int 0]
           -----------------------------
           |                           |
        [Int 1]                     [Int 2]
   -----------------             --------------
   |       |       |             |            |
[Leaf 0]  ...   [Leaf N]     [Leaf N+1]   [Leaf N+2]
```

In this case, we wrote N leaf blocks, which filled up the internal node labeled
Int 1. At this point, the writer would create Int 0 with one entry pointing to
Int 1. Further leaf blocks (N+1 and N+2) would be written to a new internal node
(Int 2). When the file is completed, Int 2 will spill, adding its entry into Int 0
as well.

Note that this strategy doesn't result in a fully balanced B-tree, but instead
results in a 100% "fill factor" on all nodes in each level except for the last
one written.

An index cblock is encoded similarly for both types of indexes:

```
<key> <cblock offset> <cblock size>
<key> <cblock offset> <cblock size>
...
   key: vint64 for positional, otherwise varint-length-prefixed string
   offset: vint64
   cblock size: vint32

<offset to first key>   (fixed32)
<offset to second key>  (fixed32)
...
   These offsets are relative to the start of the cblock.

<trailer>
   A IndexBlockTrailerPB protobuf
<trailer length>
```

Index cblocks are written using a [post-order traversal], and the index cblocks
may intersperse with the data cblocks.

The trailer protobuf includes a field which designates whether the cblock is a
leaf node or internal node of the B-Tree, allowing a reader to know whether the
pointer is to another index cblock or to a data cblock.

## DeltaFile

Every DeltaFile in a DiskRowSet is written to a CFile; in fact, a DeltaFile is
just a wrapper around CFile which specializes it for REDO and UNDO delta data.
The deltas associated with a row update are grouped into a RowChangeList and
written to as a binary values to the underlying CFile. Each value is prefixed
with a DeltaKey, which consists of the ordinal row index (within the
DiskRowSet), and the timestamp. The CFile includes a value index so that the
delta belonging to a specific row can be located efficiently.

## BloomFile

BloomFile is a wrapper around CFile which stores a bloomfilter datastructure.

[LEB128]: https://en.wikipedia.org/wiki/LEB128
[RLE]: https://en.wikipedia.org/wiki/Run-length_encoding
[`f82cf6918c`]: https://github.com/apache/kudu/commit/f82cf6918c00dff6aecad5a6b50836b7eb5db508
[B-tree]: https://en.wikipedia.org/wiki/B-tree
[bitshuffle]: https://github.com/kiyo-masui/bitshuffle
[cfile.proto]: ../../src/kudu/cfile/cfile.proto
[Group Varint]: https://static.googleusercontent.com/media/research.google.com/en//people/jeff/WSDM09-keynote.pdf
[post-order traversal]: https://en.wikipedia.org/wiki/Tree_traversal#Post-order
