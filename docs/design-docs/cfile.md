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
written to a single BlockManager Block (not to be confused with CFile blocks,
which are internal to CFiles, and discussed for the remainder of this document).

A CFile is composed of a header, a variable number of blocks, and a footer.
There are a few different types of blocks, each with a different format: data
blocks, nullable data blocks, array data blocks, index blocks,
and dictionary blocks.

Data blocks consist of a sequence of consecutive values. Each value is assigned
an ordinal index, or offset, which is unique within the CFile. Thus, a block can
be identified within a CFile by the range of ordinal indexes it contains; for
instance, the first three blocks of a CFile might have the ordinal index ranges
`[0, 55]`, `[56, 132]`, and `[133, 511]`.

In order to support efficient seeks to an arbitrary ordinal index within the
CFile, a positional index may be included which contains a mapping of starting
ordinal index to data block. See [CFile Index](#cfile-index) for more details.

For CFiles which contain data in sorted order (for example, a CFile containing
the Primary Key column), a value index may be created. The value index supports
efficiently seeking to an arbitrary value in the CFile. See
[CFile Index](#cfile-index) for more details.

## File Format

The CFile header, blocks, and footer are written consecutively to the file
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
| message | variable | encoded [`CFileFooterPB`][cfile.proto] Protobuf message |
| magic | 8 | [see below](#versioning) |
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

### Data Block Layout

| field | width (bytes) | notes |
| --- | --- | --- |
| data | variable | [encoded](#data-encodings) data values |
| checksum | 4 | optional CRC-32 checksum of the data |

If the CFile is configured to use compression then the encoded data is
compressed before the checksum is appended.

### Nullable Data Block Layout

A column marked as nullable in the schema is stored in a nullable data block.
Since null cells aren't stored in the encoded data, it's necessary to preserve
information on nullity of the cells elsewhere. To do so, such a block includes
an additional bitmap to track that. Bit positions correspond to ordinal offsets
of the cells in their rowset. Non-null cells are marked with ones,
and null cells are marked with zero bits correspondingly.

| field | width (bytes) | notes |
| --- | --- | --- |
| value count | variable | [LEB128] encoded count of values |
| bitmap length | variable | [LEB128] encoded length of the following non-null bitmap |
| non-null bitmap | variable | [RLE] encoded bitmap |
| data | variable | encoded non-null data values |
| checksum | 4 | optional CRC-32 checksum of the value count, bitmap length, non-null bitmap, and data |

If the CFile is configured to use compression then the value count, bitmap
length, non-null bitmap, and encoded data are compressed before the checksum is
appended.

### Array Data Block Layout

One-dimensional arrays of primitive types are stored in array data blocks.
It's a columnar data format dedicated to effective storage and retrieval
of data representing a single column of one-dimensional array type.
The idea is to flatten/concatenate the elements of all the arrays cells
in one column into a single sequence, while maintaining the information
on the number of elements in each array cell. The information on whether
an array cell is null/non-null is stored in a dedicated non-null bitmap
field in the block's metadata as well. With that, it's possible to attribute
each element of the flattened/concatenated sequence to a particular element
in the corresponding array cell, while using the metadata on the validity
of individual array elements and array cells themselves.

The flattened sequence of values of the same primitive type is stored similarly
to the way it's done for a regular (non-array) nullable data block which backs
a column of values of the corresponding type in a table. This approach allows
for reusing all the existing encoders/decoders without any modifications
in the layout of encoded data blocks: there is no need to introduce any
new encoders/decoders.

| field | width (bytes) | notes |
| --- | --- | --- |
| flattened value count | variable | unsigned [LEB128] encoded count of values (including nulls) |
| array count | variable | unsigned [LEB128] encoded count of arrays (including null array cells) |
| 'flattened non-null bitmap' size | variable | unsigned [LEB128] encoded size of the following bitmap field |
| flattened non-null bitmap | variable | [RLE] encoded bitmap on non-nullness of elements in the flattened sequence |
| 'array element numbers' size | variable | unsigned [LEB128] encoded size of the following field |
| array element numbers | variable | [RLE] encoded sequence of 16-bit unsigned integers |
| 'array non-null bitmap' size | variable | unsigned [LEB128] encoded size of the following bitmap field |
| array non-null bitmap | variable | [RLE] encoded bitmap on non-nullness of array cells |
| data | variable | encoded non-null data values (flattened sequence of all non-null elements in array cells) |
| checksum | 4 | optional CRC-32 checksum of the preceding fields |

The 'array element numbers' field provides information on the number
of elements (including nulls) in each array cell of the column.
The maximum possible number of elements in one array cell is 65535: the
limitation comes from the width of the RLE-encoded integers used to store
that information in the field.

The number of logical bits in 'flattened non-null bitmap' equals
to the number stored in the 'flattened value count' field.

The number of logical bits in 'array non-null bitmap' is the same
as the number of elements in 'array element numbers': that's the total
number of array cells in the column stored in the data block. The latter
is stored separately as-is in the 'array count' field.

If the CFile is configured to use compression then all the fields preceding
the `checksum` field are compressed before the checksum is appended.

Using [RLE] encoding for the number of elements in array cells allows for
effective usage of the storage space for the following use cases:
* data with all the arrays having same number of elements
* data with long runs of arrays having same number of elements in one long run
* sparse data with long runs of empty or null arrays

Since the array data block format was introduced long after the data and
nullable data blocks formats, a dedicated feature flag
`IncompatibleFeatures::ARRAY_DATA_BLOCK` is set in the `incompatible_features`
field of the CFileFooterPB message. The feature flag is used to ensure that
CFile reader implementation is aware of the array data block format,
so it can properly read the data stored in the file's blocks without confusing
it with nullable data block data.

#### Examples of Array Data Block Layout
Below is a few examples of representing columns of integer arrays of
arbitrary length in array data blocks. For the illustrative purposes,
the data is shown in human readable format, not the actual binary formats
used to store the data as per the specification above.

##### Example A

| field | value in human readable format for illustration |
| --- | --- |
| flattened value count | 10 |
| array count | 7 |
| 'flattened non-null bitmap' size | ... |
| flattened non-null bitmap | 1111111101 |
| 'array element numbers' size | ... |
| array element numbers | 2,0,0,2,4,1,1 |
| 'array non-null bitmap' size | ... |
| array non-null bitmap | 1101111 |
| data | 1,2,3,4,5,6,7,8,9 |
| checksum | ... |

This results in the following sequence of arrays, where the first column
shows the array cell's boundaries in the flattened sequence, and the second
column shows the result cells of the decoded array column:

| array boundaries | array value |
| --- | --- |
| [0, 2) | { 1,2 } |
| [2, 2) | {} |
| [2, 2) | null |
| [2, 4) | { 3,4 } |
| [4, 8) | { 5,6,7,8 } |
| [8, 9) | { null } |
| [9, .) | { 9 } |

##### Example B

| field | value in human readable format for illustration |
| --- | --- |
| flattened value count | 3 |
| array count | 4 |
| 'flattened non-null bitmap' size | ... |
| flattened non-null bitmap | 011 |
| 'array element numbers' size | ... |
| array element numbers | 1,0,0,2 |
| 'array non-null bitmap' size | ... |
| array non-null bitmap | 1011 |
| data | 4,2 |
| checksum | ... |

This results in the following sequence of arrays, where the first column
shows the array cell's boundaries in the flattened sequence, and the second
column shows the result cells of the decoded array column:

| array boundaries | array value |
| --- | --- |
| [0, 1) | { null } |
| [1, 1) | null |
| [1, 1) | {} |
| [1, .) | { 4,2 } |

##### Example C

| field | value in human readable format for illustration |
| --- | --- |
| flattened value count | 10 |
| array count | 1 |
| 'flattened non-null bitmap' size | ... |
| flattened non-null bitmap | 1101111101 |
| 'array element numbers' size | ... |
| array element numbers | 10 |
| 'array non-null bitmap' size | ... |
| array non-null bitmap | 1 |
| data | 2,3,6,8,5,3,1,0 |
| checksum | ... |

This results in the following sequence of arrays, where the first column
shows the array cell's boundaries in the flattened sequence, and the second
column shows the result cells of the decoded array column:

| field | value in human readable format |
| --- | --- |
| [0, .) | { 2,3,null,6,8,5,3,1,null,0 } |

### Checksums

Checksums can be optionally written and verified.

When checksums for the header, data, and footer are written in the CFile, the
`incompatible_features` bitset in the CFileFooterPB message is used. A
"checksum" bit is set to ensure the reader knows if checksums exist.

When reading a CFile the footer should be read first to find if the file
contains checksums. If the `incompatible_features` bitset indicates checksums
exist, the reader can optionally validate them against the read data.

## Data Encodings

Block data is encoded before being stored. If compression is enabled, then the
block is encoded first, and then compressed.

Data blocks are best-effort size limited to `--cfile_default_block_size` bytes, at
which point a new block will be added to the CFile.

### Plain Encoding

The simplest encoding type is plain encoding, which stores values in their
natural representation.

Plain encoding for fixed-width (integer) types consists of the little-endian
values, followed by a trailer containing two little-endian `uint32`s: the count
of values, and the ordinal position of the block.

Plain encoding for BINARY or STRING (variable-width) columns is laid out as
follows:

| ordinal position | little-endian `uint32` |
| --- | --- |
| num values | little-endian `uint32` |
| offsets position | little-endian `uint32` |
| values | sequential values with no padding |
| offsets | [group-varint frame-of-reference](#group-varint-frame-of-reference-encoding) encoded value offsets |

### Dictionary Encoding

Dictionary encoding may be used for BINARY or STRING
columns. All dictionary encoded blocks in a CFile share the same dictionary. If
the dictionary becomes full, subsequent blocks in the CFile switch to plain
encoding. The dictionary is stored as a plain encoded binary block, and the data
codewords are stored as [bitshuffle encoded](#bitshuffle-encoding) `uint32`s.

### Prefix Encoding

Currently used for `BINARY` and `STRING` data blocks. This is based on the
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

At the end of the block is a trailer with the offsets of the restart points:

| field | type |
| --- | --- |
| `restart_points` | `uint32[num_restarts]` |
| `num_restarts` | `uint32` |

The restart points are offsets relative to the start of the block, including the
header.

### Run-length Encoding

Integer and bool types may be [run-length encoded](RLE), which has a simply
layout: the number of values and ordinal position of the block as little-endian
`uint32`s, followed by the run-length encoded data. The encoder will
automatically fall back to a bit-packed (literal) encoding if the data is not
conducive to run-length encoding.

### Bitshuffle Encoding

Integer types may be [bitshuffle](bitshuffle) encoded. Bitshuffle-encoded
blocks are automatically LZ4 compressed, so additional compression is not
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
example, the first data block in the file has ordinal position 0. If that block
had 400 values, then the second data block would have ordinal position
400.

Followed by the actual data, each set of 4 integers using group-varint. The last
group is padded with 0s. Each integer is relative to the min element in the
header.

## CFile Index

CFiles may optionally include a positional (ordinal) index and value index.
Positional indexes are used to satisfy queries such as: "seek to the data block
containing the Nth entry in this CFile". Value indexes are used to satisfy
queries such as: "seek to the data block containing `123` in this CFile". Value
indexes are only present in CFiles which contain data stored in sorted order
(e.g. the primary key column).

Ordinal and value indices are organized as a [B-Tree] of index blocks. As data
blocks are written, entries are appended to the end of a leaf index block.
When a leaf index block reaches the configured index block size, it is added
to another index block higher up the tree, and a new leaf is started.  If the
intermediate index block fills, it will start a new intermediate block and
spill into an even higher-layer internal block.

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

An index block is encoded similarly for both types of indexes:

```
<key> <block offset> <block size>
<key> <block offset> <block size>
...
   key: vint64 for positional, otherwise varint-length-prefixed string
   offset: vint64
   block size: vint32

<offset to first key>   (fixed32)
<offset to second key>  (fixed32)
...
   These offsets are relative to the start of the block.

<trailer>
   A IndexBlockTrailerPB protobuf
<trailer length>
```

Index blocks are written using a [post-order traversal], and the index blocks
may intersperse with the data blocks.

The trailer protobuf includes a field which designates whether the block is a
leaf node or internal node of the B-Tree, allowing a reader to know whether the
pointer is to another index block or to a data block.

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
