// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_CONSENSUS_LOG_INDEX_H
#define KUDU_CONSENSUS_LOG_INDEX_H

#include <cstdint>
#include <string>
#include <map>

#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {
class Env;
namespace log {

// An entry in the index.
struct LogIndexEntry {
  consensus::OpId op_id;

  // The sequence number of the log segment which contains this entry.
  int64_t segment_sequence_number;

  // The offset within that log segment for the batch which contains this
  // entry. Note that the offset points to an entire batch which may contain
  // more than one replicate.
  int64_t offset_in_segment;

  std::string ToString() const;
};

// An on-disk structure which indexes from OpId index to the specific position in the WAL
// which contains the latest ReplicateMsg for that index.
//
// This structure is on-disk but *not durable*. We use mmap()ed IO to write it out, and
// never sync it to disk. Its only purpose is to allow random-reading earlier entries from
// the log to serve to Raft followers.
//
// This class is thread-safe, but doesn't provide a memory barrier between writers and
// readers. In other words, if a reader is expected to see an index entry written by a
// writer, there should be some other synchronization between them to ensure visibility.
//
// See .cc file for implementation notes.
class LogIndex : public RefCountedThreadSafe<LogIndex> {
 public:
  explicit LogIndex(std::string base_dir);

  // Record an index entry in the index.
  Status AddEntry(const LogIndexEntry& entry);

  // Retrieve an existing entry from the index.
  // Returns NotFound() if the given log entry was never written.
  Status GetEntry(int64_t index, LogIndexEntry* entry);

  // Indicate that we no longer need to retain information about indexes lower than the
  // given index. Note that the implementation is conservative and _may_ choose to retain
  // earlier entries.
  void GC(int64_t min_index_to_retain);

  // Number of chunks to mmap. The default value is 3.
  void SetNumMmapChunks(int64_t num_chunks);

  // Only to be used in tests. It is dangerous to change number of entries per
  // chunk on a running instance or an instance which already has created a few
  // index chunks. This changes the on-disk format of the chunk file rendering
  // any previously created chunks unreadable
  void SetNumEntriesPerChunkForTest(int64_t entries);

  // Opens all chunks files found in the file system and inserts the chunk into
  // 'open_chunks_' map. Also mmaps 'kNumChunksToMmap' latest chunks. Also
  // initializes the metric counter ''mmap_for_reads_'
  Status OpenAllChunksOnStartup(
      Env *env, const scoped_refptr<MetricEntity>& metric_entity);

 private:
  friend class RefCountedThreadSafe<LogIndex>;

  ~LogIndex();

  class IndexChunk;

  // Opens the file corresponding to 'chunk_idx' and inserts it into
  // 'open_chunks_'
  Status OpenAndInsertChunk(
      int64_t chunk_idx,
      scoped_refptr<IndexChunk>* chunk,
      bool should_mmap);

  // Open the on-disk chunk with the given index.
  // Note: 'chunk_idx' is the index of the index chunk, not the index of a log _entry_.
  Status OpenChunk(
      int64_t chunk_idx, 
      scoped_refptr<IndexChunk>* chunk);

  // mmaps the file corresponding to chunk. The caller should hold
  // 'open_chunks_lock_' and 'chunk' should have already been opened and
  // inserted into 'open_chunks_' map.
  //
  // At any given time, the instance can only mmap a max of 'kChunksToMmap'
  // chunks. Hence, this method might have to 'evict' and unmap a chunk before
  // it can mmap the provided 'chunk'. The victim is chosen to be the oldest
  // chunk that is mmapped (i.e the chunk that has the lowest chunk_idx). The
  // rationale is that writes only append an entry to the latest chunk and hence
  // it should always be mmapped. A side effect of such an eviction policy is
  // that the most lagging peer is always the victim which will delay that peer
  // from catching up sooner. One way to void this is to configure
  // 'kChunksToMMap' to be equal to the number of peers in the ring and each
  // peer have its own slot for 'mmapping' an index chunk (but this strategy is
  // not implemented yet). Check 'kChunksToMmap' for more details
  Status MmapChunk(scoped_refptr<IndexChunk>* chunk);

  // Return the index chunk which contains the given log index.
  // If 'create' is true, creates it on-demand. If 'create' is false, and
  // the index chunk does not exist, returns NotFound.
  // Whenever a new chunk is created, it also mmaps the chunk
  Status GetChunkForIndex(int64_t log_index, bool create,
                          scoped_refptr<IndexChunk>* chunk);

  // Return the path of the given index chunk.
  std::string GetChunkPath(int64_t chunk_idx);

  // The base directory where index files are located.
  const std::string base_dir_;

  simple_spinlock open_chunks_lock_;

  // Map from chunk index to IndexChunk. The chunk index is the log index modulo
  // the number of entries per chunk (see docs in log_index.cc).
  // Protected by open_chunks_lock_
  typedef std::map<int64_t, scoped_refptr<IndexChunk> > ChunkMap;
  ChunkMap open_chunks_;

  // Number of index chunks to mmap for faster access. The default value is 3.
  //
  // The latest index chunks (ones with the highest chunk_idx) is always
  // mmapped. This is required for better write performance. A write
  // 'operation' into the log will always append an entry to the latest index
  // chunk - i.e index chunk is an append only operation (unless when an OpId
  // is trimmed from the replication log)
  //
  // A lagging peer that is trying to catch up might trigger a read operation
  // from an index chunk that is unmapped. One of the existing mmapped chunk
  // is unmapped to make space for dynamically map an index chunk for read
  // operation. The victim chunk that gets unmapped is the oldest index chunk
  // (so that writes are not effected)
  //
  // It might so happen that a set of lagging peers will thrash each other. For
  // example, follower1 is trying to catchup OpId(1, 1), follower2 is trying to
  // cathcup OpId(1, 1000001) and follower3 is trying to catchup (1, 2000001).
  // In this case, each follower will thrash and end up mapping/unmapping chunks
  // rapidly to read from the replication log. This is a very unlikely scenario
  // (since there should be peers lagging across OpIds that are multiple million
  // trxs away from each other).
  //
  // On followers, learners and any other nodes in the ring that are not
  // expected to serve read requests, this could be configured to value of 1 or
  // 2 (the premise being that writes only 'append' to the latest chunk)
  int64_t kNumChunksToMmap = 3;

  // Number of entries per index chunk.
  // WARNING: this is made 'configurable' only for tests. This should never be
  // modified once a ring is created
  int64_t kEntriesPerIndexChunk = 1000000;

  // Counter tracking number of times an index chunk had to be mmapped
  // dynamically for a read operation
  scoped_refptr<Counter> mmap_for_reads_;

  DISALLOW_COPY_AND_ASSIGN(LogIndex);
};

} // namespace log
} // namespace kudu
#endif /* KUDU_CONSENSUS_LOG_INDEX_H */
