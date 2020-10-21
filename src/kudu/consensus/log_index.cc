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

// The implementation of the Log Index.
//
// The log index is implemented by a set of on-disk files, each containing a fixed number
// (kEntriesPerIndexChunk) of fixed size entries. Each index chunk is numbered such that,
// for a given log index, we can determine which chunk contains its index entry by a
// simple division operation. Because the entries are fixed size, we can compute the
// index offset by a modulo.
//
// When the log is GCed, we remove any index chunks which are no longer needed, and
// unmap them.

#include "kudu/consensus/log_index.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/env.h"

using std::string;
using std::vector;
using strings::Substitute;

METRIC_DEFINE_counter(server, log_index_chunk_mmap_for_read,
                      "Number of mmap calls for reading an index chunk",
                      kudu::MetricUnit::kUnits,
                      "Number of times an index chunk had to be mmapeed "
                      "before a read operation.");

namespace kudu {
namespace log {

// The actual physical entry in the file.
// This mirrors LogIndexEntry but uses simple primitives only so we can
// read/write it via mmap.
// See LogIndexEntry for docs.
struct PhysicalEntry {
  int64_t term;
  uint64_t segment_sequence_number;
  uint64_t offset_in_segment;
} PACKED;

////////////////////////////////////////////////////////////
// LogIndex::IndexChunk implementation
////////////////////////////////////////////////////////////

// A single chunk of the index, representing a fixed number of entries.
// This class maintains the open file descriptor and mapped memory.
class LogIndex::IndexChunk : public RefCountedThreadSafe<LogIndex::IndexChunk> {
 public:
  // Construct an index chunk.
  // 'path' is the full path for the underlying file
  // 'size' is the configured size for this index chunk/file
  explicit IndexChunk(string path, int64_t size);
  ~IndexChunk();

  // Open the chunk file
  Status Open();

  // Memory map the chunk file
  // This is not thread safe with GetEntry() and SetEntry(). The caller should
  // synchronize correctly
  Status Mmap();

  // Unmap the chunk file from memory
  // This is not thread safe with GetEntry() and SetEntry(). The caller should
  // synchronize correctly
  void Munmap();

  // Get an entry from the memory mapped cunk file for a given index
  void GetEntry(int entry_index, PhysicalEntry* ret);

  // Set an entry in the memory mapped chunk file for a given index
  void SetEntry(int entry_index, const PhysicalEntry& entry);

  // Is this chunk file memory mapped?
  bool IsMmapped() const;

 private:
  const string path_; // path of the underlying chunk file
  int fd_; // file descriptor
  uint8_t* mapping_; // mmapped memory location of the chunk
  int64_t size_; // configured size for the chunk file
};

namespace  {
Status CheckError(int rc, const char* operation) {
  if (PREDICT_FALSE(rc < 0)) {
    int err = errno;
    return Status::IOError(operation, ErrnoToString(err), err);
  }
  return Status::OK();
}
} // anonymous namespace

LogIndex::IndexChunk::IndexChunk(std::string path, int64_t size)
    : path_(std::move(path)), fd_(-1), mapping_(nullptr), size_(size) {}

LogIndex::IndexChunk::~IndexChunk() {
  if (mapping_ != nullptr) {
    munmap(mapping_, size_);
  }

  if (fd_ >= 0) {
    int ret;
    RETRY_ON_EINTR(ret, close(fd_));
    if (PREDICT_FALSE(ret != 0)) {
      PLOG(WARNING) << "Failed to close fd " << fd_;
    }
  }
}

Status LogIndex::IndexChunk::Open() {
  RETRY_ON_EINTR(fd_, open(path_.c_str(), O_CLOEXEC | O_CREAT | O_RDWR, 0666));
  RETURN_NOT_OK(CheckError(fd_, "open"));

  int err;
  RETRY_ON_EINTR(err, ftruncate(fd_, size_));
  RETURN_NOT_OK(CheckError(fd_, "truncate"));

  return Status::OK();
}

Status LogIndex::IndexChunk::Mmap() {
  if (fd_ == -1) {
    return Status::IOError("Chunk should be opened before mmapping");
  }

  if (mapping_) {
    // Already mmaped, return
    return Status::OK();
  }

  mapping_ = static_cast<uint8_t*>(mmap(nullptr, size_, PROT_READ | PROT_WRITE,
                                        MAP_SHARED, fd_, 0));
  if (mapping_ == nullptr) {
    int err = errno;
    return Status::IOError("Unable to mmap()", ErrnoToString(err), err);
  }

  return Status::OK();
}

void LogIndex::IndexChunk::Munmap() {
  if (mapping_ != nullptr) {
    munmap(mapping_, size_);
    mapping_ = nullptr;
  }
}

void LogIndex::IndexChunk::GetEntry(int entry_index, PhysicalEntry* ret) {
  DCHECK_GE(fd_, 0) << "Must Open() first";
  memcpy(ret, mapping_ + sizeof(PhysicalEntry) * entry_index, sizeof(PhysicalEntry));
}

void LogIndex::IndexChunk::SetEntry(int entry_index, const PhysicalEntry& entry) {
  DCHECK_GE(fd_, 0) << "Must Open() first";
  memcpy(mapping_ + sizeof(PhysicalEntry) * entry_index, &entry, sizeof(PhysicalEntry));
}

bool LogIndex::IndexChunk::IsMmapped() const {
  return (mapping_ != nullptr);
}

////////////////////////////////////////////////////////////
// LogIndex
////////////////////////////////////////////////////////////

LogIndex::LogIndex(std::string base_dir)
  : base_dir_(std::move(base_dir)),
    mmap_for_reads_(nullptr) {}

LogIndex::~LogIndex() {
}

string LogIndex::GetChunkPath(int64_t chunk_idx) {
  return StringPrintf("%s/index.%09" PRId64, base_dir_.c_str(), chunk_idx);
}

Status LogIndex::OpenAllChunksOnStartup(
    Env *env,
    const scoped_refptr<MetricEntity>& metric_entity) {
  DCHECK(env);
  std::vector<std::string> children;
  RETURN_NOT_OK(env->GetChildren(base_dir_, &children));

  // Initialize metric counter
  mmap_for_reads_ =
    metric_entity->FindOrCreateCounter(&METRIC_log_index_chunk_mmap_for_read);

  for (const auto& fname: children) {
    if (fname.find("index.") != 0) {
      continue;
    }

    vector<string> v = strings::Split(fname, ".");
    if (v.size() != 2) {
      LOG(INFO) << "Improperly named file in wal directory skipped on recovery: " << fname;
      continue;
    }

    int64_t chunk_idx;
    if (!safe_strto64(v[1], &chunk_idx)) {
      LOG(INFO) << "Improperly named file in wal directory skipped on recovery: " << fname;
      continue;
    }

    VLOG(1) << "Opening index file on startup: " << fname << " for chunk idx " << chunk_idx;

    scoped_refptr<IndexChunk> chunk;
    RETURN_NOT_OK(OpenAndInsertChunk(chunk_idx, &chunk, /*should_mmap=*/false));
  }

  // mmap 'kNumChunksToMmap' chunks. Note that the latest chunks are mmapped
  // (chunks having the highest chunk_idx)
  int64_t mmapped_chunks = 0;
  for (auto rit = open_chunks_.rbegin(); rit != open_chunks_.rend(); ++rit) {
    if (mmapped_chunks == kNumChunksToMmap) {
      break;
    }

    RETURN_NOT_OK(rit->second->Mmap());
    mmapped_chunks++;
  }

  return Status::OK();
}

void LogIndex::SetNumMmapChunks(int64_t num_chunks) {
  if (num_chunks <= 0)
    return;

  std::lock_guard<simple_spinlock> l(open_chunks_lock_);
  kNumChunksToMmap = num_chunks;

  // If necessary, unmap additional chunks
  if (open_chunks_.size() <= kNumChunksToMmap) {
    return;
  }

  // get the number of chunks that are currently mmapped
  int64_t num_chunks_mmapped = 0;
  for (auto it = open_chunks_.begin(); it != open_chunks_.end(); ++it) {
    if (it->second->IsMmapped()) {
      num_chunks_mmapped++;
    }
  }

  // unmap additional chunks (starting with the oldest chunks)
  for (auto it = open_chunks_.begin();
      it != open_chunks_.end() && num_chunks_mmapped > kNumChunksToMmap;
      ++it) {
    if (it->second->IsMmapped()) {
      it->second->Munmap();
      num_chunks_mmapped--;
    }
  }
}

void LogIndex::SetNumEntriesPerChunkForTest(int64_t entries) {
  // WARNING: This should be called only for tests. Changing numer of entries
  // per chunk is dangerous and could render the chunk files unreadable
  kEntriesPerIndexChunk = entries;
}

Status LogIndex::MmapChunk(scoped_refptr<IndexChunk> *chunk) {
  if (open_chunks_.size() < kNumChunksToMmap) {
    RETURN_NOT_OK((*chunk)->Mmap());
    return Status::OK();
  }

  // The victim that needs to be unmapped is the oldest chunk (i.e the chunk
  // with the oldest chunk_idx). See documentation in log_index.h for more
  // details.
  //
  // Note that we have to reverse iterate through the open_chunks_ while the
  // caller is holding onto the open_chunks_lock_. With 'open_chunks_' map
  // having only a few hundred entries, this should be acceptable (to keep
  // things simple)
  int64_t num_chunks_mmapped = 0;
  auto rit = open_chunks_.rbegin();
  for (; rit != open_chunks_.rend(); ++rit) {
    if (rit->second->IsMmapped()) {
      num_chunks_mmapped++;
    }

    if (num_chunks_mmapped == kNumChunksToMmap)
      break;
  }

  // If there are 'kNumChunksToMmap' chunks already mmapped, then unmap the
  // 'victim' chunk
  if (num_chunks_mmapped == kNumChunksToMmap && rit != open_chunks_.rend()) {
    rit->second->Munmap();
  }

  // Now mmap the provided 'chunk'
  RETURN_NOT_OK((*chunk)->Mmap());

  return Status::OK();
}

Status LogIndex::OpenChunk(
    int64_t chunk_idx,
    scoped_refptr<IndexChunk>* chunk) {
  string path = GetChunkPath(chunk_idx);
  int64_t size = kEntriesPerIndexChunk * sizeof(PhysicalEntry);

  scoped_refptr<IndexChunk> new_chunk(new IndexChunk(path, size));
  RETURN_NOT_OK(new_chunk->Open());

  chunk->swap(new_chunk);
  return Status::OK();
}

Status LogIndex::OpenAndInsertChunk(
    int64_t chunk_idx,
    scoped_refptr<IndexChunk>* chunk,
    bool should_mmap) {
  RETURN_NOT_OK_PREPEND(OpenChunk(chunk_idx, chunk),
                        "Couldn't open index chunk");
  std::lock_guard<simple_spinlock> l(open_chunks_lock_);
  if (PREDICT_FALSE(ContainsKey(open_chunks_, chunk_idx))) {
    // Someone else opened the chunk in the meantime.
    // We'll just return that one.
    *chunk = FindOrDie(open_chunks_, chunk_idx);
    return Status::OK();
  }

  InsertOrDie(&open_chunks_, chunk_idx, *chunk);

  if (should_mmap) {
    RETURN_NOT_OK(MmapChunk(chunk));
  }

  return Status::OK();
}

Status LogIndex::GetChunkForIndex(int64_t log_index, bool create,
                                  scoped_refptr<IndexChunk>* chunk) {
  CHECK_GT(log_index, 0);
  int64_t chunk_idx = log_index / kEntriesPerIndexChunk;

  {
    std::lock_guard<simple_spinlock> l(open_chunks_lock_);
    if (FindCopy(open_chunks_, chunk_idx, chunk)) {
      return Status::OK();
    }
  }

  if (!create) {
    return Status::NotFound("chunk not found");
  }

  return OpenAndInsertChunk(chunk_idx, chunk, /*should_mmap=*/true);
}

Status LogIndex::AddEntry(const LogIndexEntry& entry) {
  scoped_refptr<IndexChunk> chunk;
  RETURN_NOT_OK(GetChunkForIndex(entry.op_id.index(),
                                 true /* create if not found */,
                                 &chunk));

  int index_in_chunk = entry.op_id.index() % kEntriesPerIndexChunk;
  DCHECK_LT(index_in_chunk, kEntriesPerIndexChunk);

  PhysicalEntry phys;
  phys.term = entry.op_id.term();
  phys.segment_sequence_number = entry.segment_sequence_number;
  phys.offset_in_segment = entry.offset_in_segment;

  {
    // Grab the 'open_chunks_lock_' to ensure that the chunk does not get
    // unmapped
    std::lock_guard<simple_spinlock> l(open_chunks_lock_);
    if (PREDICT_FALSE(!chunk->IsMmapped())) {
      RETURN_NOT_OK(MmapChunk(&chunk));
    }
    chunk->SetEntry(index_in_chunk, phys);
    VLOG(3) << "Added log index entry " << entry.ToString();
  }

  return Status::OK();
}

Status LogIndex::GetEntry(int64_t index, LogIndexEntry* entry) {
  scoped_refptr<IndexChunk> chunk;
  RETURN_NOT_OK(GetChunkForIndex(index,
                                 false /* do not create */,
                                 &chunk));
  int index_in_chunk = index % kEntriesPerIndexChunk;
  DCHECK_LT(index_in_chunk, kEntriesPerIndexChunk);
  PhysicalEntry phys;

  {
    // Grab the 'open_chunks_lock_' to ensure that the chunk does not get
    // unmapped
    std::lock_guard<simple_spinlock> l(open_chunks_lock_);
    if (PREDICT_FALSE(!chunk->IsMmapped())) {
      RETURN_NOT_OK(MmapChunk(&chunk));

      if (mmap_for_reads_) {
        mmap_for_reads_->Increment();
      }
    }

    chunk->GetEntry(index_in_chunk, &phys);
  }

  // We never write any real entries to offset 0, because there's a header
  // in each log segment. So, this indicates an entry that was never written.
  if (phys.offset_in_segment == 0) {
    return Status::NotFound("entry not found");
  }

  entry->op_id = consensus::MakeOpId(phys.term, index);
  entry->segment_sequence_number = phys.segment_sequence_number;
  entry->offset_in_segment = phys.offset_in_segment;

  return Status::OK();
}

void LogIndex::GC(int64_t min_index_to_retain) {
  int min_chunk_to_retain = min_index_to_retain / kEntriesPerIndexChunk;

  // Enumerate which chunks to delete.
  vector<int64_t> chunks_to_delete;
  {
    std::lock_guard<simple_spinlock> l(open_chunks_lock_);
    for (auto it = open_chunks_.begin();
         it != open_chunks_.lower_bound(min_chunk_to_retain); ++it) {
      chunks_to_delete.push_back(it->first);
    }
  }

  // Outside of the lock, try to delete them (avoid holding the lock during IO).
  for (int64_t chunk_idx : chunks_to_delete) {
    string path = GetChunkPath(chunk_idx);
    int rc = unlink(path.c_str());
    if (rc != 0) {
      PLOG(WARNING) << "Unable to delete index chunk " << path;
      continue;
    }
    LOG(INFO) << "Deleted log index segment " << path;
    {
      std::lock_guard<simple_spinlock> l(open_chunks_lock_);
      open_chunks_.erase(chunk_idx);
    }
  }
}

string LogIndexEntry::ToString() const {
  return Substitute("op_id=$0.$1 segment_sequence_number=$2 offset=$3",
                    op_id.term(), op_id.index(),
                    segment_sequence_number,
                    offset_in_segment);
}

} // namespace log
} // namespace kudu
