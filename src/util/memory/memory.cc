// Copyright 2010 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "util/memory/memory.h"

#include <string.h>

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <cstdlib>

#include <gflags/gflags.h>

namespace kudu {

namespace {
static char dummy_buffer[0] = {};
}

void OverwriteWithPattern(char* p, size_t len, StringPiece pattern) {
  CHECK_LT(0, pattern.size());
  for (size_t i = 0; i < len; ++i) {
    p[i] = pattern[i % pattern.size()];
  }
}

Buffer::~Buffer() {
#ifndef NDEBUG
  OverwriteWithPattern(reinterpret_cast<char*>(data_), size_, "BAD");
#endif
  if (allocator_ != NULL) allocator_->FreeInternal(this);
}

void BufferAllocator::LogAllocation(size_t requested,
                                    size_t minimal,
                                    Buffer* buffer) {
  if (buffer == NULL) {
    LOG(WARNING) << "Memory allocation failed in Supersonic. "
                 << "Number of bytes requested: " << requested
                 << ", minimal: " << minimal;
    return;
  }
  if (buffer->size() < requested) {
    LOG(WARNING) << "Memory allocation warning in Supersonic. "
                 << "Number of bytes requested to allocate: " << requested
                 << ", minimal: " << minimal
                 << ", and actually allocated: " << buffer->size();
  }
}

// TODO(onufry) - test whether the code still tests OK if we set this to true,
// or remove this code and add a test that Google allocator does not change it's
// contract - 16-aligned in -c opt and %16 == 8 in debug.
DEFINE_bool(allocator_aligned_mode, false,
            "Use 16-byte alignment instead of 8-byte, "
            "unless explicitly specified otherwise - to boost SIMD");

HeapBufferAllocator::HeapBufferAllocator()
  : aligned_mode_(FLAGS_allocator_aligned_mode) {
}

Buffer* HeapBufferAllocator::AllocateInternal(
    const size_t requested,
    const size_t minimal,
    BufferAllocator* const originator) {
  DCHECK_LE(minimal, requested);
  void* data;
  size_t attempted = requested;
  while (true) {
    data = (attempted == 0) ? &dummy_buffer[0] : Malloc(attempted);
    if (data != NULL) {
      return CreateBuffer(data, attempted, originator);
    }
    if (attempted == minimal) return NULL;
    attempted = minimal + (attempted - minimal - 1) / 2;
  }
}

bool HeapBufferAllocator::ReallocateInternal(
    const size_t requested,
    const size_t minimal,
    Buffer* const buffer,
    BufferAllocator* const originator) {
  DCHECK_LE(minimal, requested);
  void* data;
  size_t attempted = requested;
  while (true) {
    if (attempted == 0) {
      if (buffer->size() > 0) free(buffer->data());
      data = &dummy_buffer[0];
    } else {
      if (buffer->size() > 0) {
        data = Realloc(buffer->data(), buffer->size(), attempted);
      } else {
        data = Malloc(attempted);
      }
    }
    if (data != NULL) {
      UpdateBuffer(data, attempted, buffer);
      return true;
    }
    if (attempted == minimal) return false;
    attempted = minimal + (attempted - minimal - 1) / 2;
  }
}

void HeapBufferAllocator::FreeInternal(Buffer* buffer) {
  if (buffer->size() > 0) free(buffer->data());
}

void* HeapBufferAllocator::Malloc(size_t size) {
  if (aligned_mode_) {
    void* data;
    if (posix_memalign(&data, 16, ((size + 15) / 16) * 16)) {
       return NULL;
    }
    return data;
  } else {
    return malloc(size);
  }
}

void* HeapBufferAllocator::Realloc(void* previousData, size_t previousSize,
                                   size_t newSize) {
  if (aligned_mode_) {
    void* data = Malloc(newSize);
    if (data) {
// NOTE(ptab): We should use realloc here to avoid memmory coping,
// but it doesn't work on memory allocated by posix_memalign(...).
// realloc reallocates the memory but doesn't preserve the content.
// TODO(ptab): reiterate after some time to check if it is fixed (tcmalloc ?)
      memcpy(data, previousData, min(previousSize, newSize));
      free(previousData);
      return data;
    } else {
      return NULL;
    }
  } else {
    return realloc(previousData, newSize);
  }
}

Buffer* ClearingBufferAllocator::AllocateInternal(size_t requested,
                                                  size_t minimal,
                                                  BufferAllocator* originator) {
  Buffer* buffer = DelegateAllocate(delegate_, requested, minimal,
                                    originator);
  if (buffer != NULL) memset(buffer->data(), 0, buffer->size());
  return buffer;
}

bool ClearingBufferAllocator::ReallocateInternal(size_t requested,
                                                 size_t minimal,
                                                 Buffer* buffer,
                                                 BufferAllocator* originator) {
  size_t offset = (buffer != NULL ? buffer->size() : 0);
  bool success = DelegateReallocate(delegate_, requested, minimal, buffer,
                                    originator);
  if (success && buffer->size() > offset) {
    memset(static_cast<char*>(buffer->data()) + offset, 0,
           buffer->size() - offset);
  }
  return success;
}

void ClearingBufferAllocator::FreeInternal(Buffer* buffer) {
  DelegateFree(delegate_, buffer);
}

Buffer* MediatingBufferAllocator::AllocateInternal(
    const size_t requested,
    const size_t minimal,
    BufferAllocator* const originator) {
  // Allow the mediator to trim the request.
  size_t granted;
  if (requested > 0) {
    granted = mediator_->Allocate(requested, minimal);
    if (granted < minimal) return NULL;
  } else {
    granted = 0;
  }
  Buffer* buffer = DelegateAllocate(delegate_, granted, minimal, originator);
  if (buffer == NULL) {
    mediator_->Free(granted);
  } else if (buffer->size() < granted) {
    mediator_->Free(granted - buffer->size());
  }
  return buffer;
}

bool MediatingBufferAllocator::ReallocateInternal(
    const size_t requested,
    const size_t minimal,
    Buffer* const buffer,
    BufferAllocator* const originator) {
  // Allow the mediator to trim the request. Be conservative; assume that
  // realloc may degenerate to malloc-memcpy-free.
  size_t granted;
  if (requested > 0) {
    granted = mediator_->Allocate(requested, minimal);
    if (granted < minimal) return false;
  } else {
    granted = 0;
  }
  size_t old_size = buffer->size();
  if (DelegateReallocate(delegate_, granted, minimal, buffer, originator)) {
    mediator_->Free(granted - buffer->size() + old_size);
    return true;
  } else {
    mediator_->Free(granted);
    return false;
  }
}

void MediatingBufferAllocator::FreeInternal(Buffer* buffer) {
  mediator_->Free(buffer->size());
  DelegateFree(delegate_, buffer);
}

Buffer* MemoryStatisticsCollectingBufferAllocator::AllocateInternal(
    const size_t requested,
    const size_t minimal,
    BufferAllocator* const originator) {
  Buffer* buffer = DelegateAllocate(delegate_, requested, minimal, originator);
  if (buffer != NULL) {
    memory_stats_collector_->AllocatedMemoryBytes(buffer->size());
  } else {
    memory_stats_collector_->RefusedMemoryBytes(minimal);
  }
  return buffer;
}

bool MemoryStatisticsCollectingBufferAllocator::ReallocateInternal(
    const size_t requested,
    const size_t minimal,
    Buffer* const buffer,
    BufferAllocator* const originator) {
  const size_t old_size = buffer->size();
  bool outcome = DelegateReallocate(delegate_, requested, minimal, buffer,
                                    originator);
  if (buffer->size() > old_size) {
    memory_stats_collector_->AllocatedMemoryBytes(buffer->size() - old_size);
  } else if (buffer->size() < old_size) {
    memory_stats_collector_->FreedMemoryBytes(old_size - buffer->size());
  } else if (!outcome && (minimal > buffer->size())) {
    memory_stats_collector_->RefusedMemoryBytes(minimal - buffer->size());
  }
  return outcome;
}

void MemoryStatisticsCollectingBufferAllocator::FreeInternal(Buffer* buffer) {
  DelegateFree(delegate_, buffer);
  memory_stats_collector_->FreedMemoryBytes(buffer->size());
}

}  // namespace supersonic
