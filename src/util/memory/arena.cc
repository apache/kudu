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

#include <boost/thread/mutex.hpp>

#include "util/memory/arena.h"

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;

namespace kudu {

template <bool THREADSAFE>
ArenaBase<THREADSAFE>::ArenaBase(
  BufferAllocator* const buffer_allocator,
  size_t initial_buffer_size,
  size_t max_buffer_size)
    : buffer_allocator_(buffer_allocator),
      max_buffer_size_(max_buffer_size),
      arena_footprint_(0) {
  AddComponent( CHECK_NOTNULL(NewComponent(initial_buffer_size, 0)) );
}

template <bool THREADSAFE>
ArenaBase<THREADSAFE>::ArenaBase(size_t initial_buffer_size, size_t max_buffer_size)
    : buffer_allocator_(HeapBufferAllocator::Get()),
      max_buffer_size_(max_buffer_size),
      arena_footprint_(0) {
  AddComponent( CHECK_NOTNULL(NewComponent(initial_buffer_size, 0)) );
}

template <bool THREADSAFE>
void *ArenaBase<THREADSAFE>::AllocateBytesFallback(const size_t size, const size_t align) {
  boost::lock_guard<boost::mutex> lock(component_lock_);

  // It's possible another thread raced with us and already allocated
  // a new component, in which case we should try the "fast path" again
  void * result = current_->AllocateBytesAligned(size, align);
  if (PREDICT_FALSE(result != NULL)) return result;

  // Really need to allocate more space.
  size_t next_component_size = min(2 * current_->size(), max_buffer_size_);
  // But, allocate enough, even if the request is large. In this case,
  // might violate the max_element_size bound.
  if (next_component_size < size) {
    next_component_size = size;
  }
  // If soft quota is exhausted we will only get the "minimal" amount of memory
  // we ask for. In this case if we always use "size" as minimal, we may degrade
  // to allocating a lot of tiny components, one for each string added to the
  // arena. This would be very inefficient, so let's first try something between
  // "size" and "next_component_size". If it fails due to hard quota being
  // exhausted, we'll fall back to using "size" as minimal.
  size_t minimal = (size + next_component_size) / 2;
  CHECK_LE(size, minimal);
  CHECK_LE(minimal, next_component_size);
  // Now, just make sure we can actually get the memory.
  Component* component = NewComponent(next_component_size, minimal);
  if (component == NULL) {
    component = NewComponent(next_component_size, size);
  }
  if (!component) return NULL;

  // Now, must succeed. The component has at least 'size' bytes.
  result = component->AllocateBytesAligned(size, align);
  CHECK(result != NULL);

  // Now add it to the arena.
  AddComponent(component);

  return result;
}

template <bool THREADSAFE>
typename ArenaBase<THREADSAFE>::Component* ArenaBase<THREADSAFE>::NewComponent(
  size_t requested_size,
  size_t minimum_size) {
  Buffer* buffer = buffer_allocator_->BestEffortAllocate(requested_size,
                                                         minimum_size);
  if (buffer == NULL) return NULL;

  CHECK_EQ(reinterpret_cast<uintptr_t>(buffer->data()) & (64 - 1), 0)
    << "Components should be 64-byte aligned: " << buffer->data();

  return new Component(buffer);
}

// LOCKING: component_lock_ must be held by the current thread.
template <bool THREADSAFE>
void ArenaBase<THREADSAFE>::AddComponent(ArenaBase::Component *component) {
  current_ = component;
  arena_.push_back(shared_ptr<Component>(current_));
  arena_footprint_ += current_->size();
}

template <bool THREADSAFE>
void ArenaBase<THREADSAFE>::Reset() {
  boost::lock_guard<boost::mutex> lock(component_lock_);

  shared_ptr<Component> last = arena_.back();
  if (arena_.size() > 1) {
    arena_.clear();
    arena_.push_back(last);
    current_ = last.get();
  }
  last->Reset();

#ifndef NDEBUG
  // In debug mode release the last component too for (hopefully) better
  // detection of memory-related bugs (invalid shallow copies, etc.).
  arena_.clear();
  AddComponent( CHECK_NOTNULL(NewComponent(last->size(), 0)) );
#endif
}

// Explicit instantiation.
template class ArenaBase<true>;
template class ArenaBase<false>;


}  // namespace supersonic
