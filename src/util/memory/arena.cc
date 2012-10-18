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

#include "util/memory/arena.h"

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;

namespace kudu {

Arena::Arena(BufferAllocator* const buffer_allocator,
             size_t initial_buffer_size,
             size_t max_buffer_size)
    : buffer_allocator_(buffer_allocator),
      max_buffer_size_(max_buffer_size),
      arena_footprint_(0) {
  CHECK_NOTNULL(AddComponent(initial_buffer_size, 0));
}

Arena::Arena(size_t initial_buffer_size, size_t max_buffer_size)
    : buffer_allocator_(HeapBufferAllocator::Get()),
      max_buffer_size_(max_buffer_size),
      arena_footprint_(0) {
  CHECK_NOTNULL(AddComponent(initial_buffer_size, 0));
}

void* Arena::AllocateBytes(const size_t size) {
  void* result = current_->AllocateBytes(size);
  if (result != NULL) return result;

  // Need to allocate more space.
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
  Component* component = AddComponent(next_component_size, minimal);
  if (component == NULL) {
    component = AddComponent(next_component_size, size);
  }
  if (!component) return NULL;
  // Now, must succeed. The component has at least 'size' bytes.
  result = component->AllocateBytes(size);
  CHECK(result != NULL);
  return result;
}

Arena::Component* Arena::AddComponent(size_t requested_size,
                                      size_t minimum_size) {
  Buffer* buffer = buffer_allocator_->BestEffortAllocate(requested_size,
                                                         minimum_size);
  if (buffer == NULL) return NULL;
  current_ = new Component(buffer);
  arena_.push_back(linked_ptr<Component>(current_));
  arena_footprint_ += current_->size();
  return current_;
}

void Arena::Reset() {
  linked_ptr<Component> last = arena_.back();
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
  CHECK_NOTNULL(AddComponent(last->size(), 0));
#endif
}

}  // namespace supersonic
