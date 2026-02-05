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

#include "kudu/codegen/jit_frame_manager.h"

#include <sys/mman.h>

#include <cerrno>
#include <cstdint>
#include <ostream>
#include <system_error>
#include <utility>

#include <glog/logging.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/Support/Memory.h>
#include <llvm/Support/Process.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/port.h"

using llvm::SectionMemoryManager;
using llvm::sys::Memory;
using llvm::sys::MemoryBlock;
using llvm::sys::Process;

namespace kudu {
namespace codegen {

namespace {

int getPosixProtectionFlags(unsigned flags) {
  switch (flags & Memory::MF_RWE_MASK) {
  case Memory::MF_READ:
    return PROT_READ;
  case Memory::MF_WRITE:
    return PROT_WRITE;
  case Memory::MF_READ | Memory::MF_WRITE:
    return PROT_READ | PROT_WRITE;
  case Memory::MF_READ | Memory::MF_EXEC:
    return PROT_READ | PROT_EXEC;
  case Memory::MF_READ | Memory::MF_WRITE | Memory::MF_EXEC:
    return PROT_READ | PROT_WRITE | PROT_EXEC;
  case Memory::MF_EXEC:
    return PROT_EXEC;
  default:
    LOG(DFATAL) << "unsupported LLVM memory protection flags";
    return PROT_NONE;
  }
}

} // anonymous namespace


JITFrameManager::CustomMapper::CustomMapper()
    : memory_range_bytes_left_(0) {
}

// This implementation of allocateMappedMemory() method is modeled after LLVM's
// llvm::sys::Memory::allocateMappedMemory() in lib/Support/Unix/Memory.inc.
// One important difference is treating the 'near_block' memory address hint
// (if present) as the exact address for the result memory mapping, adding
// MAP_FIXED to the corresponding flags for the mmap() call.
MemoryBlock JITFrameManager::CustomMapper::allocateMappedMemory(
    SectionMemoryManager::AllocationPurpose /*purpose*/,
    size_t num_bytes,
    const MemoryBlock* const near_block,
    unsigned protection_flags,
    std::error_code& ec) {

  ec = std::error_code();
  if (num_bytes == 0) {
    return {};
  }

  // Is this a request to pre-allocate memory range for subsequent allocations?
  const bool is_pre_allocation = !near_block ||
      (near_block->base() == nullptr && near_block->allocatedSize() == 0);

  uintptr_t start = near_block ? reinterpret_cast<uintptr_t>(near_block->base()) +
                                     near_block->allocatedSize()
                               : 0;
  int mm_flags = MAP_PRIVATE | MAP_ANON;
  if (!is_pre_allocation) {
    mm_flags |= MAP_FIXED;
  }

  // If the start address is non-zero, this must be a follow-up allocation
  // within the pre-allocated memory range. Vice versa, a pre-allocation request
  // doesn't provide valid start address.
  DCHECK((start != 0 && !is_pre_allocation) ^ (start == 0 && is_pre_allocation));

  // Memory is allocated/mapped in multiples of the memory page size.
  static const size_t page_size = Process::getPageSizeEstimate();
  const size_t num_pages = (num_bytes + page_size - 1) / page_size;

  // Use the near hint and the page size to set a page-aligned starting address.
  if (start && start % page_size) {
    // Move the start address up to the nearest page boundary.
    start += page_size - (start % page_size);
  }
  const size_t size_bytes = page_size * num_pages;

  if (is_pre_allocation) {
    if (PREDICT_FALSE(isPreAllocatedRangeSet())) {
      // Once the pre-allocated range is set, all the subsequent requests
      // must provide non-null 'near_block' for allocations within the range.
      LOG(DFATAL) << "must not attempt pre-allocating memory multiple times";
      return {};
    }
    DCHECK_EQ(0, memory_range_bytes_left_);
  } else {
    // If this is a follow-up request, the information on the pre-allocated
    // range must already be set.
    if (PREDICT_FALSE(!isPreAllocatedRangeSet())) {
      LOG(DFATAL) << "must set pre-allocated memory range first";
      return {};
    }
    // For subsequent allocations, make sure there is still enough pre-allocated
    // memory left to accomodate a new memory range of the requested size.
    if (PREDICT_FALSE(memory_range_bytes_left_ < size_bytes)) {
      LOG(DFATAL) << "insufficient pre-allocated memory";
      return {};
    }

    // Check the provided 'near_block' hint for sanity: it must be within
    // the pre-allocated area.
    if (PREDICT_FALSE(reinterpret_cast<uintptr_t>(start) <
                      reinterpret_cast<uintptr_t>(memory_range_.base()))) {
      LOG(DFATAL) << "'near_hint' start address is beyond pre-allocated range";
      return {};
    }
    if (PREDICT_FALSE(reinterpret_cast<uintptr_t>(start) + size_bytes >
                      reinterpret_cast<uintptr_t>(memory_range_.base()) +
                          memory_range_.allocatedSize())) {
      LOG(DFATAL) << "invalid 'near_hint'";
      return {};
    }
    // Make sure the requested range with the provided 'near_block' hint
    // wouldn't clobber previously allocated range. As for the address hints
    // provided with the 'near_block' parameter, start addresses for new memory
    // ranges must increase.
    if (PREDICT_FALSE(reinterpret_cast<uintptr_t>(start) <
                      reinterpret_cast<uintptr_t>(prev_range_.base()) +
                          prev_range_.allocatedSize())) {
      LOG(DFATAL) << "'near_hint' would clobber previously allocated range";
      return {};
    }
  }

  const int protect = getPosixProtectionFlags(protection_flags);
  void* addr = ::mmap(
      reinterpret_cast<void*>(start), size_bytes, protect, mm_flags, -1, 0);
  if (PREDICT_FALSE(addr == MAP_FAILED)) {
    const int err = errno;
    ec = std::error_code(err, std::generic_category());
    return {};
  }

  MemoryBlock result(addr, size_bytes);
  // Update the amount of the pre-allocated memory left.
  if (!is_pre_allocation) {
    memory_range_bytes_left_ -= size_bytes;
    prev_range_ = result;
  }

  // Rely on protectMappedMemory to invalidate instruction cache.
  if (protection_flags & Memory::MF_EXEC) {
    ec = Memory::protectMappedMemory(result, protection_flags);
    if (ec) {
      return {};
    }
  }

  return result;
}

std::error_code JITFrameManager::CustomMapper::releaseMappedMemory(MemoryBlock& m) {
  return Memory::releaseMappedMemory(m);
}

std::error_code JITFrameManager::CustomMapper::protectMappedMemory(
    const MemoryBlock& block, unsigned flags) {
  return Memory::protectMappedMemory(block, flags);
}

void JITFrameManager::CustomMapper::setPreAllocatedRange(
    const llvm::sys::MemoryBlock& range) {
  // This should be called at most once per CustomMapper instance.
  DCHECK(!memory_range_.base());
  DCHECK_EQ(0, memory_range_.allocatedSize());
  memory_range_ = range;

  DCHECK_EQ(0, memory_range_bytes_left_);
  memory_range_bytes_left_ = range.allocatedSize();
}

bool JITFrameManager::CustomMapper::isPreAllocatedRangeSet() const {
  return memory_range_.base() != nullptr &&
         memory_range_.allocatedSize() != 0;
}

// TODO(aserbin): find a way to get rid of down_cast;
//                delegating constructor didn't help
JITFrameManager::JITFrameManager(std::unique_ptr<CustomMapper> mm)
    : SectionMemoryManager(std::move(mm)),
      mm_(down_cast<CustomMapper*>(this->getMemoryMapper())) {
}

JITFrameManager::~JITFrameManager() {
  // Release the memory mapping if it's been successfully allocated.
  if (preallocated_block_.base() != nullptr &&
      preallocated_block_.allocatedSize() != 0) {
    if (mm_->releaseMappedMemory(preallocated_block_)) {
      LOG(WARNING) << "JITFrameManager: could not release pre-allocated memory";
    }
  }
}

void JITFrameManager::reserveAllocationSpace(uintptr_t code_size,
                                             uint32_t code_align,
                                             uintptr_t ro_data_size,
                                             uint32_t ro_data_align,
                                             uintptr_t rw_data_size,
                                             uint32_t rw_data_align) {
  // This can be called only once per JITFrameManager instance.
  DCHECK(!preallocated_block_.base());
  DCHECK_EQ(0, preallocated_block_.allocatedSize());

  DCHECK_NE(0, code_align);
  DCHECK_NE(0, ro_data_align);
  DCHECK_NE(0, rw_data_align);

  static const size_t page_size = Process::getPageSizeEstimate();

  constexpr auto align_up = [](uintptr_t size, uint32_t alignment) {
    return alignment * ((size + alignment - 1) / alignment);
  };

  const auto code_required_size_bytes = align_up(code_size, code_align);
  const auto ro_data_required_size_bytes = align_up(ro_data_size, ro_data_align);
  const auto rw_data_required_size_bytes = align_up(rw_data_size, rw_data_align);

  // Extra safety margin: pre-allocate 2 times more, aligning up the the memory
  // page size for each section type.
  const size_t required_size_bytes = 2 * (
      align_up(code_required_size_bytes, page_size) +
      align_up(ro_data_required_size_bytes, page_size) +
      align_up(rw_data_required_size_bytes, page_size));

  // Reserve enough memory for the jitted sections to avoid fragmentation and
  // eliminate the risk of interleaving with any other memory allocations by
  // the process. Use the returned address as a hint for subsequent smaller
  // allocations performed by the loader for the segments of the jitted object
  // being loaded. Those smaller allocations will re-map consecutive chunks of
  // the pre-allocated MAP_ANON region using exact addresses and MAP_FIXED flag.
  // This approach guarantees reserving the pre-allocated range exclusively for
  // this JITFrameManager's subsequent activity, so it's guaranteed to have
  // no interleaving with any other memory areas allocated by this process.
  std::error_code ec;
  preallocated_block_ = mm_->allocateMappedMemory(
      SectionMemoryManager::AllocationPurpose::RWData,
      required_size_bytes,
      nullptr,
      Memory::MF_READ | Memory::MF_WRITE,
      ec);
  if (ec) {
    LOG(DFATAL) << "JITFrameManager: memory pre-allocation failed";
  } else {
    // Providing memory block 'mb_near' with the correct address and 0 size to
    // the SectionMemoryManager for subsequent memory allocations. This is to
    // point 'mb_near.base() + mb_near.allocatedSize()' to the start address
    // of the pre-allocated memory area. So, when RuntimeDyld requests this
    // JITFrameManager instance to allocate memory for the sections of the
    // jitted object being loaded, the memory for each section is allocated
    // strictly within the reserved memory area.
    DCHECK(preallocated_block_.base());
    DCHECK_NE(0, preallocated_block_.allocatedSize());
    setNearHintMB(MemoryBlock(preallocated_block_.base(), 0));
    mm_->setPreAllocatedRange(preallocated_block_);
  }
}

} // namespace codegen
} // namespace kudu
