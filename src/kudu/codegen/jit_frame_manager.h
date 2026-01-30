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
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>

#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/Support/Memory.h>

namespace kudu {
namespace codegen {

class JITFrameManager final : public llvm::SectionMemoryManager {
 public:

  // This implementation of the SectionMemoryManager::MemoryMapper interface
  // is used by SectionMemoryManager to request memory pages from the OS.
  // For the documentation of the interface, see in-line docs
  // for LLVM's SectionMemoryManager::MemoryMapper in SectionMemoryManager.h.
  class CustomMapper final : public SectionMemoryManager::MemoryMapper {
   public:
    CustomMapper();
    ~CustomMapper() override = default;

    llvm::sys::MemoryBlock allocateMappedMemory(
        SectionMemoryManager::AllocationPurpose /*purpose*/,
        size_t num_bytes,
        const llvm::sys::MemoryBlock* const near_block, // NOLINT(readability-avoid-const-params-in-decls)
        unsigned protection_flags,
        std::error_code& ec) override;

    std::error_code releaseMappedMemory(llvm::sys::MemoryBlock& m) override;

    std::error_code protectMappedMemory(const llvm::sys::MemoryBlock& block,
                                        unsigned flags) override;

    // Store the information on the pre-allocated memory range. This
    // information is used to check for the range of subsequent memory
    // allocations when allocateMappedMemory() is called with non-null
    // 'near_block' argument.
    void setPreAllocatedRange(const llvm::sys::MemoryBlock& range);

   private:
    // Whether a valid pre-allocated memory range has been set.
    bool isPreAllocatedRangeSet() const;

    // The pre-allocated range that all the allocations with non-null
    // 'near_block' must fit into.
    llvm::sys::MemoryBlock memory_range_;

    // Previously allocated memory block within the pre-allocated area.
    // It's used to make sure follow-up allocation requests with provided
    // 'near_hint' memory block don't overlap with already allocated ranges.
    llvm::sys::MemoryBlock prev_range_;

    // The number of remaining bytes in the pre-allocated memory range.
    // Each call to the allocateMappedMemory() method with non-null 'near_block'
    // decrements this by the size of the newly allocated memory block.
    int64_t memory_range_bytes_left_;
  };

  explicit JITFrameManager(std::unique_ptr<CustomMapper> mm);
  ~JITFrameManager() override;

  // This custom memory manager reserves/allocates memory for object sections
  // to be loaded in advance.
  bool needsToReserveAllocationSpace() override {
    return true;
  }

  // Reserve the memory to provide at least the specified amount of memory for
  // object sections.
  void reserveAllocationSpace(uintptr_t code_size,
                              uint32_t code_align,
                              uintptr_t ro_data_size,
                              uint32_t ro_data_align,
                              uintptr_t rw_data_size,
                              uint32_t rw_data_align) override;
 private:
  // This is a non-owning pointer to the memory mapper object that's passed to
  // the constructor and then to the base SectionMemoryManager object.
  CustomMapper* const mm_;

  // The result of memory pre-allocation performed by reserveAllocationSpace().
  llvm::sys::MemoryBlock preallocated_block_;
};

} // namespace codegen
} // namespace kudu
