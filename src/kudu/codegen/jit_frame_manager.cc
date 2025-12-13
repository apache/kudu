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

#include <cstdint>
#include <iterator>

#include <llvm/ExecutionEngine/SectionMemoryManager.h>

// External symbols from libgcc/libunwind.
extern "C" void __register_frame(void*);  // NOLINT(bugprone-reserved-identifier)
extern "C" void __deregister_frame(void*);// NOLINT(bugprone-reserved-identifier)

using llvm::SectionMemoryManager;
using llvm::StringRef;
using std::lock_guard;

namespace kudu {
namespace codegen {

// Initialize the static mutex
std::mutex JITFrameManager::kRegistrationMutex;

JITFrameManager::~JITFrameManager() {
  // Be explicit about avoiding the virtual dispatch: invoke
  // deregisterEHFramesImpl() instead of deregisterEHFrames().
  deregisterEHFramesImpl();
}

uint8_t* JITFrameManager::allocateCodeSection(uintptr_t size,
                                              unsigned alignment,
                                              unsigned section_id,
                                              StringRef section_name) {
    // Add extra padding for EH frame section: it's zeroed out later upon
    // registerEHFrames() calls.
    if (section_name == ".eh_frame") {
      size += 4;
    }
    return SectionMemoryManager::allocateCodeSection(
        size, alignment, section_id, section_name);
  }

void JITFrameManager::registerEHFrames(uint8_t* addr,
                                       uint64_t /*load_addr*/,
                                       size_t size) {
  lock_guard guard(kRegistrationMutex);

  // libgcc expects a null-terminated list of FDEs: write 4 zero bytes in the
  // end of the allocated section.
  auto* terminator = reinterpret_cast<uint32_t*>(addr + size);
  *terminator = 0;

  __register_frame(addr);
  registered_frames_.push_back(addr);
}

void JITFrameManager::deregisterEHFrames() {
  return deregisterEHFramesImpl();
}

void JITFrameManager::deregisterEHFramesImpl() {
  lock_guard guard(kRegistrationMutex);
  for (auto it = registered_frames_.rbegin(); it != registered_frames_.rend(); ++it) {
    __deregister_frame(*it);
  }
  registered_frames_.clear();
}

} // namespace codegen
} // namespace kudu
