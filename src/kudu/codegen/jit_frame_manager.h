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
#include <deque>
#include <mutex>

#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include <llvm/ADT/StringRef.h>

namespace kudu {
namespace codegen {

class JITFrameManager : public llvm::SectionMemoryManager {
 public:
  JITFrameManager() = default;
  ~JITFrameManager() override;

  // Override to add space for the 4-byte null terminator.
  uint8_t* allocateCodeSection(uintptr_t size,
                               unsigned alignment,
                               unsigned section_id,
                               llvm::StringRef section_name) override;

  void registerEHFrames(uint8_t* addr, uint64_t load_addr, size_t size) override;
  void deregisterEHFrames() override;

 private:
  void deregisterEHFramesImpl();

  // Mutex to prevent races in libgcc/libunwind. Since it should work across
  // multiple instances, it's a static one.
  static std::mutex kRegistrationMutex;

  // Container to keep track of registered frames: this information is necessary
  // for unregistring all of them.
  std::deque<uint8_t*> registered_frames_;
};

} // namespace codegen
} // namespace kudu
