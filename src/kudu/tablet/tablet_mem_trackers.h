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

#include <memory>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/mem_tracker.h"

namespace kudu {
namespace tablet {

struct TabletMemTrackers {

  // Intended for unit tests where the tracker hierarchy doesn't matter.
  TabletMemTrackers()
    : tablet_tracker(MemTracker::GetRootTracker()),
      bloomfile_tracker(MemTracker::GetRootTracker()),
      cfile_reader_tracker(MemTracker::GetRootTracker()),
      dms_tracker(MemTracker::GetRootTracker()) {
  }

  TabletMemTrackers(std::string tablet_id,
                    std::shared_ptr<MemTracker> parent_mem_tracker)
    : tablet_tracker(MemTracker::CreateTracker(
        -1,
        strings::Substitute("tablet-$0", tablet_id),
        std::move(parent_mem_tracker))),
      bloomfile_tracker(MemTracker::CreateTracker(-1, "BloomFileReaders", tablet_tracker)),
      cfile_reader_tracker(MemTracker::CreateTracker(-1, "CFileReaders", tablet_tracker)),
      dms_tracker(MemTracker::CreateTracker(-1, "DeltaMemStores", tablet_tracker)) {
  }

  std::shared_ptr<MemTracker> tablet_tracker;

  // All of the below are children of tablet_tracker;
  std::shared_ptr<MemTracker> bloomfile_tracker;
  std::shared_ptr<MemTracker> cfile_reader_tracker;
  std::shared_ptr<MemTracker> dms_tracker;
};

} // namespace tablet
} // namespace kudu
