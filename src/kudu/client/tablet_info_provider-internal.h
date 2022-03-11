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

#include <string>

#include "kudu/util/status.h"

namespace kudu {

class Partition;

namespace client {

class KuduClient;

namespace internal {

// This is a class whose sole responsibility is to provide access to the
// client's metacache to fetch various information on tablets.
//
// Having this single class reduces cruft in friend class declarations in the
// client.h file.
class TabletInfoProvider {
 public:
  // Provides partition information for the tablet with the specified
  // identifier.
  static Status GetPartitionInfo(KuduClient* client,
                                 const std::string& tablet_id,
                                 Partition* partition);
};

} // namespace internal
} // namespace client
} // namespace kudu
