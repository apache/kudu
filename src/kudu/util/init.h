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

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

// Return a NotSupported Status if the current CPU does not support the CPU flags
// required for Kudu.
Status CheckCPUFlags() WARN_UNUSED_RESULT;

// Initialize Kudu, checking that the platform we are running on is supported,
// etc. Returns non-OK status if we fail to init. Calls abort() if it turns out
// that at least one of the standard descriptors is not open.
Status InitKudu() WARN_UNUSED_RESULT;

} // namespace kudu
