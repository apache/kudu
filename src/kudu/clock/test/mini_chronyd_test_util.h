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

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

class MiniChronyd;
struct MiniChronydOptions;

// Reserve a port and start chronyd NTP server using MiniChronyd with
// MiniChronydOptions options specified by 'options' parameter. The only field
// affecting the port reservation process is 'MiniChronydOptions::index' field.
// The result MiniChronyd object is wrapped into std::unique_ptr smart pointer
// and output into the 'chronyd' out parameter.
Status StartChronydAtAutoReservedPort(
    MiniChronydOptions options,
    std::unique_ptr<MiniChronyd>* chronyd) WARN_UNUSED_RESULT;

} // namespace clock
} // namespace kudu
