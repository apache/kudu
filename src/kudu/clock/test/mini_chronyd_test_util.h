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

// Reserve a port and start chronyd, outputting the result chronyd object
// wrapped into std::unique_ptr smart pointer. The 'options' parameter is
// in-out: the bound port and address are output into the 'port' and
// 'bindaddress' fields correspondingly. All other fields of the 'options'
// parameter are untouched, and the only field affective the port reservation
// process is the 'index' field.
Status StartChronydAtAutoReservedPort(
    std::unique_ptr<MiniChronyd>* chronyd,
    MiniChronydOptions* options = nullptr) WARN_UNUSED_RESULT;

} // namespace clock
} // namespace kudu
