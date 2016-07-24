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

// Test-related utility methods that can be called from non-test
// code. This module is part of the 'util' module and is built into
// all binaries, not just tests, whereas 'test_util.cc' is linked
// only into test binaries.

#pragma once

namespace kudu {

// Return true if the current binary is a gtest. More specifically,
// returns true if the 'test_util.cc' module has been linked in
// (either dynamically or statically) to the running process.
bool IsGTest();

} // namespace kudu
