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

#include <cstdint>

namespace kudu {

// Extract bits from 'a' corresponding to 'mask'.
//
// This is a software implementation of the 'PEXT' instruction
// from the BMI2 instruction set.
//
// This implementation uses the CLMUL instruction set. Callers should
// verify that the instruction is present (eg using base::CPU) before
// calling.
uint64_t zp7_pext_64_clmul(uint64_t a, uint64_t mask);

// This implementation is slower but doesn't require any special instructions.
uint64_t zp7_pext_64_simple(uint64_t a, uint64_t mask);

} // namespace kudu
