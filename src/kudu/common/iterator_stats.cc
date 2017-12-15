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

#include "kudu/common/iterator_stats.h"

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"

namespace kudu {

using std::string;
using strings::Substitute;

IteratorStats::IteratorStats()
    : cells_read(0),
      bytes_read(0),
      cblocks_read(0) {
}

string IteratorStats::ToString() const {
  return Substitute("cells_read=$0 bytes_read=$1 cblocks_read=$2",
                    cells_read, bytes_read, cblocks_read);
}

IteratorStats& IteratorStats::operator+=(const IteratorStats& other) {
  cells_read += other.cells_read;
  bytes_read += other.bytes_read;
  cblocks_read += other.cblocks_read;
  DCheckNonNegative();
  return *this;
}

IteratorStats& IteratorStats::operator-=(const IteratorStats& other) {
  cells_read -= other.cells_read;
  bytes_read -= other.bytes_read;
  cblocks_read -= other.cblocks_read;
  DCheckNonNegative();
  return *this;
}

IteratorStats IteratorStats::operator+(const IteratorStats& other) {
  IteratorStats copy = *this;
  copy += other;
  return copy;
}

IteratorStats IteratorStats::operator-(const IteratorStats& other) {
  IteratorStats copy = *this;
  copy -= other;
  return copy;
}

void IteratorStats::DCheckNonNegative() const {
  DCHECK_GE(cells_read, 0);
  DCHECK_GE(bytes_read, 0);
  DCHECK_GE(cblocks_read, 0);
}
} // namespace kudu
