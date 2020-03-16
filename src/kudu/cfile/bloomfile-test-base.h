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
#include <memory>

#include "kudu/cfile/bloomfile.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace cfile {

class BloomFileTestBase : public KuduTest {
 public:
  static const int kKeyShift = 2;

  void SetUp() override;

  // Creates a test bloomfile on disk. The block ID is written to block_id_.
  void WriteTestBloomFile();

  // Opens the bloomfile with block id block_id_ for reading.
  // WriteTestBloomFile() must have been called. The reader is written to bfr_.
  Status OpenBloomFile();

  // Uses the bloomfile reader in bfr_ to run a simple query benchmark.
  //
  // Returns the number of queries that were a hit in the bloom filter.
  uint64_t ReadBenchmark();

  FsManager* fs_manager() const { return fs_manager_.get(); }
  BloomFileReader* bfr() const { return bfr_.get(); }
  BlockId block_id() const { return block_id_; }
 private:

  // Appends FLAG_n_keys keys to 'bfw'.
  static void AppendBlooms(BloomFileWriter* bfw);

  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<BloomFileReader> bfr_;
  BlockId block_id_;
};

} // namespace cfile
} // namespace kudu

