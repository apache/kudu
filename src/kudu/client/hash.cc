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

#include "kudu/client/hash.h"

#include <ostream>

#include <glog/logging.h>

#include "kudu/client/hash-internal.h"
#include "kudu/util/hash.pb.h"

namespace kudu {
namespace client {

kudu::HashAlgorithm ToInternalHashAlgorithm(HashAlgorithm hash_algorithm) {
  switch (hash_algorithm) {
    case UNKNOWN_HASH: return kudu::UNKNOWN_HASH;
    case MURMUR_HASH_2: return kudu::MURMUR_HASH_2;
    case CITY_HASH: return kudu::CITY_HASH;
    case FAST_HASH: return kudu::FAST_HASH;
    default: LOG(FATAL)  << "Unexpected hash algorithm: " << hash_algorithm;
  }
}

HashAlgorithm FromInternalHashAlgorithm(kudu::HashAlgorithm hash_algorithm) {
  switch (hash_algorithm) {
    case kudu::UNKNOWN_HASH: return UNKNOWN_HASH;
    case kudu::MURMUR_HASH_2: return MURMUR_HASH_2;
    case kudu::CITY_HASH: return CITY_HASH;
    case kudu::FAST_HASH: return FAST_HASH;
    default: LOG(FATAL)  << "Unexpected hash algorithm: " << hash_algorithm;
  }
}

} // namespace client
} // namespace kudu
