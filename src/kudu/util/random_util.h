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

#include <cstdlib>
#include <stdint.h>

#include <set>
#include <string>
#include <vector>

namespace kudu {

class Random;

// Writes exactly n random bytes to dest using the parameter Random generator.
// Note RandomString() does not null-terminate its strings, though '\0' could
// be written to dest with the same probability as any other byte.
void RandomString(void* dest, size_t n, Random* rng);

// Same as the above, but returns the string.
std::string RandomString(size_t n, Random* rng);

// Generate a 32-bit random seed from several sources, including timestamp,
// pid & tid.
uint32_t GetRandomSeed32();

// Returns a randomly-selected element from the container.
template <typename Container, typename T, typename Rand>
T SelectRandomElement(const Container& c, Rand* r) {
  CHECK(!c.empty());
  std::vector<T> rand_list;
  ReservoirSample(c, 1, std::set<T>{}, r, &rand_list);
  return rand_list[0];
}

// Returns a randomly-selected subset from the container.
template <typename Container, typename T, typename Rand>
std::vector<T> SelectRandomSubset(const Container& c, int min_to_return, Rand* r) {
  CHECK_GT(c.size(), min_to_return);
  int num_to_return = min_to_return + r->Uniform(c.size() - min_to_return);
  std::vector<T> rand_list;
  ReservoirSample(c, num_to_return, std::set<T>{}, r, &rand_list);
  return rand_list;
}

// Sample 'k' random elements from the collection 'c' into 'result', taking
// care not to sample any elements that are already present in 'avoid'.
//
// In the case that 'c' has fewer than 'k' elements then all elements in 'c'
// will be selected.
//
// 'c' should be an iterable STL collection such as a vector, set, or list.
// 'avoid' should be an STL-compatible set.
//
// The results are not stored in a randomized order: the order of results will
// match their order in the input collection.
template<class Collection, class Set, class T, typename Rand>
void ReservoirSample(const Collection& c, int k, const Set& avoid,
                     Rand* r, std::vector<T>* result) {
  result->clear();
  result->reserve(k);
  int i = 0;
  for (const T& elem : c) {
    if (ContainsKey(avoid, elem)) {
      continue;
    }
    i++;
    // Fill the reservoir if there is available space.
    if (result->size() < k) {
      result->push_back(elem);
      continue;
    }
    // Otherwise replace existing elements with decreasing probability.
    int j = r->Uniform(i);
    if (j < k) {
      (*result)[j] = elem;
    }
  }
}

} // namespace kudu

