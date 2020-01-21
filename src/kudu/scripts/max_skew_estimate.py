#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# The purpose of this script is to estimate the distribution of the maximum
# skew produced by Kudu's "power of two choices" placement algorithm,
# which is used to place replicas on tablet servers (at least in Kudu <= 1.7).
import math
import random
import sys
try:
    xrange  # For Python 2
except NameError:
    xrange = range  # For Python 3

# Replicates Random::ReservoirSample from kudu/util/random.h.
def reservoir_sample(n, sample_size, avoid):
    result = list()
    k = 0
    for i in xrange(n):
        if i in avoid:
            continue
        k += 1
        if len(result) < sample_size:
            result.append(i)
            continue
        j = random.randrange(k)
        if j < sample_size:
            result[j] = i
    return result

# Follows CatalogManager::SelectReplica, which implements the power of two
# choices selection algorithm, except we assume we always have a placement.
def select_replica(num_servers, avoid, counts):
  two_choices = reservoir_sample(num_servers, 2, avoid)
  assert(len(two_choices) > 0)
  assert(len(two_choices) <= 2)
  if len(two_choices) == 1:
      return two_choices[0]
  else:
      a, b = two_choices[0], two_choices[1]
      if counts[a] < counts[b]:
          return a
      else:
          return b

# Quickly cribbed from https://stackoverflow.com/a/15589202.
# 'data' must be sorted.
def percentile(data, percentile):
    size = len(data)
    return data[int(math.ceil((size * percentile) / 100)) - 1]

def generate_max_skew(num_servers, num_tablets, rf):
    counts = {i : 0 for i in xrange(num_servers)}
    for t in xrange(num_tablets):
        avoid = set()
        for r in range(rf):
            replica = select_replica(num_servers, avoid, counts)
            avoid.add(replica)
            counts[replica] += 1
    return max(counts.values()) - min(counts.values())

def main():
    args = sys.argv
    if len(args) != 5:
        print("max_skew_estimate.py <num trials> <num servers> <num_tablets> <repl factor>")
        sys.exit(1)
    num_trials, num_servers, num_tablets, rf = int(args[1]), int(args[2]), int(args[3]), int(args[4])
    skews = [generate_max_skew(num_servers, num_tablets, rf) for _ in xrange(num_trials)]
    skews.sort()
    for p in [5, 25, 50, 75, 99]:
        print("{0:02d} percentile: {1:d}".format(p, percentile(skews, p)))

if __name__ == "__main__":
    main()

