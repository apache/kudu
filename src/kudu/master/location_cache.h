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

#include <string>
#include <unordered_map>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {
namespace master {

// A primitive cache of unlimited capacity to store assigned locations for
// a key. The cache entries are kept in the cache for the lifetime of the cache
// itself.
class LocationCache {
 public:
  // The location assignment command is specified by the 'location_mapping_cmd'
  // parameter (the command might be a script or an executable). The
  // 'metric_entity' is used to register standard cache counters: total number
  // of queries and number of cache hits during the cache's lifetime.
  explicit LocationCache(std::string location_mapping_cmd,
                         MetricEntity* metric_entity = nullptr);
  ~LocationCache() = default;

  // Get the location for the specified key. The key is treated as an opaque
  // identifier.
  //
  // If no cached location is found, the location mapping command is run,
  // caching the result for the lifetime of the cache.
  //
  // This method returns an error if there was an issue running the location
  // assignment command.
  Status GetLocation(const std::string& key, std::string* location);

 private:
  // Resolves an opaque 'key' into a location using the command 'cmd'.
  // The result will be stored in 'location', which must not be null. If there
  // is an error running the command or the output is invalid, an error Status
  // will be returned.
  //
  // TODO(wdberkeley): Eventually we may want to get multiple locations at once
  // by giving the location mapping command multiple arguments (like Hadoop).
  static Status GetLocationFromLocationMappingCmd(const std::string& cmd,
                                                  const std::string& key,
                                                  std::string* location);

  // The executable to run when assigning locations for keys which are not yet
  // in the cache.
  const std::string location_mapping_cmd_;

  // Counter to track cache hits, i.e. when it was not necessary to run
  // the location assignment command.
  scoped_refptr<Counter> location_mapping_cache_hits_;

  // Counter to track overall cache queries, i.e. hits plus misses. Every miss
  // results in the location assignment command being run.
  scoped_refptr<Counter> location_mapping_cache_queries_;

  // Spinlock to protect the location assignment map (location_map_).
  rw_spinlock location_map_lock_;

  // The location assignment map: dictionary of key --> location.
  std::unordered_map<std::string, std::string> location_map_;

  DISALLOW_COPY_AND_ASSIGN(LocationCache);
};

} // namespace master
} // namespace kudu
