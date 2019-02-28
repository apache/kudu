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

#include "kudu/master/location_cache.h"

#include <cstdio>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/charset.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/trace.h"

METRIC_DEFINE_counter(server, location_mapping_cache_hits,
                      "Location Mapping Cache Hits",
                      kudu::MetricUnit::kCacheHits,
                      "Number of times location mapping assignment used "
                      "cached data");
METRIC_DEFINE_counter(server, location_mapping_cache_queries,
                      "Location Mapping Cache Queries",
                      kudu::MetricUnit::kCacheQueries,
                      "Number of queries to the location mapping cache");

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

namespace {
// Returns if 'location' is a valid location string, i.e. it begins with /
// and consists of /-separated tokens each of which contains only characters
// from the set [a-zA-Z0-9_-.].
bool IsValidLocation(const string& location) {
  if (location.empty() || location[0] != '/') {
    return false;
  }
  const strings::CharSet charset("abcdefghijklmnopqrstuvwxyz"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "0123456789"
                                 "_-./");
  for (const auto c : location) {
    if (!charset.Test(c)) {
      return false;
    }
  }
  return true;
}
} // anonymous namespace

LocationCache::LocationCache(string location_mapping_cmd,
                             MetricEntity* metric_entity)
    : location_mapping_cmd_(std::move(location_mapping_cmd)) {
  if (metric_entity != nullptr) {
    location_mapping_cache_hits_ = metric_entity->FindOrCreateCounter(
          &METRIC_location_mapping_cache_hits);
    location_mapping_cache_queries_ = metric_entity->FindOrCreateCounter(
          &METRIC_location_mapping_cache_queries);
  }
}

Status LocationCache::GetLocation(const string& key, string* location) {
  if (PREDICT_TRUE(location_mapping_cache_queries_)) {
    location_mapping_cache_queries_->Increment();
  }
  {
    // First check whether the location for the key has already been assigned.
    shared_lock<rw_spinlock> l(location_map_lock_);
    const auto* value_ptr = FindOrNull(location_map_, key);
    if (value_ptr) {
      DCHECK(!value_ptr->empty());
      *location = *value_ptr;
      if (PREDICT_TRUE(location_mapping_cache_hits_)) {
        location_mapping_cache_hits_->Increment();
      }
      return Status::OK();
    }
  }
  string value;
  TRACE(Substitute("key $0: assigning location", key));
  Status s = GetLocationFromLocationMappingCmd(
      location_mapping_cmd_, key, &value);
  TRACE(Substitute("key $0: assigned location '$1'", key, value));
  if (s.ok()) {
    CHECK(!value.empty());
    std::lock_guard<rw_spinlock> l(location_map_lock_);
    // This simple implementation doesn't protect against multiple runs of the
    // location-mapping command for the same key.
    InsertIfNotPresent(&location_map_, key, value);
    *location = value;
  }
  return s;
}

Status LocationCache::GetLocationFromLocationMappingCmd(const string& cmd,
                                                        const string& key,
                                                        string* location) {
  DCHECK(location);
  vector<string> argv = strings::Split(cmd, " ", strings::SkipEmpty());
  if (argv.empty()) {
    return Status::RuntimeError("invalid empty location mapping command");
  }
  argv.push_back(key);
  string stderr, location_temp;
  Status s = Subprocess::Call(argv, /*stdin_in=*/"", &location_temp, &stderr);
  if (!s.ok()) {
    return Status::RuntimeError(
        Substitute("failed to run location mapping command: $0", s.ToString()),
        stderr);
  }
  StripWhiteSpace(&location_temp);
  // Special case an empty location for a better error.
  if (location_temp.empty()) {
    return Status::RuntimeError(
        "location mapping command returned invalid empty location");
  }
  if (!IsValidLocation(location_temp)) {
    return Status::RuntimeError(
        "location mapping command returned invalid location",
        location_temp);
  }
  *location = std::move(location_temp);
  return Status::OK();
}

} // namespace master
} // namespace kudu
