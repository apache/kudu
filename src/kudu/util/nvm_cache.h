// Copyright (c) 2015 Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_NVM_CACHE_H_
#define KUDU_UTIL_NVM_CACHE_H_

#include <string>

namespace kudu {
class Cache;

// Create a cache in persistent memory with the given capacity.
Cache* NewLRUNvmCache(size_t capacity, const std::string& id);

}  // namespace kudu

#endif
