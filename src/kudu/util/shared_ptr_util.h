// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_SHARED_PTR_UTIL_H_
#define KUDU_UTIL_SHARED_PTR_UTIL_H_

#include <cstddef>
#include <tr1/memory>

namespace kudu {

// This is needed on TR1. With C++11, std::hash<std::shared_ptr<>> is provided.
template <class T>
struct SharedPtrHashFunctor {
  std::size_t operator()(const std::tr1::shared_ptr<T>& key) const {
    return reinterpret_cast<std::size_t>(key.get());
  }
};

} // namespace kudu

#endif // KUDU_UTIL_SHARED_PTR_UTIL_H_
