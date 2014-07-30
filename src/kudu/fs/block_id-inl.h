// Copyright (c) 2013, Cloudera, inc.
//
// Inline implementations of BlockId methods which are rarely
// used.
#ifndef KUDU_FS_BLOCK_ID_INL_H
#define KUDU_FS_BLOCK_ID_INL_H

#include <string>

#include "kudu/gutil/strtoint.h"

namespace kudu {

inline std::string BlockId::hash0() const { return id_.substr(0, 2); }
inline std::string BlockId::hash1() const { return id_.substr(2, 2); }
inline std::string BlockId::hash2() const { return id_.substr(4, 2); }
inline std::string BlockId::hash3() const { return id_.substr(6, 2); }

inline size_t BlockId::hash() const {
  return (strto32(hash0().c_str(), NULL, 16) << 24) +
    (strto32(hash1().c_str(), NULL, 16) << 16) +
    (strto32(hash2().c_str(), NULL, 16) << 8) +
    strto32(hash3().c_str(), NULL, 16);
}

} // namespace kudu
#endif /* KUDU_FS_BLOCK_ID-INL_H */
