// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_FS_BLOCK_ID_H
#define KUDU_FS_BLOCK_ID_H

#include <iosfwd>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/stringprintf.h"

namespace kudu {

class BlockIdPB;

namespace fs {
namespace internal {
class FileBlockLocation;
} // namespace internal
} // namespace fs

class BlockId {
 public:
  BlockId()
    : id_(kInvalidId) {
  }

  explicit BlockId(uint64_t id) {
    SetId(id);
  }

  void SetId(uint64_t id) {
    id_ = id;
  }

  bool IsNull() const { return id_ == kInvalidId; }

  std::string ToString() const {
    return StringPrintf("%016lx", id_);
  }

  bool operator==(const BlockId& other) const {
    return id_ == other.id_;
  }

  bool operator!=(const BlockId& other) const {
    return id_ != other.id_;
  }

  // Returns the raw ID. Use with care; in most cases the BlockId should be
  // treated as a completely opaque value.
  uint64_t id() const { return id_; }

  // Join the given block IDs with ','. Useful for debug printouts.
  static std::string JoinStrings(const std::vector<BlockId>& blocks);

  void CopyToPB(BlockIdPB* pb) const;

  static BlockId FromPB(const BlockIdPB& pb);

 private:
  static const uint64_t kInvalidId;

  uint64_t id_;
};

std::ostream& operator<<(std::ostream& o, const BlockId& block_id);

struct BlockIdHash {
  size_t operator()(const BlockId& block_id) const {
    return block_id.id();
  }
};

struct BlockIdCompare {
  bool operator()(const BlockId& first, const BlockId& second) const {
    return first.id() < second.id();
  }
};

struct BlockIdEqual {
  bool operator()(const BlockId& first, const BlockId& second) const {
    return first.id() == second.id();
  }
};

} // namespace kudu
#endif /* KUDU_FS_BLOCK_ID_H */
