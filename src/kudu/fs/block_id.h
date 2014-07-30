// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_FS_BLOCK_ID_H
#define KUDU_FS_BLOCK_ID_H

#include <iosfwd>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"

namespace kudu {

class BlockIdPB;

class BlockId {
 public:
  BlockId() {}
  explicit BlockId(const std::string& id) { SetId(id); }

  void SetId(const std::string& id) {
    CHECK_GE(id.size(), 8);
    id_ = id;
  }

  bool IsNull() const { return id_.empty(); }
  const std::string& ToString() const { return id_; }

  bool operator==(const BlockId& other) const {
    return id_ == other.id_;
  }
  bool operator!=(const BlockId& other) const {
    return id_ != other.id_;
  }

  // Join the given block IDs with ','. Useful for debug printouts.
  static std::string JoinStrings(const std::vector<BlockId>& blocks);

  void CopyToPB(BlockIdPB* pb) const;
  static BlockId FromPB(const BlockIdPB& pb);

 private:
  friend class FsManager;
  friend class BlockIdHash;

  // Used for on-disk partition
  std::string hash0() const;
  std::string hash1() const;
  std::string hash2() const;
  std::string hash3() const;

  size_t hash() const;

  std::string id_;
};

std::ostream& operator<<(std::ostream& o, const BlockId& block_id);

struct BlockIdHash {
  size_t operator()(const BlockId& block_id) const {
    return block_id.hash();
  }
};

} // namespace kudu
#endif /* KUDU_FS_BLOCK_ID_H */
