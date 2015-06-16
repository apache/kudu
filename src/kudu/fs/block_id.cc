// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/block_id.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/join.h"

using std::string;
using std::vector;

namespace kudu {

const uint64_t BlockId::kInvalidId = 0;

string BlockId::JoinStrings(const vector<BlockId>& blocks) {
  vector<string> strings;
  strings.reserve(blocks.size());
  BOOST_FOREACH(const BlockId& block, blocks) {
    strings.push_back(block.ToString());
  }
  return ::JoinStrings(strings, ",");
}

void BlockId::CopyToPB(BlockIdPB *pb) const {
  pb->set_id(id_);
}

BlockId BlockId::FromPB(const BlockIdPB& pb) {
  DCHECK(pb.has_id());
  return BlockId(pb.id());
}

} // namespace kudu
