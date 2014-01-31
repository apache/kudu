// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "common/wire_protocol.h"
#include "server/metadata.pb.h"
#include "server/metadata_util.h"

namespace kudu {
namespace metadata {

void BlockIdToPB(const BlockId& block_id, BlockIdPB *pb) {
  pb->set_id(block_id.ToString());
}

BlockId BlockIdFromPB(const BlockIdPB& pb) {
  return BlockId(pb.id());
}

} // namespace metadata
} // namespace kudu

