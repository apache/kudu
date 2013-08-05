// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#ifndef KUDU_RPC_RPC_CONSTANTS_H
#define KUDU_RPC_RPC_CONSTANTS_H

#include <stdint.h>

namespace kudu {
namespace rpc {

// There is a 4-byte length prefix before any packet.
static const uint8_t kMsgLengthPrefixLength = 4;

} // namespace rpc
} // namespace kudu

#endif // KUDU_RPC_RPC_CONSTANTS_H
