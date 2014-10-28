// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_RPC_RESPONSE_CALLBACK_H
#define KUDU_RPC_RESPONSE_CALLBACK_H

#include <boost/function.hpp>

namespace kudu {
namespace rpc {

typedef boost::function<void()> ResponseCallback;

}
}

#endif
