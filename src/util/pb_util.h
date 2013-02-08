// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_PB_UTIL_H
#define KUDU_UTIL_PB_UTIL_H

#include "util/faststring.h"

namespace google { namespace protobuf {
class MessageLite;
}
}


namespace kudu {
namespace pb_util {

using google::protobuf::MessageLite;

// See MessageLite::AppendToString
bool AppendToString(const MessageLite &msg, faststring *output);

// See MessageLite::AppendPartialToString
bool AppendPartialToString(const MessageLite &msg, faststring *output);

bool SerializeToString(const MessageLite &msg, faststring *output);

}
}

#endif
