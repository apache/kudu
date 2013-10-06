// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Utilities for dealing with protocol buffers.
// These are mostly just functions similar to what are found in the protobuf
// library itself, but using kudu::faststring instances instead of STL strings.
#ifndef KUDU_UTIL_PB_UTIL_H
#define KUDU_UTIL_PB_UTIL_H

#include "util/faststring.h"

namespace google { namespace protobuf {
class MessageLite;
}
}

namespace kudu {

class SequentialFile;
class WritableFile;
class Status;

namespace pb_util {

using google::protobuf::MessageLite;

// See MessageLite::AppendToString
bool AppendToString(const MessageLite &msg, faststring *output);

// See MessageLite::AppendPartialToString
bool AppendPartialToString(const MessageLite &msg, faststring *output);

// See MessageLite::SerializeToString.
bool SerializeToString(const MessageLite &msg, faststring *output);

// See MessageLite::ParseFromZeroCopyStream
// TODO: change this to return Status - differentiate IO error from bad PB
bool ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile);

// Similar to MessageLite::ParseFromArray, with the difference that it returns
// Status::Corruption() if the message could not be parsed.
Status ParseFromArray(MessageLite* msg, const uint8_t* data, uint32_t length);

// See MessageLite::SerializeToZeroCopyStream.
bool SerializeToWritableFile(const MessageLite& msg, WritableFile *wfile);

} // namespace pb_util
} // namespace kudu
#endif
