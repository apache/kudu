// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Utilities for dealing with protocol buffers.
// These are mostly just functions similar to what are found in the protobuf
// library itself, but using kudu::faststring instances instead of STL strings.
#ifndef KUDU_UTIL_PB_UTIL_H
#define KUDU_UTIL_PB_UTIL_H

#include <string>
#include "kudu/util/faststring.h"

namespace google { namespace protobuf {
class MessageLite;
class Message;
}
}

namespace kudu {

class Env;
class SequentialFile;
class Status;
class WritableFile;

namespace pb_util {

using google::protobuf::MessageLite;

enum SyncMode {
  SYNC,
  NO_SYNC
};

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

// Load a protobuf from the given path.
Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg);

// Serialize a protobuf to the given path.
//
// If SyncMode SYNC is provided, ensures the changes are made durable.
Status WritePBToPath(Env* env, const std::string& path, const MessageLite& msg, SyncMode sync);

// Truncate any 'bytes' or 'string' fields of this message to max_len.
// The text "<truncated>" is appended to any such truncated fields.
void TruncateFields(google::protobuf::Message* message, int max_len);

} // namespace pb_util
} // namespace kudu
#endif
