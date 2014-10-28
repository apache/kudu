// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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

// Load a protobuf from the given path.
Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg);

// Serialize a protobuf to the given path.
//
// If SyncMode SYNC is provided, ensures the changes are made durable.
Status WritePBToPath(Env* env, const std::string& path, const MessageLite& msg, SyncMode sync);

// Truncate any 'bytes' or 'string' fields of this message to max_len.
// The text "<truncated>" is appended to any such truncated fields.
void TruncateFields(google::protobuf::Message* message, int max_len);

// Functions to read / write a "containerized" protobuf.
//
// A protobuf "container" has the following format (all integers in
// little-endian byte order):
//
//
//
// magic number: 8 byte string identifying the file format.
//
//               Included so that we have a minimal guarantee that this file is
//               of the type we expect and that we are not just reading garbage.
//
// container_version: 4 byte unsigned integer indicating the "version" of the
//                    container format. Must be set to 1 at this time.
//
//                    Included so that this file format may be extended at some
//                    later date while maintaining backwards compatibility.
//
// data size: 4 byte unsigned integer indicating the size of the encoded data.
//
//            Included so that it is easy to allocate memory all at once to hold
//            the data portion, as well as arbitrarily define a point at which
//            it would be too much memory to allocate to read the file. In such
//            a scenario, it may make sense to simply fail and refuse to read
//            the file.
//
// data: "size" bytes of protobuf data encoded according to the schema.
//
//       Our payload.
//
// checksum: 4 byte unsigned integer containing the CRC32C checksum of the
//           entire contents of the container file up to and including the last
//           byte of "data".
//
//           Included to ensure validity of the data on-disk.
//

// Load a "containerized" protobuf, including magic number, size, and checksum,
// from the given path.
Status ReadPBContainerFromPath(Env* env, const std::string& path,
                               const char* magic, MessageLite* msg);

// Serialize a "containerized" protobuf, including magic number, size, and
// checksum, to the given path.
Status WritePBContainerToPath(Env* env, const std::string& path,
                              const char* magic, const MessageLite& msg,
                              SyncMode sync);

} // namespace pb_util
} // namespace kudu
#endif
