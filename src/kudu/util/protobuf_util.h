// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_PROTOBUF_UTIL_H
#define KUDU_UTIL_PROTOBUF_UTIL_H

#include <google/protobuf/message_lite.h>

namespace kudu {

bool AppendPBToString(const google::protobuf::MessageLite &msg, faststring *output) {
  int old_size = output->size();
  int byte_size = msg.ByteSize();
  output->resize(old_size + byte_size);
  uint8* start = reinterpret_cast<uint8*>(output->data() + old_size);
  uint8* end = msg.SerializeWithCachedSizesToArray(start);
  CHECK(end - start == byte_size)
    << "Error in serialization. byte_size=" << byte_size
    << " new ByteSize()=" << msg.ByteSize()
    << " end-start=" << (end-start);
  return true;
}

} // namespace kudu

#endif
