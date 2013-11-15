// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Some portions copyright (C) 2008, Google, inc.
//
// Utilities for working with protobufs.
// Some of this code is cribbed from the protobuf source,
// but modified to work with kudu's 'faststring' instead of STL strings.

#include "util/pb_util.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/message.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "util/pb_util-internal.h"
#include "util/status.h"
#include "util/env.h"
#include "util/env_util.h"

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::Reflection;
using std::string;
using std::tr1::shared_ptr;

static const char* const kTmpSuffix = ".tmp";

namespace kudu {
namespace pb_util {

namespace {

// When serializing, we first compute the byte size, then serialize the message.
// If serialization produces a different number of bytes than expected, we
// call this function, which crashes.  The problem could be due to a bug in the
// protobuf implementation but is more likely caused by concurrent modification
// of the message.  This function attempts to distinguish between the two and
// provide a useful error message.
void ByteSizeConsistencyError(int byte_size_before_serialization,
                              int byte_size_after_serialization,
                              int bytes_produced_by_serialization) {
  CHECK_EQ(byte_size_before_serialization, byte_size_after_serialization)
      << "Protocol message was modified concurrently during serialization.";
  CHECK_EQ(bytes_produced_by_serialization, byte_size_before_serialization)
      << "Byte size calculation and serialization were inconsistent.  This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of the message.";
  LOG(FATAL) << "This shouldn't be called if all the sizes are equal.";
}

string InitializationErrorMessage(const char* action,
                                  const MessageLite& message) {
  // Note:  We want to avoid depending on strutil in the lite library, otherwise
  //   we'd use:
  //
  // return strings::Substitute(
  //   "Can't $0 message of type \"$1\" because it is missing required "
  //   "fields: $2",
  //   action, message.GetTypeName(),
  //   message.InitializationErrorString());

  string result;
  result += "Can't ";
  result += action;
  result += " message of type \"";
  result += message.GetTypeName();
  result += "\" because it is missing required fields: ";
  result += message.InitializationErrorString();
  return result;
}

} // anonymous namespace

bool AppendToString(const MessageLite &msg, faststring *output) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  return AppendPartialToString(msg, output);
}

bool AppendPartialToString(const MessageLite &msg, faststring* output) {
  int old_size = output->size();
  int byte_size = msg.ByteSize();

  output->resize(old_size + byte_size);

  uint8* start = &((*output)[old_size]);
  uint8* end = msg.SerializeWithCachedSizesToArray(start);
  if (end - start != byte_size) {
    ByteSizeConsistencyError(byte_size, msg.ByteSize(), end - start);
  }
  return true;
}

bool SerializeToString(const MessageLite &msg, faststring *output) {
  output->clear();
  return AppendToString(msg, output);
}

bool ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile) {
  SequentialFileFileInputStream istream(rfile);
  return msg->ParseFromZeroCopyStream(&istream);
}

Status ParseFromArray(MessageLite* msg, const uint8_t* data, uint32_t length) {
  if (!msg->ParseFromArray(data, length)) {
    return Status::Corruption("Error parsing msg", InitializationErrorMessage("parse", *msg));
  }
  return Status::OK();
}

bool SerializeToWritableFile(const MessageLite& msg, WritableFile *wfile) {
  WritableFileOutputStream ostream(wfile);
  bool res = msg.SerializeToZeroCopyStream(&ostream);
  return res && ostream.Flush();
}

Status WritePBToPath(Env* env, const std::string& path, const MessageLite& msg) {
  const string path_tmp = path + kTmpSuffix;

  shared_ptr<WritableFile> file;
  RETURN_NOT_OK_PREPEND(env_util::OpenFileForWrite(env, path_tmp, &file),
                        "Couldn't open master block file in " + path_tmp);
  env_util::ScopedFileDeleter tmp_deleter(env, path_tmp);

  if (!SerializeToWritableFile(msg, file.get())) {
    return Status::IOError("Failed to serialize to file");
  }
  RETURN_NOT_OK_PREPEND(file->Flush(), "Failed to Flush() " + path_tmp);
  RETURN_NOT_OK_PREPEND(file->Sync(), "Failed to Sync() " + path_tmp);
  RETURN_NOT_OK_PREPEND(file->Close(), "Failed to Close() " + path_tmp);
  RETURN_NOT_OK_PREPEND(env->RenameFile(path_tmp, path), "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  return Status::OK();
}

Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg) {
  shared_ptr<SequentialFile> rfile;
  RETURN_NOT_OK(env_util::OpenFileForSequential(env, path, &rfile));
  if (!ParseFromSequentialFile(msg, rfile.get())) {
    return Status::IOError("Unable to parse PB from path", path);
  }
  return Status::OK();
}

static void TruncateString(string* s, int max_len) {
  if (s->size() > max_len) {
    s->resize(max_len);
    s->append("<truncated>");
  }
}

void TruncateFields(Message* message, int max_len) {
  const Reflection* reflection = message->GetReflection();
  vector<const FieldDescriptor*> fields;
  reflection->ListFields(*message, &fields);
  BOOST_FOREACH(const FieldDescriptor* field, fields) {
    if (field->is_repeated()) {
      for (int i = 0; i < reflection->FieldSize(*message, field); i++) {
        switch (field->cpp_type()) {
          case FieldDescriptor::CPPTYPE_STRING: {
            const string& s_const = reflection->GetRepeatedStringReference(*message, field, i, NULL);
            TruncateString(const_cast<string*>(&s_const), max_len);
            break;
          }
          case FieldDescriptor::CPPTYPE_MESSAGE: {
            TruncateFields(reflection->MutableRepeatedMessage(message, field, i), max_len);
            break;
          }
          default:
            break;
        }
      }
    } else {
      switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_STRING: {
          const string& s_const = reflection->GetStringReference(*message, field, NULL);
          TruncateString(const_cast<string*>(&s_const), max_len);
          break;
        }
        case FieldDescriptor::CPPTYPE_MESSAGE: {
          TruncateFields(reflection->MutableMessage(message, field), max_len);
          break;
        }
        default:
          break;
      }
    }
  }
}

} // namespace pb_util
} // namespace kudu
