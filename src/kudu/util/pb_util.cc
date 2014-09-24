// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Some portions copyright (C) 2008, Google, inc.
//
// Utilities for working with protobufs.
// Some of this code is cribbed from the protobuf source,
// but modified to work with kudu's 'faststring' instead of STL strings.

#include "kudu/util/pb_util.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/message.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/crc.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/status.h"

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::Reflection;
using kudu::crc::Crc;
using kudu::pb_util::internal::SequentialFileFileInputStream;
using kudu::pb_util::internal::WritableFileOutputStream;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using strings::Utf8SafeCEscape;

static const char* const kTmpTemplateSuffix = ".tmp.XXXXXX";

// Protobuf container constants.
static const int kPBContainerVersion = 1;
static const int kPBContainerMagicLen = 8;
static const int kPBContainerHeaderLen =
    // magic number + version + data size
    kPBContainerMagicLen + sizeof(uint32_t) + sizeof(uint32_t);
static const int kPBContainerChecksumLen = sizeof(uint32_t);
static const int kPBContainerMinFileSize =
    kPBContainerHeaderLen + kPBContainerChecksumLen;

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

// Perform a generic operation in which we create a temporary file, serialize
// data to that WritableFile via a provided callback, ensure that operation
// succeeded, sync (if enabled) and close the file, rename the temporary file
// to its final location, and sync the parent directory (if enabled).
// Syncing relies on the specified sync flag.
//
// By the time it arrives to this function, possibly after being partially
// applied / bound, "serialize_func" callback must have the following signature:
//
//   Status Serialize(WritableFile* file)
//
Status DoSafeFileWrite(Env* env, const std::string& path,
                       const Callback<Status(WritableFile*)>& serialize_func,
                       SyncMode sync) {
  const string tmp_template = path + kTmpTemplateSuffix;
  string tmp_path;

  gscoped_ptr<WritableFile> file;
  RETURN_NOT_OK(env->NewTempWritableFile(WritableFileOptions(), tmp_template, &tmp_path, &file));
  env_util::ScopedFileDeleter tmp_deleter(env, tmp_path);

  RETURN_NOT_OK_PREPEND(serialize_func.Run(file.get()),
                        "Failed to serialize data to " + tmp_path);

  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(file->Sync(), "Failed to Sync() " + tmp_path);
  }
  RETURN_NOT_OK_PREPEND(file->Close(), "Failed to Close() " + tmp_path);
  RETURN_NOT_OK_PREPEND(env->RenameFile(tmp_path, path), "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(env->SyncDir(DirName(path)), "Failed to SyncDir() parent of " + path);
  }
  return Status::OK();
}

// See MessageLite::SerializeToZeroCopyStream.
static Status SerializePBToWritableFile(const MessageLite& msg, WritableFile *wfile) {
  WritableFileOutputStream ostream(wfile);
  bool res = msg.SerializeToZeroCopyStream(&ostream);
  if (!res || !ostream.Flush()) {
    return Status::IOError("Unable to serialize PB to file");
  }
  return Status::OK();
}

Status WritePBToPath(Env* env, const std::string& path, const MessageLite& msg, SyncMode sync) {
  Callback<Status(WritableFile*)> serialize_func = Bind(&SerializePBToWritableFile, ConstRef(msg));
  return DoSafeFileWrite(env, path, serialize_func, sync);
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
            const string& s_const = reflection->GetRepeatedStringReference(*message, field, i,
                                                                           NULL);
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

// Validate the magic number and version in the proto container file header and
// return the parsed data size as an out parameter.
static Status ValidateHeaderAndParseDataSize(const char* magic, const Slice& header,
                                             uint32_t* data_size) {
  // Validate magic number.
  if (PREDICT_FALSE(!strings::memeq(magic, header.data(), kPBContainerMagicLen))) {
    string file_magic(reinterpret_cast<const char*>(header.data()), kPBContainerMagicLen);
    return Status::Corruption("Invalid magic number",
                              Substitute("Expected: $0, found: $1",
                                         Utf8SafeCEscape(magic),
                                         Utf8SafeCEscape(file_magic)));
  }
  size_t header_offset = kPBContainerMagicLen;

  // Validate container file version.
  uint32_t version = DecodeFixed32(header.data() + header_offset);
  if (PREDICT_FALSE(version != kPBContainerVersion)) {
    // We only support version 1.
    return Status::NotSupported(
        Substitute("Protobuf container has version $0, we only support version $1",
                   version, kPBContainerVersion));
  }
  header_offset += sizeof(uint32_t);

  // Parse file size.
  *data_size = DecodeFixed32(header.data() + header_offset);
  return Status::OK();
}

Status ReadPBContainerFromPath(Env* env, const std::string& path,
                               const char* magic, MessageLite* msg) {
  DCHECK_EQ(kPBContainerMagicLen, strlen(magic)) << "Magic number string incorrect length";

  gscoped_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &file));

  uint64_t file_size;
  RETURN_NOT_OK(file->Size(&file_size));

  // Sanity check on minimum file size for container files.
  // Actually less than the minimum, since it assumes 0 bytes for the PB.
  if (PREDICT_FALSE(file_size < kPBContainerMinFileSize)) {
    return Status::Corruption("File size not large enough to be valid",
                              Substitute("Proto container file $0 is only $1 bytes long",
                                         path, file_size));
  }

  // Read header data.
  Slice header;
  faststring scratch(kPBContainerHeaderLen);
  size_t offset = 0;
  RETURN_NOT_OK(env_util::ReadFully(file.get(), offset, kPBContainerHeaderLen,
                                    &header, scratch.data()));
  offset += kPBContainerHeaderLen;

  uint32_t data_size = 0;
  RETURN_NOT_OK_PREPEND(ValidateHeaderAndParseDataSize(magic, header, &data_size),
                        Substitute("Header parsing failed for proto container file $0", path));

  uint32_t expected_file_size = kPBContainerHeaderLen + data_size + kPBContainerChecksumLen;
  if (PREDICT_FALSE(file_size != expected_file_size)) {
    return Status::Corruption(
        Substitute("Incorrect file size for file $0: actually $1, expected $2 bytes",
                   path, file_size, expected_file_size));
  }

  // Read body data into buffer for checksum & parsing.
  Slice body;
  faststring body_scratch(data_size);
  RETURN_NOT_OK(env_util::ReadFully(file.get(), offset, data_size,
                                    &body, body_scratch.data()));
  offset += data_size;

  // Validate checksum.
  uint32_t expected_checksum = 0;
  {
    Slice encoded_checksum;
    uint8_t checksum_scratch[sizeof(kPBContainerChecksumLen)];
    RETURN_NOT_OK(env_util::ReadFully(file.get(), offset, kPBContainerChecksumLen,
                                      &encoded_checksum, checksum_scratch));
    expected_checksum = DecodeFixed32(encoded_checksum.data());
  }

  // Validate CRC32C checksum.
  Crc* crc32c = crc::GetCrc32cInstance();
  // Compute a rolling checksum over the two byte arrays (header, body).
  uint64_t actual_checksum = 0;
  crc32c->Compute(header.data(), header.size(), &actual_checksum);
  crc32c->Compute(body.data(), body.size(), &actual_checksum);
  if (PREDICT_FALSE(actual_checksum != expected_checksum)) {
    return Status::Corruption(Substitute("Incorrect checksum of file $0: actually $1, expected $2",
                                         path, actual_checksum, expected_checksum));
  }

  // The file looks valid. Time to decode.
  if (PREDICT_FALSE(!msg->ParseFromArray(body.data(), body.size()))) {
    return Status::IOError("Unable to parse PB from path", path);
  }

  return Status::OK();
}

// Serialize an entire proto container file into a faststring buffer.
static Status SerializePBContainerToFile(const char* magic,
                                         const MessageLite& msg,
                                         WritableFile* file) {
  DCHECK_EQ(kPBContainerMagicLen, strlen(magic)) << "Magic number string incorrect length";
  DCHECK(msg.IsInitialized()) << "Cannot serialize uninitialized protobuf";
  int data_size = msg.ByteSize();
  DCHECK_GE(data_size, 0);

  faststring buf;
  uint64_t bufsize = kPBContainerHeaderLen + data_size + kPBContainerChecksumLen;
  buf.resize(bufsize);

  // Serialize the magic.
  strings::memcpy_inlined(buf.data(), magic, kPBContainerMagicLen);
  size_t offset = kPBContainerMagicLen;

  // Serialize the version.
  InlineEncodeFixed32(buf.data() + offset, kPBContainerVersion);
  offset += sizeof(uint32_t);

  // Serialize the data size.
  InlineEncodeFixed32(buf.data() + offset, static_cast<uint32_t>(data_size));
  offset += sizeof(uint32_t);

  // Serialize the data.
  if (PREDICT_FALSE(!msg.SerializeToArray(buf.data() + offset, data_size))) {
    return Status::IOError("Failed to serialize PB to array");
  }
  offset += data_size;

  // Calculate and serialize the checksum.
  uint32_t checksum = crc::Crc32c(buf.data(), offset);
  InlineEncodeFixed32(buf.data() + offset, checksum);
  offset += kPBContainerChecksumLen;

  DCHECK_EQ(bufsize, offset) << "Serialized unexpected number of total bytes";
  RETURN_NOT_OK_PREPEND(file->Append(buf), "Failed to Append() data to file");

  return Status::OK();
}

Status WritePBContainerToPath(Env* env, const std::string& path,
                              const char* magic, const MessageLite& msg,
                              SyncMode sync) {
  Callback<Status(WritableFile*)> serialize_func = Bind(&SerializePBContainerToFile, magic,
                                                        ConstRef(msg));
  return DoSafeFileWrite(env, path, serialize_func, sync);
}

} // namespace pb_util
} // namespace kudu
