// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Some portions copyright (C) 2008, Google, inc.
//
// Utilities for working with protobufs.
// Some of this code is cribbed from the protobuf source,
// but modified to work with kudu's 'faststring' instead of STL strings.

#include "kudu/util/pb_util.h"

#include <deque>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/text_format.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mutex.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/pb_util.pb.h"
#include "kudu/util/status.h"

using google::protobuf::Descriptor;
using google::protobuf::DescriptorPool;
using google::protobuf::DynamicMessageFactory;
using google::protobuf::FieldDescriptor;
using google::protobuf::FileDescriptor;
using google::protobuf::FileDescriptorProto;
using google::protobuf::FileDescriptorSet;
using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::Reflection;
using google::protobuf::SimpleDescriptorDatabase;
using google::protobuf::TextFormat;
using kudu::crc::Crc;
using kudu::pb_util::internal::SequentialFileFileInputStream;
using kudu::pb_util::internal::WritableFileOutputStream;
using std::deque;
using std::endl;
using std::initializer_list;
using std::ostream;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;
using strings::Utf8SafeCEscape;

namespace std {

// Allow the use of FileState with DCHECK_EQ.
std::ostream& operator<< (std::ostream& os, const kudu::pb_util::FileState& state) {
  os << static_cast<int>(state);
  return os;
}

} // namespace std

namespace kudu {
namespace pb_util {

static const char* const kTmpTemplateSuffix = ".XXXXXX";

// Protobuf container constants.
static const uint32_t kPBContainerInvalidVersion = 0;
static const uint32_t kPBContainerDefaultVersion = 2;
static const int kPBContainerChecksumLen = sizeof(uint32_t);
static const char kPBContainerMagic[] = "kuducntr";
static const int kPBContainerMagicLen = 8;
static const int kPBContainerV1HeaderLen =
    kPBContainerMagicLen + sizeof(uint32_t); // Magic number + version.
static const int kPBContainerV2HeaderLen =
    kPBContainerV1HeaderLen + kPBContainerChecksumLen; // Same as V1 plus a checksum.

const int kPBContainerMinimumValidLength = kPBContainerV1HeaderLen;

static_assert(arraysize(kPBContainerMagic) - 1 == kPBContainerMagicLen,
              "kPBContainerMagic does not match expected length");

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

// Returns true iff the specified protobuf container file version is supported
// by this implementation.
bool IsSupportedContainerVersion(uint32_t version) {
  if (version == 1 || version == 2) {
    return true;
  }
  return false;
}

// Perform a non-short read. The contract is that we may go to great lengths to
// retry reads, but if we are ultimately unable to read 'length' bytes from the
// file then a non-OK Status is returned.
template<typename T>
Status NonShortRead(T* file, uint64_t offset, uint64_t length, Slice* result, uint8_t* scratch);

template<>
Status NonShortRead<RandomAccessFile>(RandomAccessFile* file, uint64_t offset, uint64_t length,
                                      Slice* result, uint8_t* scratch) {
  return env_util::ReadFully(file, offset, length, result, scratch);
}

template<>
Status NonShortRead<RWFile>(RWFile* file, uint64_t offset, uint64_t length,
                            Slice* result, uint8_t* scratch) {
  return file->Read(offset, length, result, scratch);
}

// Reads exactly 'length' bytes from the container file into 'scratch',
// validating that there is sufficient data in the file to read this length
// before attempting to do so, and validating that it has read that length
// after performing the read.
//
// If the file size is less than the requested size of the read, returns
// Status::Incomplete.
// If there is an unexpected short read, returns Status::Corruption.
//
// A Slice of the bytes read into 'scratch' is returned in 'result'.
template<typename ReadableFileType>
Status ValidateAndReadData(ReadableFileType* reader, uint64_t file_size,
                           uint64_t* offset, uint64_t length,
                           Slice* result, unique_ptr<uint8_t[]>* scratch) {
  // Validate the read length using the file size.
  if (*offset + length > file_size) {
    return Status::Incomplete("File size not large enough to be valid",
                              Substitute("Proto container file $0: "
                                  "Tried to read $1 bytes at offset "
                                  "$2 but file size is only $3 bytes",
                                  reader->filename(), length,
                                  *offset, file_size));
  }

  // Perform the read.
  Slice s;
  unique_ptr<uint8_t[]> local_scratch(new uint8_t[length]);
  RETURN_NOT_OK(NonShortRead(reader, *offset, length, &s, local_scratch.get()));
  CHECK_EQ(length, s.size()) // Should never trigger due to contract with reader APIs.
      << Substitute("Unexpected short read: Proto container file $0: Tried to read $1 bytes "
                    "but only read $2 bytes",
                    reader->filename(), length, s.size());

  *offset += length;
  *result = s;
  scratch->swap(local_scratch);
  return Status::OK();
}

// Helper macro for use with ParseAndCompareChecksum(). Example usage:
// RETURN_NOT_OK_PREPEND(ParseAndCompareChecksum(checksum.data(), { data }),
//    CHECKSUM_ERR_MSG("Data checksum does not match", filename, offset));
#define CHECKSUM_ERR_MSG(prefix, filename, cksum_offset) \
  Substitute("$0: Incorrect checksum in file $1 at offset $2", prefix, filename, cksum_offset)

// Parses a checksum from the specified buffer and compares it to the bytes
// given in 'slices' by calculating a rolling CRC32 checksum of the bytes in
// the 'slices'.
// If they match, returns OK. Otherwise, returns Status::Corruption.
Status ParseAndCompareChecksum(const uint8_t* checksum_buf,
                               const initializer_list<Slice>& slices) {
  uint32_t written_checksum = DecodeFixed32(checksum_buf);
  uint64_t actual_checksum = 0;
  Crc* crc32c = crc::GetCrc32cInstance();
  for (Slice s : slices) {
    crc32c->Compute(s.data(), s.size(), &actual_checksum);
  }
  if (PREDICT_FALSE(actual_checksum != written_checksum)) {
    return Status::Corruption(Substitute("Checksum does not match. Expected: $0. Actual: $1",
                                         written_checksum, actual_checksum));
  }
  return Status::OK();
}

// Read and parse a message of the specified format at the given offset in the
// format documented in pb_util.h. 'offset' is an in-out parameter and will be
// updated with the new offset on success. On failure, 'offset' is not modified.
template<typename ReadableFileType>
Status ReadPBStartingAt(ReadableFileType* reader, int version, uint64_t* offset, Message* msg) {
  uint64_t tmp_offset = *offset;
  VLOG(1) << "Reading PB with version " << version << " starting at offset " << *offset;

  uint64_t file_size;
  RETURN_NOT_OK(reader->Size(&file_size));
  if (tmp_offset == file_size) {
    return Status::EndOfFile("Reached end of file");
  }

  // Read the data length from the file.
  // Version 2+ includes a checksum for the length field.
  uint64_t length_buflen = (version == 1) ? sizeof(uint32_t)
                                          : sizeof(uint32_t) + kPBContainerChecksumLen;
  Slice len_and_cksum_slice;
  unique_ptr<uint8_t[]> length_scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, length_buflen,
                                            &len_and_cksum_slice, &length_scratch),
                        Substitute("Could not read data length from proto container file $0 "
                                   "at offset $1", reader->filename(), *offset));
  Slice length(len_and_cksum_slice.data(), sizeof(uint32_t));

  // Versions >= 2 have an individual checksum for the data length.
  if (version >= 2) {
    Slice length_checksum(len_and_cksum_slice.data() + sizeof(uint32_t), kPBContainerChecksumLen);
    RETURN_NOT_OK_PREPEND(ParseAndCompareChecksum(length_checksum.data(), { length }),
        CHECKSUM_ERR_MSG("Data length checksum does not match",
                         reader->filename(), tmp_offset - kPBContainerChecksumLen));
  }
  uint32_t data_length = DecodeFixed32(length.data());

  // Read body and checksum into buffer for checksum & parsing.
  uint64_t data_and_cksum_buflen = data_length + kPBContainerChecksumLen;
  Slice body_and_cksum_slice;
  unique_ptr<uint8_t[]> body_scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, data_and_cksum_buflen,
                                            &body_and_cksum_slice, &body_scratch),
                        Substitute("Could not read PB message data from proto container file $0 "
                                   "at offset $1",
                                   reader->filename(), tmp_offset));
  Slice body(body_and_cksum_slice.data(), data_length);
  Slice record_checksum(body_and_cksum_slice.data() + data_length, kPBContainerChecksumLen);

  // Version 1 has a single checksum for length, body.
  // Version 2+ has individual checksums for length and body, respectively.
  if (version == 1) {
    RETURN_NOT_OK_PREPEND(ParseAndCompareChecksum(record_checksum.data(), { length, body }),
        CHECKSUM_ERR_MSG("Length and data checksum does not match",
                         reader->filename(), tmp_offset - kPBContainerChecksumLen));
  } else {
    RETURN_NOT_OK_PREPEND(ParseAndCompareChecksum(record_checksum.data(), { body }),
        CHECKSUM_ERR_MSG("Data checksum does not match",
                         reader->filename(), tmp_offset - kPBContainerChecksumLen));
  }

  // The checksum is correct. Time to decode the body.
  //
  // We could compare pb_type_ against msg.GetTypeName(), but:
  // 1. pb_type_ is not available when reading the supplemental header,
  // 2. ParseFromArray() should fail if the data cannot be parsed into the
  //    provided message type.

  // To permit parsing of very large PB messages, we must use parse through a
  // CodedInputStream and bump the byte limit. The SetTotalBytesLimit() docs
  // say that 512MB is the shortest theoretical message length that may produce
  // integer overflow warnings, so that's what we'll use.
  ArrayInputStream ais(body.data(), body.size());
  CodedInputStream cis(&ais);
  cis.SetTotalBytesLimit(512 * 1024 * 1024, -1);
  if (PREDICT_FALSE(!msg->ParseFromCodedStream(&cis))) {
    return Status::IOError("Unable to parse PB from path", reader->filename());
  }

  *offset = tmp_offset;
  return Status::OK();
}

// Wrapper around ReadPBStartingAt() to enforce that we don't return
// Status::Incomplete() for V1 format files.
template<typename ReadableFileType>
Status ReadFullPB(ReadableFileType* reader, int version, uint64_t* offset, Message* msg) {
  Status s = ReadPBStartingAt(reader, version, offset, msg);
  if (PREDICT_FALSE(s.IsIncomplete() && version == 1)) {
    return Status::Corruption("Unrecoverable incomplete record", s.ToString());
  }
  return s;
}

// Read and parse the protobuf container file-level header documented in pb_util.h.
template<typename ReadableFileType>
Status ParsePBFileHeader(ReadableFileType* reader, uint64_t* offset, int* version) {
  uint64_t file_size;
  RETURN_NOT_OK(reader->Size(&file_size));

  // We initially read enough data for a V2+ file header. This optimizes for
  // V2+ and is valid on a V1 file because we don't consider these files valid
  // unless they contain a record in addition to the file header. The
  // additional 4 bytes required by a V2+ header (vs V1) is still less than the
  // minimum number of bytes required for a V1 format data record.
  uint64_t tmp_offset = *offset;
  Slice header;
  unique_ptr<uint8_t[]> scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, kPBContainerV2HeaderLen,
                                            &header, &scratch),
                        Substitute("Could not read header for proto container file $0",
                                   reader->filename()));
  Slice magic_and_version(header.data(), kPBContainerMagicLen + sizeof(uint32_t));
  Slice checksum(header.data() + kPBContainerMagicLen + sizeof(uint32_t), kPBContainerChecksumLen);

  // Validate magic number.
  if (PREDICT_FALSE(!strings::memeq(kPBContainerMagic, header.data(), kPBContainerMagicLen))) {
    string file_magic(reinterpret_cast<const char*>(header.data()), kPBContainerMagicLen);
    return Status::Corruption("Invalid magic number",
                              Substitute("Expected: $0, found: $1",
                                         Utf8SafeCEscape(kPBContainerMagic),
                                         Utf8SafeCEscape(file_magic)));
  }

  // Validate container file version.
  uint32_t tmp_version = DecodeFixed32(header.data() + kPBContainerMagicLen);
  if (PREDICT_FALSE(!IsSupportedContainerVersion(tmp_version))) {
    return Status::NotSupported(
        Substitute("Protobuf container has unsupported version: $0. Default version: $1",
                   tmp_version, kPBContainerDefaultVersion));
  }

  // Versions >= 2 have a checksum after the magic number and encoded version
  // to ensure the integrity of these fields.
  if (tmp_version >= 2) {
    RETURN_NOT_OK_PREPEND(ParseAndCompareChecksum(checksum.data(), { magic_and_version }),
        CHECKSUM_ERR_MSG("File header checksum does not match",
                         reader->filename(), tmp_offset - kPBContainerChecksumLen));
  } else {
    // Version 1 doesn't have a header checksum. Rewind our read offset so this
    // data will be read again when we next attempt to read a data record.
    tmp_offset -= kPBContainerChecksumLen;
  }

  *offset = tmp_offset;
  *version = tmp_version;
  return Status::OK();
}

// Read and parse the supplemental header from the container file.
template<typename ReadableFileType>
Status ReadSupplementalHeader(ReadableFileType* reader, int version, uint64_t* offset,
                              ContainerSupHeaderPB* sup_header) {
  RETURN_NOT_OK_PREPEND(ReadFullPB(reader, version, offset, sup_header),
      Substitute("Could not read supplemental header from proto container file $0 "
                 "with version $1 at offset $2",
                 reader->filename(), version, *offset));
  return Status::OK();
}

} // anonymous namespace

void AppendToString(const MessageLite &msg, faststring *output) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  AppendPartialToString(msg, output);
}

void AppendPartialToString(const MessageLite &msg, faststring* output) {
  size_t old_size = output->size();
  int byte_size = msg.ByteSize();
  // Messages >2G cannot be serialized due to overflow computing ByteSize.
  DCHECK_GE(byte_size, 0) << "Error computing ByteSize";

  output->resize(old_size + static_cast<size_t>(byte_size));

  uint8* start = &((*output)[old_size]);
  uint8* end = msg.SerializeWithCachedSizesToArray(start);
  if (end - start != byte_size) {
    ByteSizeConsistencyError(byte_size, msg.ByteSize(), end - start);
  }
}

void SerializeToString(const MessageLite &msg, faststring *output) {
  output->clear();
  AppendToString(msg, output);
}

Status ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile) {
  SequentialFileFileInputStream istream(rfile);
  if (!msg->ParseFromZeroCopyStream(&istream)) {
    RETURN_NOT_OK(istream.status());

    // If it's not a file IO error then it's a parsing error.
    // Probably, we read wrong or damaged data here.
    return Status::Corruption("Error parsing msg", InitializationErrorMessage("parse", *msg));
  }
  return Status::OK();
}

Status ParseFromArray(MessageLite* msg, const uint8_t* data, uint32_t length) {
  if (!msg->ParseFromArray(data, length)) {
    return Status::Corruption("Error parsing msg", InitializationErrorMessage("parse", *msg));
  }
  return Status::OK();
}

Status WritePBToPath(Env* env, const std::string& path,
                     const MessageLite& msg,
                     SyncMode sync) {
  const string tmp_template = path + kTmpInfix + kTmpTemplateSuffix;
  string tmp_path;

  unique_ptr<WritableFile> file;
  RETURN_NOT_OK(env->NewTempWritableFile(WritableFileOptions(), tmp_template, &tmp_path, &file));
  env_util::ScopedFileDeleter tmp_deleter(env, tmp_path);

  WritableFileOutputStream ostream(file.get());
  bool res = msg.SerializeToZeroCopyStream(&ostream);
  if (!res || !ostream.Flush()) {
    return Status::IOError("Unable to serialize PB to file");
  }

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

Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg) {
  shared_ptr<SequentialFile> rfile;
  RETURN_NOT_OK(env_util::OpenFileForSequential(env, path, &rfile));
  RETURN_NOT_OK(ParseFromSequentialFile(msg, rfile.get()));
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
  for (const FieldDescriptor* field : fields) {
    if (field->is_repeated()) {
      for (int i = 0; i < reflection->FieldSize(*message, field); i++) {
        switch (field->cpp_type()) {
          case FieldDescriptor::CPPTYPE_STRING: {
            const string& s_const = reflection->GetRepeatedStringReference(*message, field, i,
                                                                           nullptr);
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
          const string& s_const = reflection->GetStringReference(*message, field, nullptr);
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

namespace {
class SecureFieldPrinter : public TextFormat::FieldValuePrinter {
 public:
  using super = TextFormat::FieldValuePrinter;

  string PrintFieldName(const Message& message,
                        const Reflection* reflection,
                        const FieldDescriptor* field) const override {
    hide_next_string_ = field->cpp_type() == FieldDescriptor::CPPTYPE_STRING &&
        field->options().GetExtension(REDACT);
    return super::PrintFieldName(message, reflection, field);
  }

  string PrintString(const string& val) const override {
    if (hide_next_string_) {
      hide_next_string_ = false;
      return KUDU_REDACT(super::PrintString(val));
    }
    return super::PrintString(val);
  }
  string PrintBytes(const string& val) const override {
    if (hide_next_string_) {
      hide_next_string_ = false;
      return KUDU_REDACT(super::PrintBytes(val));
    }
    return super::PrintBytes(val);
  }

  mutable bool hide_next_string_ = false;
};
} // anonymous namespace

string SecureDebugString(const Message& msg) {
  string debug_string;
  TextFormat::Printer printer;
  printer.SetDefaultFieldValuePrinter(new SecureFieldPrinter());
  printer.PrintToString(msg, &debug_string);
  return debug_string;
}

string SecureShortDebugString(const Message& msg) {
  string debug_string;

  TextFormat::Printer printer;
  printer.SetSingleLineMode(true);
  printer.SetDefaultFieldValuePrinter(new SecureFieldPrinter());

  printer.PrintToString(msg, &debug_string);
  // Single line mode currently might have an extra space at the end.
  if (!debug_string.empty() &&
      debug_string[debug_string.size() - 1] == ' ') {
    debug_string.resize(debug_string.size() - 1);
  }

  return debug_string;
}


WritablePBContainerFile::WritablePBContainerFile(shared_ptr<RWFile> writer)
  : state_(FileState::NOT_INITIALIZED),
    offset_(0),
    version_(kPBContainerDefaultVersion),
    writer_(std::move(writer)) {
}

WritablePBContainerFile::~WritablePBContainerFile() {
  WARN_NOT_OK(Close(), "Could not Close() when destroying file");
}

Status WritablePBContainerFile::SetVersionForTests(int version) {
  DCHECK_EQ(FileState::NOT_INITIALIZED, state_);
  if (!IsSupportedContainerVersion(version)) {
    return Status::NotSupported(Substitute("Version $0 is not supported", version));
  }
  version_ = version;
  return Status::OK();
}

Status WritablePBContainerFile::Init(const Message& msg) {
  DCHECK_EQ(FileState::NOT_INITIALIZED, state_);

  const uint64_t kHeaderLen = (version_ == 1) ? kPBContainerV1HeaderLen
                                              : kPBContainerV1HeaderLen + kPBContainerChecksumLen;

  faststring buf;
  buf.resize(kHeaderLen);

  // Serialize the magic.
  strings::memcpy_inlined(buf.data(), kPBContainerMagic, kPBContainerMagicLen);
  uint64_t offset = kPBContainerMagicLen;

  // Serialize the version.
  InlineEncodeFixed32(buf.data() + offset, version_);
  offset += sizeof(uint32_t);
  DCHECK_EQ(kPBContainerV1HeaderLen, offset)
    << "Serialized unexpected number of total bytes";

  // Versions >= 2: Checksum the magic and version.
  if (version_ >= 2) {
    uint32_t header_checksum = crc::Crc32c(buf.data(), offset);
    InlineEncodeFixed32(buf.data() + offset, header_checksum);
    offset += sizeof(uint32_t);
  }

  // Serialize the supplemental header.
  ContainerSupHeaderPB sup_header;
  PopulateDescriptorSet(msg.GetDescriptor()->file(),
                        sup_header.mutable_protos());
  sup_header.set_pb_type(msg.GetTypeName());
  RETURN_NOT_OK_PREPEND(AppendMsgToBuffer(sup_header, &buf),
                        "Failed to prepare supplemental header for writing");

  // Write the serialized buffer to the file.
  RETURN_NOT_OK_PREPEND(AppendBytes(buf),
                        "Failed to append header to file");
  state_ = FileState::OPEN;
  return Status::OK();
}

Status WritablePBContainerFile::Reopen() {
  DCHECK(state_ == FileState::NOT_INITIALIZED || state_ == FileState::OPEN) << state_;
  {
    std::lock_guard<Mutex> l(offset_lock_);
    offset_ = 0;
    RETURN_NOT_OK(ParsePBFileHeader(writer_.get(), &offset_, &version_));
    ContainerSupHeaderPB sup_header;
    RETURN_NOT_OK(ReadSupplementalHeader(writer_.get(), version_, &offset_, &sup_header));
    RETURN_NOT_OK(writer_->Size(&offset_)); // Reset the write offset to the end of the file.
  }
  state_ = FileState::OPEN;
  return Status::OK();
}

Status WritablePBContainerFile::AppendBytes(const Slice& data) {
  std::lock_guard<Mutex> l(offset_lock_);
  RETURN_NOT_OK(writer_->Write(offset_, data));
  offset_ += data.size();
  return Status::OK();
}

Status WritablePBContainerFile::Append(const Message& msg) {
  DCHECK_EQ(FileState::OPEN, state_);

  faststring buf;
  RETURN_NOT_OK_PREPEND(AppendMsgToBuffer(msg, &buf),
                        "Failed to prepare buffer for writing");
  RETURN_NOT_OK_PREPEND(AppendBytes(buf), "Failed to append data to file");

  return Status::OK();
}

Status WritablePBContainerFile::Flush() {
  DCHECK_EQ(FileState::OPEN, state_);

  // TODO: Flush just the dirty bytes.
  RETURN_NOT_OK_PREPEND(writer_->Flush(RWFile::FLUSH_ASYNC, 0, 0), "Failed to Flush() file");

  return Status::OK();
}

Status WritablePBContainerFile::Sync() {
  DCHECK_EQ(FileState::OPEN, state_);

  RETURN_NOT_OK_PREPEND(writer_->Sync(), "Failed to Sync() file");

  return Status::OK();
}

Status WritablePBContainerFile::Close() {
  if (state_ != FileState::CLOSED) {
    state_ = FileState::CLOSED;
    Status s = writer_->Close();
    writer_.reset();
    RETURN_NOT_OK_PREPEND(s, "Failed to Close() file");
  }
  return Status::OK();
}

const string& WritablePBContainerFile::filename() const {
  return writer_->filename();
}

Status WritablePBContainerFile::AppendMsgToBuffer(const Message& msg, faststring* buf) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  int data_len = msg.ByteSize();
  // Messages >2G cannot be serialized due to overflow computing ByteSize.
  DCHECK_GE(data_len, 0) << "Error computing ByteSize";
  uint64_t record_buflen =  sizeof(uint32_t) + data_len + sizeof(uint32_t);
  if (version_ >= 2) {
    record_buflen += sizeof(uint32_t); // Additional checksum just for the length.
  }

  // Grow the buffer to hold the new data.
  uint64_t record_offset = buf->size();
  buf->resize(record_offset + record_buflen);
  uint8_t* dst = buf->data() + record_offset;

  // Serialize the data length.
  size_t cur_offset = 0;
  InlineEncodeFixed32(dst + cur_offset, static_cast<uint32_t>(data_len));
  cur_offset += sizeof(uint32_t);

  // For version >= 2: Serialize the checksum of the data length.
  if (version_ >= 2) {
    uint32_t length_checksum = crc::Crc32c(&data_len, sizeof(data_len));
    InlineEncodeFixed32(dst + cur_offset, length_checksum);
    cur_offset += sizeof(uint32_t);
  }

  // Serialize the data.
  uint64_t data_offset = cur_offset;
  if (PREDICT_FALSE(!msg.SerializeWithCachedSizesToArray(dst + cur_offset))) {
    return Status::IOError("Failed to serialize PB to array");
  }
  cur_offset += data_len;

  // Calculate and serialize the data checksum.
  // For version 1, this is the checksum of the len + data.
  // For version >= 2, this is only the checksum of the data.
  uint32_t data_checksum;
  if (version_ == 1) {
    data_checksum = crc::Crc32c(dst, cur_offset);
  } else {
    data_checksum = crc::Crc32c(dst + data_offset, data_len);
  }
  InlineEncodeFixed32(dst + cur_offset, data_checksum);
  cur_offset += sizeof(uint32_t);

  DCHECK_EQ(record_buflen, cur_offset) << "Serialized unexpected number of total bytes";
  return Status::OK();
}

void WritablePBContainerFile::PopulateDescriptorSet(
    const FileDescriptor* desc, FileDescriptorSet* output) {
  // Because we don't compile protobuf with TSAN enabled, copying the
  // static PB descriptors in this function ends up triggering a lot of
  // race reports. We suppress the reports, but TSAN still has to walk
  // the stack, etc, and this function becomes very slow. So, we ignore
  // TSAN here.
  debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;

  FileDescriptorSet all_descs;

  // Tracks all schemas that have been added to 'unemitted' at one point
  // or another. Is a superset of 'unemitted' and only ever grows.
  unordered_set<const FileDescriptor*> processed;

  // Tracks all remaining unemitted schemas.
  deque<const FileDescriptor*> unemitted;

  InsertOrDie(&processed, desc);
  unemitted.push_front(desc);
  while (!unemitted.empty()) {
    const FileDescriptor* proto = unemitted.front();

    // The current schema is emitted iff we've processed (i.e. emitted) all
    // of its dependencies.
    bool emit = true;
    for (int i = 0; i < proto->dependency_count(); i++) {
      const FileDescriptor* dep = proto->dependency(i);
      if (InsertIfNotPresent(&processed, dep)) {
        unemitted.push_front(dep);
        emit = false;
      }
    }
    if (emit) {
      unemitted.pop_front();
      proto->CopyTo(all_descs.mutable_file()->Add());
    }
  }
  all_descs.Swap(output);
}

ReadablePBContainerFile::ReadablePBContainerFile(shared_ptr<RandomAccessFile> reader)
  : state_(FileState::NOT_INITIALIZED),
    version_(kPBContainerInvalidVersion),
    offset_(0),
    reader_(std::move(reader)) {
}

ReadablePBContainerFile::~ReadablePBContainerFile() {
  Close();
}

Status ReadablePBContainerFile::Open() {
  DCHECK_EQ(FileState::NOT_INITIALIZED, state_);
  RETURN_NOT_OK(ParsePBFileHeader(reader_.get(), &offset_, &version_));
  ContainerSupHeaderPB sup_header;
  RETURN_NOT_OK(ReadSupplementalHeader(reader_.get(), version_, &offset_, &sup_header));
  protos_.reset(sup_header.release_protos());
  pb_type_ = sup_header.pb_type();
  state_ = FileState::OPEN;
  return Status::OK();
}

Status ReadablePBContainerFile::ReadNextPB(Message* msg) {
  DCHECK_EQ(FileState::OPEN, state_);
  return ReadFullPB(reader_.get(), version_, &offset_, msg);
}

Status ReadablePBContainerFile::Dump(ostream* os, bool oneline) {
  DCHECK_EQ(FileState::OPEN, state_);

  // Use the embedded protobuf information from the container file to
  // create the appropriate kind of protobuf Message.
  //
  // Loading the schemas into a DescriptorDatabase (and not directly into
  // a DescriptorPool) defers resolution until FindMessageTypeByName()
  // below, allowing for schemas to be loaded in any order.
  SimpleDescriptorDatabase db;
  for (int i = 0; i < protos()->file_size(); i++) {
    if (!db.Add(protos()->file(i))) {
      return Status::Corruption("Descriptor not loaded", Substitute(
          "Could not load descriptor for PB type $0 referenced in container file",
          pb_type()));
    }
  }
  DescriptorPool pool(&db);
  const Descriptor* desc = pool.FindMessageTypeByName(pb_type());
  if (!desc) {
    return Status::NotFound("Descriptor not found", Substitute(
        "Could not find descriptor for PB type $0 referenced in container file",
        pb_type()));
  }
  DynamicMessageFactory factory;
  const Message* prototype = factory.GetPrototype(desc);
  if (!prototype) {
    return Status::NotSupported("Descriptor not supported", Substitute(
        "Descriptor $0 referenced in container file not supported",
        pb_type()));
  }
  unique_ptr<Message> msg(prototype->New());

  // Dump each message in the container file.
  int count = 0;
  Status s;
  for (s = ReadNextPB(msg.get());
      s.ok();
      s = ReadNextPB(msg.get())) {
    if (oneline) {
      *os << count++ << "\t" << SecureShortDebugString(*msg) << endl;
    } else {
      *os << "Message " << count << endl;
      *os << "-------" << endl;
      *os << SecureDebugString(*msg) << endl;
      count++;
    }
  }
  return s.IsEndOfFile() ? s.OK() : s;
}

Status ReadablePBContainerFile::Close() {
  state_ = FileState::CLOSED;
  reader_.reset();
  return Status::OK();
}

int ReadablePBContainerFile::version() const {
  DCHECK_EQ(FileState::OPEN, state_);
  return version_;
}

uint64_t ReadablePBContainerFile::offset() const {
  DCHECK_EQ(FileState::OPEN, state_);
  return offset_;
}

Status ReadPBContainerFromPath(Env* env, const std::string& path, Message* msg) {
  unique_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &file));

  ReadablePBContainerFile pb_file(std::move(file));
  RETURN_NOT_OK(pb_file.Open());
  RETURN_NOT_OK(pb_file.ReadNextPB(msg));
  return pb_file.Close();
}

Status WritePBContainerToPath(Env* env, const std::string& path,
                              const Message& msg,
                              CreateMode create,
                              SyncMode sync) {
  TRACE_EVENT2("io", "WritePBContainerToPath",
               "path", path,
               "msg_type", msg.GetTypeName());

  if (create == NO_OVERWRITE && env->FileExists(path)) {
    return Status::AlreadyPresent(Substitute("File $0 already exists", path));
  }

  const string tmp_template = path + kTmpInfix + kTmpTemplateSuffix;
  string tmp_path;

  unique_ptr<RWFile> file;
  RETURN_NOT_OK(env->NewTempRWFile(RWFileOptions(), tmp_template, &tmp_path, &file));
  env_util::ScopedFileDeleter tmp_deleter(env, tmp_path);

  WritablePBContainerFile pb_file(std::move(file));
  RETURN_NOT_OK(pb_file.Init(msg));
  RETURN_NOT_OK(pb_file.Append(msg));
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK(pb_file.Sync());
  }
  RETURN_NOT_OK(pb_file.Close());
  RETURN_NOT_OK_PREPEND(env->RenameFile(tmp_path, path),
                        "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(env->SyncDir(DirName(path)),
                          "Failed to SyncDir() parent of " + path);
  }
  return Status::OK();
}


scoped_refptr<debug::ConvertableToTraceFormat> PbTracer::TracePb(const Message& msg) {
  return make_scoped_refptr(new PbTracer(msg));
}

PbTracer::PbTracer(const Message& msg) : msg_(msg.New()) {
  msg_->CopyFrom(msg);
}

void PbTracer::AppendAsTraceFormat(std::string* out) const {
  pb_util::TruncateFields(msg_.get(), kMaxFieldLengthToTrace);
  std::ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.Protobuf(*msg_);
  out->append(ss.str());
}

} // namespace pb_util
} // namespace kudu
