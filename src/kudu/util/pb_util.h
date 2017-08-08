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
// Utilities for dealing with protocol buffers.
// These are mostly just functions similar to what are found in the protobuf
// library itself, but using kudu::faststring instances instead of STL strings.
#ifndef KUDU_UTIL_PB_UTIL_H
#define KUDU_UTIL_PB_UTIL_H

#include <memory>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mutex.h"

namespace google {
namespace protobuf {
class DescriptorPool;
class FileDescriptor;
class FileDescriptorSet;
class Message;
class MessageFactory;
class MessageLite;
class SimpleDescriptorDatabase;
} // namespace protobuf
} // namespace google

namespace kudu {

class Env;
class RandomAccessFile;
class SequentialFile;
class Slice;
class Status;
class RWFile;

namespace pb_util {

enum SyncMode {
  SYNC,
  NO_SYNC
};

enum CreateMode {
  OVERWRITE,
  NO_OVERWRITE
};

enum class FileState {
  NOT_INITIALIZED,
  OPEN,
  CLOSED
};

// The minimum valid length of a PBC file.
extern const int kPBContainerMinimumValidLength;

// See MessageLite::AppendToString
void AppendToString(const google::protobuf::MessageLite &msg, faststring *output);

// See MessageLite::AppendPartialToString
void AppendPartialToString(const google::protobuf::MessageLite &msg, faststring *output);

// See MessageLite::SerializeToString.
void SerializeToString(const google::protobuf::MessageLite &msg, faststring *output);

// See MessageLite::ParseFromZeroCopyStream
Status ParseFromSequentialFile(google::protobuf::MessageLite *msg, SequentialFile *rfile);

// Similar to MessageLite::ParseFromArray, with the difference that it returns
// Status::Corruption() if the message could not be parsed.
Status ParseFromArray(google::protobuf::MessageLite* msg, const uint8_t* data, uint32_t length);

// Load a protobuf from the given path.
Status ReadPBFromPath(Env* env, const std::string& path, google::protobuf::MessageLite* msg);

// Serialize a protobuf to the given path.
//
// If SyncMode SYNC is provided, ensures the changes are made durable.
Status WritePBToPath(Env* env, const std::string& path,
                     const google::protobuf::MessageLite& msg, SyncMode sync);

// Truncate any 'bytes' or 'string' fields of this message to max_len.
// The text "<truncated>" is appended to any such truncated fields.
void TruncateFields(google::protobuf::Message* message, int max_len);

// Redaction-sensitive variant of Message::DebugString.
//
// For most protobufs, this has identical output to Message::DebugString. However,
// a field with string or binary type may be tagged with the 'kudu.REDACT' option,
// available by importing 'pb_util.proto'. When such a field is encountered by this
// method, its contents will be redacted using the 'KUDU_REDACT' macro as documented
// in kudu/util/logging.h.
std::string SecureDebugString(const google::protobuf::Message& msg);

// Same as SecureDebugString() above, but equivalent to Message::ShortDebugString.
std::string SecureShortDebugString(const google::protobuf::Message& msg);

// A protobuf "container" has the following format (all integers in
// little-endian byte order).
//
// <file header>
// <1 or more records>
//
// Note: There are two versions (version 1 and version 2) of the record format.
// Version 2 of the file format differs from version 1 in the following ways:
//
//   * Version 2 has a file header checksum.
//   * Version 2 has separate checksums for the record length and record data
//     fields.
//
// File header format
// ------------------
//
// Each protobuf container file contains a file header identifying the file.
// This includes:
//
// magic number: 8 byte string identifying the file format.
//
//    Included so that we have a minimal guarantee that this file is of the
//    type we expect and that we are not just reading garbage.
//
// container_version: 4 byte unsigned integer indicating the "version" of the
//                    container format. May be set to 1 or 2.
//
//    Included so that this file format may be extended at some later date
//    while maintaining backwards compatibility.
//
// file_header_checksum (version 2+ only): 4 byte unsigned integer with a CRC32C
//                                         of the magic and version fields.
//
//    Included so that we can validate the container version number.
//
// The remaining container fields are considered part of a "record". There may
// be 1 or more records in a valid protobuf container file.
//
// Record format
// -------------
//
// data length: 4 byte unsigned integer indicating the size of the encoded data.
//
//    Included because PB messages aren't self-delimiting, and thus
//    writing a stream of messages to the same file requires
//    delimiting each with its size.
//
//    See https://developers.google.com/protocol-buffers/docs/techniques?hl=zh-cn#streaming
//    for more details.
//
// length checksum (version 2+ only): 4-byte unsigned integer containing the
//                                    CRC32C checksum of "data length".
//
//    Included so that we may discern the difference between a truncated file
//    and a corrupted length field.
//
// data: "size" bytes of protobuf data encoded according to the schema.
//
//    Our payload.
//
// data checksum: 4 byte unsigned integer containing the CRC32C checksum of "data".
//
//    Included to ensure validity of the data on-disk.
//    Note: In version 1 of the file format, this is a checksum of both the
//    "data length" and "data" fields. In version 2+, this is only a checksum
//    of the "data" field.
//
// Supplemental header
// -------------------
//
// A valid container must have at least one record, the first of
// which is known as the "supplemental header". The supplemental header
// contains additional container-level information, including the protobuf
// schema used for the records following it. See pb_util.proto for details. As
// a containerized PB message, the supplemental header is protected by a CRC32C
// checksum like any other message.
//
// Error detection and tolerance
// -----------------------------
//
// It is worth describing the kinds of errors that can be detected by the
// protobuf container and the kinds that cannot.
//
// The checksums in the container are independent, not rolling. As such,
// they won't detect the disappearance or reordering of entire protobuf
// messages, which can happen if a range of the file is collapsed (see
// man fallocate(2)) or if the file is otherwise manually manipulated.
//
// In version 1, the checksums do not protect against corruption in the data
// length field. However, version 2 of the format resolves that problem. The
// benefit is that version 2 files can tell the difference between a record
// with a corrupted length field and a record that was only partially written.
// See ReadablePBContainerFile::ReadNextPB() for discussion on how this
// difference is expressed via the API.
//
// In version 1 of the format, corruption of the version field in the file
// header is not detectable. However, version 2 of the format addresses that
// limitation as well.
//
// Corruption of the protobuf data itself is detected in all versions of the
// file format (subject to CRC32 limitations).
//
// The container does not include footers or periodic checkpoints. As such, it
// will not detect if entire records are truncated.
//
// The design and implementation relies on data ordering guarantees provided by
// the file system to ensure that bytes are written to a file before the file
// metadata (file size) is updated. A partially-written record (the result of a
// failed append) is identified by one of the following criteria:
// 1. Too-few bytes remain in the file to constitute a valid record. For
//    version 2, that would be fewer than 12 bytes (data len, data len
//    checksum, and data checksum), or
// 2. Assuming a record's data length field is valid, then fewer bytes remain
//    in the file than are specified in the data length field (plus enough for
//    checksums).
// In the above scenarios, it is assumed that the system faulted while in the
// middle of appending a record, and it is considered safe to truncate the file
// at the beginning of the partial record.
//
// If filesystem preallocation is used (at the time of this writing, the
// implementation does not support preallocation) then even version 2 of the
// format cannot safely support culling trailing partially-written records.
// This is because it is not possible to reliably tell the difference between a
// partially-written record that did not complete fsync (resulting in a bad
// checksum) vs. a record that successfully was written to disk but then fell
// victim to bit-level disk corruption. See also KUDU-1414.
//
// These tradeoffs in error detection are reasonable given the failure
// environment that Kudu operates within. We tolerate failures such as
// "kill -9" of the Kudu process, machine power loss, or fsync/fdatasync
// failure, but not failures like runaway processes mangling data files
// in arbitrary ways or attackers crafting malicious data files.
//
// In short, no version of the file format will detect truncation of entire
// protobuf records. Version 2 relies on ordered data flushing semantics for
// automatic recoverability from partial record writes. Version 1 of the file
// format cannot support automatic recoverability from partial record writes.
//
// For further reading on what files might look like following a normal
// filesystem failure or disk corruption, and the likelihood of various types
// of disk errors, see the following papers:
//
// https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf
// https://www.usenix.org/legacy/event/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf

// Protobuf container file opened for writing. Can be built around an existing
// file or a completely new file.
//
// Every function is thread-safe unless indicated otherwise.
class WritablePBContainerFile {
 public:

  // Initializes the class instance; writer must be open.
  explicit WritablePBContainerFile(std::shared_ptr<RWFile> writer);

  // Closes the container if not already closed.
  ~WritablePBContainerFile();

  // Writes the file header to disk and initializes the write offset to the
  // byte after the file header. This method should NOT be called when opening
  // an existing file for append; use OpenExisting() for that.
  //
  // 'msg' need not be populated; its type is used to "lock" the container
  // to a particular protobuf message type in Append().
  //
  // Not thread-safe.
  Status CreateNew(const google::protobuf::Message& msg);

  // Opens an existing protobuf container file for append. The file must
  // already have a valid file header. To initialize a new blank file for
  // writing, use CreateNew() instead.
  //
  // The file header is read and the version specified there is used as the
  // format version. The length of the file is also read and is used as the
  // write offset for subsequent Append() calls. WritablePBContainerFile caches
  // the write offset instead of constantly calling stat() on the file each
  // time append is called.
  //
  // Not thread-safe.
  Status OpenExisting();

  // Writes a protobuf message to the container, beginning with its size
  // and ending with its CRC32 checksum. One of CreateNew() or OpenExisting()
  // must be called prior to calling Append(), i.e. the file must be open.
  Status Append(const google::protobuf::Message& msg);

  // Asynchronously flushes all dirty container data to the filesystem.
  // The file must be open.
  Status Flush();

  // Synchronizes all dirty container data to the filesystem.
  // The file must be open.
  //
  // Note: the parent directory is _not_ synchronized. Because the
  // container file was provided during construction, we don't know whether
  // it was created or reopened, and parent directory synchronization is
  // only needed in the former case.
  Status Sync();

  // Closes the container.
  //
  // Not thread-safe.
  Status Close();

  // Returns the path to the container's underlying file handle.
  const std::string& filename() const;

 private:
  friend class TestPBUtil;
  FRIEND_TEST(TestPBUtil, TestPopulateDescriptorSet);

  // Set the file format version. Only used for testing.
  // Must be called before CreateNew().
  Status SetVersionForTests(int version);

  // Write the protobuf schemas belonging to 'desc' and all of its
  // dependencies to 'output'.
  //
  // Schemas are written in dependency order (i.e. if A depends on B which
  // depends on C, the order is C, B, A).
  static void PopulateDescriptorSet(const google::protobuf::FileDescriptor* desc,
                                    google::protobuf::FileDescriptorSet* output);

  // Serialize the contents of 'msg' into 'buf' along with additional metadata
  // to aid in deserialization.
  Status AppendMsgToBuffer(const google::protobuf::Message& msg, faststring* buf);

  // Append bytes to the file.
  Status AppendBytes(const Slice& data);

  // State of the file.
  FileState state_;

  // Protects offset_.
  Mutex offset_lock_;

  // Current write offset into the file.
  uint64_t offset_;

  // Protobuf container file version.
  int version_;

  // File writer.
  std::shared_ptr<RWFile> writer_;
};

// Protobuf container file opened for reading.
//
// Can be built around a file with existing contents or an empty file (in
// which case it's safe to interleave with WritablePBContainerFile).
class ReadablePBContainerFile {
 public:

  // Initializes the class instance; reader must be open.
  explicit ReadablePBContainerFile(std::shared_ptr<RandomAccessFile> reader);

  // Closes the file if not already closed.
  ~ReadablePBContainerFile();

  // Reads the header information from the container and validates it.
  // Must be called before any of the other methods.
  Status Open();

  // Reads a protobuf message from the container, validating its size and
  // data using a CRC32 checksum. File must be open.
  //
  // Return values:
  // * If there are no more records in the file, returns Status::EndOfFile.
  // * If there is a partial record, but it is not long enough to be a full
  //   record or the written length of the record is less than the remaining
  //   bytes in the file, returns Status::Incomplete. If Status::Incomplete
  //   is returned, calling offset() will return the point in the file where
  //   the invalid partial record begins. In order to append additional records
  //   to the file, the file must first be truncated at that offset.
  //   Note: Version 1 of this file format will never return
  //   Status::Incomplete() from this method.
  // * If a corrupt record is encountered, returns Status::Corruption.
  // * On success, stores the result in '*msg' and returns OK.
  Status ReadNextPB(google::protobuf::Message* msg);

  // Dumps any unread protobuf messages in the container to 'os'. Each
  // message's DebugString() method is invoked to produce its textual form.
  // File must be open.
  enum class Format {
    // Print each message on multiple lines, with intervening headers.
    DEFAULT,
    // Print each message on its own line.
    ONELINE,
    // Dump in JSON.
    JSON
  };
  Status Dump(std::ostream* os, Format format);

  // Closes the container.
  Status Close();

  // Expected PB type and schema for each message to be read.
  //
  // Only valid after a successful call to Open().
  const std::string& pb_type() const { return pb_type_; }
  const google::protobuf::FileDescriptorSet* protos() const {
    return protos_.get();
  }

  // Get the prototype instance for the type of messages stored in this
  // file. The returned Message is owned by this ReadablePBContainerFile instance.
  Status GetPrototype(const google::protobuf::Message** prototype);

  // Return the protobuf container file format version.
  // File must be open.
  int version() const;

  // Return current read offset.
  // File must be open.
  uint64_t offset() const;

 private:
  FileState state_;
  int version_;
  uint64_t offset_;

  // The fully-qualified PB type name of the messages in the container.
  std::string pb_type_;

  // Wrapped in a unique_ptr so that clients need not include PB headers.
  std::unique_ptr<google::protobuf::FileDescriptorSet> protos_;

  // Protobuf infrastructure which owns the message prototype 'prototype_'.
  std::unique_ptr<google::protobuf::SimpleDescriptorDatabase> db_;
  std::unique_ptr<google::protobuf::DescriptorPool> descriptor_pool_;
  std::unique_ptr<google::protobuf::MessageFactory> message_factory_;
  const google::protobuf::Message* prototype_ = nullptr;

  std::shared_ptr<RandomAccessFile> reader_;
};

// Convenience functions for protobuf containers holding just one record.

// Load a "containerized" protobuf from the given path.
// If the file does not exist, returns Status::NotFound(). Otherwise, may
// return other Status error codes such as Status::IOError.
Status ReadPBContainerFromPath(Env* env, const std::string& path,
                               google::protobuf::Message* msg);

// Serialize a "containerized" protobuf to the given path.
//
// If create == NO_OVERWRITE and 'path' already exists, the function will fail.
// If sync == SYNC, the newly created file will be fsynced before returning.
Status WritePBContainerToPath(Env* env, const std::string& path,
                              const google::protobuf::Message& msg,
                              CreateMode create,
                              SyncMode sync);

// Wrapper for a protobuf message which lazily converts to JSON when
// the trace buffer is dumped.
//
// When tracing, an instance of this class can be associated with
// a given trace, instead of a stringified PB, thus avoiding doing
// stringification inline and moving that work to the tracing process.
//
// Example usage:
//  TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
//                         "response", pb_util::PbTracer::TracePb(*response_pb_),
//                         ...);
//
class PbTracer : public debug::ConvertableToTraceFormat {
 public:
  enum {
    kMaxFieldLengthToTrace = 100
  };

  // Static helper to be called when adding a stringified PB to a trace.
  // This does not actually stringify 'msg', that will be done later
  // when/if AppendAsTraceFormat() is called on the returned object.
  static scoped_refptr<debug::ConvertableToTraceFormat> TracePb(
      const google::protobuf::Message& msg);

  explicit PbTracer(const google::protobuf::Message& msg);

  // Actually stringifies the PB and appends the string to 'out'.
  void AppendAsTraceFormat(std::string* out) const override;
 private:
  const std::unique_ptr<google::protobuf::Message> msg_;
};

} // namespace pb_util

// TODO(todd) Replacing all Message::ToString call sites for KUDU-1812
// is much easier if these are available in the 'kudu' namespace. We should
// consider removing these imports and move them to all call sites.
using pb_util::SecureDebugString; // NOLINT
using pb_util::SecureShortDebugString; // NOLINT

} // namespace kudu
#endif
