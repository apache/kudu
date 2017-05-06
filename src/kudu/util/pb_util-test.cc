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

#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <google/protobuf/descriptor.pb.h>
#include <gtest/gtest.h>

#include "kudu/util/env_util.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/pb_util_test.pb.h"
#include "kudu/util/proto_container_test.pb.h"
#include "kudu/util/proto_container_test2.pb.h"
#include "kudu/util/proto_container_test3.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace pb_util {

using google::protobuf::FileDescriptorSet;
using internal::WritableFileOutputStream;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

static const char* kTestFileName = "pb_container.meta";
static const char* kTestKeyvalName = "my-key";
static const int kTestKeyvalValue = 1;
static const int kUseDefaultVersion = 0; // Use the default container version (don't set it).

class TestPBUtil : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    path_ = GetTestPath(kTestFileName);
  }

 protected:
  // Create a container file with expected values.
  // Since this is a unit test class, and we want it to be fast, we do not
  // fsync by default.
  Status CreateKnownGoodContainerFile(CreateMode create = OVERWRITE,
                                      SyncMode sync = NO_SYNC);

  // Create a new Protobuf Container File Writer.
  // Set version to kUseDefaultVersion to use the default version.
  Status NewPBCWriter(int version, RWFileOptions opts,
                      unique_ptr<WritablePBContainerFile>* pb_writer);

  // Same as CreateKnownGoodContainerFile(), but with settable file version.
  // Set version to kUseDefaultVersion to use the default version.
  Status CreateKnownGoodContainerFileWithVersion(int version,
                                                 CreateMode create = OVERWRITE,
                                                 SyncMode sync = NO_SYNC);

  // XORs the data in the specified range of the file at the given path.
  Status BitFlipFileByteRange(const string& path, uint64_t offset, uint64_t length);

  void DumpPBCToString(const string& path, bool oneline_output, string* ret);

  // Truncate the specified file to the specified length.
  Status TruncateFile(const string& path, uint64_t size);

  // Output file name for most unit tests.
  string path_;
};

// Parameterized test class for running tests across various versions of PB
// container files.
class TestPBContainerVersions : public TestPBUtil,
                                public ::testing::WithParamInterface<int> {
 public:
  TestPBContainerVersions()
      : version_(GetParam()) {
  }

 protected:
  const int version_; // The parameterized container version we are testing.
};

INSTANTIATE_TEST_CASE_P(SupportedVersions, TestPBContainerVersions,
                        ::testing::Values(1, 2, kUseDefaultVersion));

Status TestPBUtil::CreateKnownGoodContainerFile(CreateMode create, SyncMode sync) {
  ProtoContainerTestPB test_pb;
  test_pb.set_name(kTestKeyvalName);
  test_pb.set_value(kTestKeyvalValue);
  return WritePBContainerToPath(env_, path_, test_pb, create, sync);
}

Status TestPBUtil::NewPBCWriter(int version, RWFileOptions opts,
                                unique_ptr<WritablePBContainerFile>* pb_writer) {
  unique_ptr<RWFile> writer;
  RETURN_NOT_OK(env_->NewRWFile(opts, path_, &writer));
  pb_writer->reset(new WritablePBContainerFile(std::move(writer)));
  if (version != kUseDefaultVersion) {
    (*pb_writer)->SetVersionForTests(version);
  }
  return Status::OK();
}

Status TestPBUtil::CreateKnownGoodContainerFileWithVersion(int version,
                                                           CreateMode create,
                                                           SyncMode sync) {
  ProtoContainerTestPB test_pb;
  test_pb.set_name(kTestKeyvalName);
  test_pb.set_value(kTestKeyvalValue);

  unique_ptr<WritablePBContainerFile> pb_writer;
  RETURN_NOT_OK(NewPBCWriter(version, RWFileOptions(), &pb_writer));
  RETURN_NOT_OK(pb_writer->CreateNew(test_pb));
  RETURN_NOT_OK(pb_writer->Append(test_pb));
  RETURN_NOT_OK(pb_writer->Close());
  return Status::OK();
}

Status TestPBUtil::BitFlipFileByteRange(const string& path, uint64_t offset, uint64_t length) {
  faststring buf;
  // Read the data from disk.
  {
    unique_ptr<RandomAccessFile> file;
    RETURN_NOT_OK(env_->NewRandomAccessFile(path, &file));
    uint64_t size;
    RETURN_NOT_OK(file->Size(&size));
    faststring scratch;
    scratch.resize(size);
    Slice slice(scratch.data(), size);
    RETURN_NOT_OK(file->Read(0, &slice));
    buf.append(slice.data(), slice.size());
  }

  // Flip the bits.
  for (uint64_t i = 0; i < length; i++) {
    uint8_t* addr = buf.data() + offset + i;
    *addr = ~*addr;
  }

  // Write the data back to disk.
  unique_ptr<WritableFile> file;
  RETURN_NOT_OK(env_->NewWritableFile(path, &file));
  RETURN_NOT_OK(file->Append(buf));
  RETURN_NOT_OK(file->Close());

  return Status::OK();
}

Status TestPBUtil::TruncateFile(const string& path, uint64_t size) {
  unique_ptr<RWFile> file;
  RWFileOptions opts;
  opts.mode = Env::OPEN_EXISTING;
  RETURN_NOT_OK(env_->NewRWFile(opts, path, &file));
  RETURN_NOT_OK(file->Truncate(size));
  return Status::OK();
}

TEST_F(TestPBUtil, TestWritableFileOutputStream) {
  shared_ptr<WritableFile> file;
  string path = GetTestPath("test.out");
  ASSERT_OK(env_util::OpenFileForWrite(env_, path, &file));

  WritableFileOutputStream stream(file.get(), 4096);

  void* buf;
  int size;

  // First call should yield the whole buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(4096, stream.ByteCount());

  // Backup 1000 and the next call should yield 1000
  stream.BackUp(1000);
  ASSERT_EQ(3096, stream.ByteCount());

  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(1000, size);

  // Another call should flush and yield a new buffer of 4096
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(8192, stream.ByteCount());

  // Should be able to backup to 7192
  stream.BackUp(1000);
  ASSERT_EQ(7192, stream.ByteCount());

  // Flushing shouldn't change written count.
  ASSERT_TRUE(stream.Flush());
  ASSERT_EQ(7192, stream.ByteCount());

  // Since we just flushed, we should get another full buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(7192 + 4096, stream.ByteCount());

  ASSERT_TRUE(stream.Flush());

  ASSERT_EQ(stream.ByteCount(), file->Size());
}

// Basic read/write test.
TEST_F(TestPBUtil, TestPBContainerSimple) {
  // Exercise both the SYNC and NO_SYNC codepaths, despite the fact that we
  // aren't able to observe a difference in the test.
  vector<SyncMode> modes = { SYNC, NO_SYNC };
  for (SyncMode mode : modes) {

    // Write the file.
    ASSERT_OK(CreateKnownGoodContainerFile(NO_OVERWRITE, mode));

    // Read it back, should validate and contain the expected values.
    ProtoContainerTestPB test_pb;
    ASSERT_OK(ReadPBContainerFromPath(env_, path_, &test_pb));
    ASSERT_EQ(kTestKeyvalName, test_pb.name());
    ASSERT_EQ(kTestKeyvalValue, test_pb.value());

    // Delete the file.
    ASSERT_OK(env_->DeleteFile(path_));
  }
}

// Corruption / various failure mode test.
TEST_P(TestPBContainerVersions, TestCorruption) {
  // Test that we indicate when the file does not exist.
  ProtoContainerTestPB test_pb;
  Status s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsNotFound()) << "Should not be found: " << path_ << ": " << s.ToString();

  // Test that an empty file looks like corruption.
  {
    // Create the empty file.
    unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(path_, &file));
    ASSERT_OK(file->Close());
  }
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsIncomplete()) << "Should be zero length: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");

  // Test truncated file.
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  uint64_t known_good_size = 0;
  ASSERT_OK(env_->GetFileSize(path_, &known_good_size));
  ASSERT_OK(TruncateFile(path_, known_good_size - 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect size: " << path_ << ": " << s.ToString();
  } else {
    ASSERT_TRUE(s.IsIncomplete()) << "Should be incorrect size: " << path_ << ": " << s.ToString();
  }
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");

  // Test corrupted magic.
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(BitFlipFileByteRange(path_, 0, 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should have invalid magic: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid magic number");

  // Test corrupted version.
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(BitFlipFileByteRange(path_, 8, 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsNotSupported()) << "Should have unsupported version number: " << path_ << ": "
                                  << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), " Protobuf container has unsupported version");

  // Test corrupted magic+version checksum (only exists in the V2+ format).
  if (version_ >= 2) {
    ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
    ASSERT_OK(BitFlipFileByteRange(path_, 12, 2));
    s = ReadPBContainerFromPath(env_, path_, &test_pb);
    ASSERT_TRUE(s.IsCorruption()) << "Should have corrupted file header checksum: " << path_ << ": "
                                    << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "File header checksum does not match");
  }

  // Test record corruption below.
  const int kFirstRecordOffset = (version_ == 1) ? 12 : 16;

  // Test corrupted data length.
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(BitFlipFileByteRange(path_, kFirstRecordOffset, 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  } else {
    ASSERT_TRUE(s.IsCorruption()) << "Should be invalid data length checksum: "
                                  << path_ << ": " << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
  }

  // Test corrupted data (looks like bad checksum).
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(BitFlipFileByteRange(path_, kFirstRecordOffset + 4, 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect checksum: " << path_ << ": "
                                << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");

  // Test corrupted checksum.
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(BitFlipFileByteRange(path_, known_good_size - 4, 2));
  s = ReadPBContainerFromPath(env_, path_, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect checksum: " << path_ << ": "
                                << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
}

// Test partial record at end of file.
TEST_P(TestPBContainerVersions, TestPartialRecord) {
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  uint64_t known_good_size;
  ASSERT_OK(env_->GetFileSize(path_, &known_good_size));
  ASSERT_OK(TruncateFile(path_, known_good_size - 2));

  unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &file));
  ReadablePBContainerFile pb_file(std::move(file));
  ASSERT_OK(pb_file.Open());
  ProtoContainerTestPB test_pb;
  Status s = pb_file.ReadNextPB(&test_pb);
  // Loop to verify that the same response is repeatably returned.
  for (int i = 0; i < 2; i++) {
    if (version_ == 1) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    } else {
      ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    }
    ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  }
  ASSERT_OK(pb_file.Close());
}

// Test that it is possible to append after a partial write if we truncate the
// partial record. This is only fully supported in V2+.
TEST_P(TestPBContainerVersions, TestAppendAfterPartialWrite) {
  uint64_t known_good_size;
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ASSERT_OK(env_->GetFileSize(path_, &known_good_size));

  unique_ptr<WritablePBContainerFile> writer;
  RWFileOptions opts;
  opts.mode = Env::OPEN_EXISTING;
  ASSERT_OK(NewPBCWriter(version_, opts, &writer));
  ASSERT_OK(writer->OpenExisting());

  ASSERT_OK(TruncateFile(path_, known_good_size - 2));

  unique_ptr<RandomAccessFile> file;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &file));
  ReadablePBContainerFile reader(std::move(file));
  ASSERT_OK(reader.Open());
  ProtoContainerTestPB test_pb;
  Status s = reader.ReadNextPB(&test_pb);
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");
  if (version_ == 1) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    return; // The rest of the test does not apply to version 1.
  }
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();

  // Now truncate cleanly.
  ASSERT_OK(TruncateFile(path_, reader.offset()));
  s = reader.ReadNextPB(&test_pb);
  ASSERT_TRUE(s.IsEndOfFile()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Reached end of file");

  // Reopen the writer to allow appending more records.
  // Append a record and read it back.
  ASSERT_OK(NewPBCWriter(version_, opts, &writer));
  ASSERT_OK(writer->OpenExisting());
  test_pb.set_name("hello");
  test_pb.set_value(1);
  ASSERT_OK(writer->Append(test_pb));
  test_pb.Clear();
  ASSERT_OK(reader.ReadNextPB(&test_pb));
  ASSERT_EQ("hello", test_pb.name());
  ASSERT_EQ(1, test_pb.value());
}

// Simple test for all versions.
TEST_P(TestPBContainerVersions, TestSingleMessage) {
  ASSERT_OK(CreateKnownGoodContainerFileWithVersion(version_));
  ProtoContainerTestPB test_pb;
  ASSERT_OK(ReadPBContainerFromPath(env_, path_, &test_pb));
  ASSERT_EQ(kTestKeyvalName, test_pb.name());
  ASSERT_EQ(kTestKeyvalValue, test_pb.value());
}

TEST_P(TestPBContainerVersions, TestMultipleMessages) {
  ProtoContainerTestPB pb;
  pb.set_name("foo");
  pb.set_note("bar");

  unique_ptr<WritablePBContainerFile> pb_writer;
  ASSERT_OK(NewPBCWriter(version_, RWFileOptions(), &pb_writer));
  ASSERT_OK(pb_writer->CreateNew(pb));

  for (int i = 0; i < 10; i++) {
    pb.set_value(i);
    ASSERT_OK(pb_writer->Append(pb));
  }
  ASSERT_OK(pb_writer->Close());

  int pbs_read = 0;
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));
  ASSERT_OK(pb_reader.Open());
  for (int i = 0;; i++) {
    ProtoContainerTestPB read_pb;
    Status s = pb_reader.ReadNextPB(&read_pb);
    if (s.IsEndOfFile()) {
      break;
    }
    ASSERT_OK(s);
    ASSERT_EQ(pb.name(), read_pb.name());
    ASSERT_EQ(read_pb.value(), i);
    ASSERT_EQ(pb.note(), read_pb.note());
    pbs_read++;
  }
  ASSERT_EQ(10, pbs_read);
  ASSERT_OK(pb_reader.Close());
}

TEST_P(TestPBContainerVersions, TestInterleavedReadWrite) {
  ProtoContainerTestPB pb;
  pb.set_name("foo");
  pb.set_note("bar");

  // Open the file for writing and reading.
  unique_ptr<WritablePBContainerFile> pb_writer;
  ASSERT_OK(NewPBCWriter(version_, RWFileOptions(), &pb_writer));
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path_, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));

  // Write the header (writer) and validate it (reader).
  ASSERT_OK(pb_writer->CreateNew(pb));
  ASSERT_OK(pb_reader.Open());

  for (int i = 0; i < 10; i++) {
    // Write a message and read it back.
    pb.set_value(i);
    ASSERT_OK(pb_writer->Append(pb));
    ProtoContainerTestPB read_pb;
    ASSERT_OK(pb_reader.ReadNextPB(&read_pb));
    ASSERT_EQ(pb.name(), read_pb.name());
    ASSERT_EQ(read_pb.value(), i);
    ASSERT_EQ(pb.note(), read_pb.note());
  }

  // After closing the writer, the reader should be out of data.
  ASSERT_OK(pb_writer->Close());
  ASSERT_TRUE(pb_reader.ReadNextPB(nullptr).IsEndOfFile());
  ASSERT_OK(pb_reader.Close());
}

TEST_F(TestPBUtil, TestPopulateDescriptorSet) {
  {
    // No dependencies --> just one proto.
    ProtoContainerTestPB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(1, protos.file_size());
  }
  {
    // One direct dependency --> two protos.
    ProtoContainerTest2PB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(2, protos.file_size());
  }
  {
    // One direct and one indirect dependency --> three protos.
    ProtoContainerTest3PB pb;
    FileDescriptorSet protos;
    WritablePBContainerFile::PopulateDescriptorSet(
        pb.GetDescriptor()->file(), &protos);
    ASSERT_EQ(3, protos.file_size());
  }
}

void TestPBUtil::DumpPBCToString(const string& path, bool oneline_output,
                                 string* ret) {
  unique_ptr<RandomAccessFile> reader;
  ASSERT_OK(env_->NewRandomAccessFile(path, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));
  ASSERT_OK(pb_reader.Open());
  ostringstream oss;
  ASSERT_OK(pb_reader.Dump(&oss, oneline_output));
  ASSERT_OK(pb_reader.Close());
  *ret = oss.str();
}

TEST_P(TestPBContainerVersions, TestDumpPBContainer) {
  const char* kExpectedOutput =
      "Message 0\n"
      "-------\n"
      "record_one {\n"
      "  name: \"foo\"\n"
      "  value: 0\n"
      "}\n"
      "record_two {\n"
      "  record {\n"
      "    name: \"foo\"\n"
      "    value: 0\n"
      "  }\n"
      "}\n"
      "\n"
      "Message 1\n"
      "-------\n"
      "record_one {\n"
      "  name: \"foo\"\n"
      "  value: 1\n"
      "}\n"
      "record_two {\n"
      "  record {\n"
      "    name: \"foo\"\n"
      "    value: 2\n"
      "  }\n"
      "}\n\n";

  const char* kExpectedOutputShort =
    "0\trecord_one { name: \"foo\" value: 0 } record_two { record { name: \"foo\" value: 0 } }\n"
    "1\trecord_one { name: \"foo\" value: 1 } record_two { record { name: \"foo\" value: 2 } }\n";

  ProtoContainerTest3PB pb;
  pb.mutable_record_one()->set_name("foo");
  pb.mutable_record_two()->mutable_record()->set_name("foo");

  unique_ptr<WritablePBContainerFile> pb_writer;
  ASSERT_OK(NewPBCWriter(version_, RWFileOptions(), &pb_writer));
  ASSERT_OK(pb_writer->CreateNew(pb));

  for (int i = 0; i < 2; i++) {
    pb.mutable_record_one()->set_value(i);
    pb.mutable_record_two()->mutable_record()->set_value(i*2);
    ASSERT_OK(pb_writer->Append(pb));
  }
  ASSERT_OK(pb_writer->Close());

  string output;
  NO_FATALS(DumpPBCToString(path_, false, &output));
  ASSERT_STREQ(kExpectedOutput, output.c_str());

  NO_FATALS(DumpPBCToString(path_, true, &output));
  ASSERT_STREQ(kExpectedOutputShort, output.c_str());
}

TEST_F(TestPBUtil, TestOverwriteExistingPB) {
  ASSERT_OK(CreateKnownGoodContainerFile(NO_OVERWRITE));
  ASSERT_TRUE(CreateKnownGoodContainerFile(NO_OVERWRITE).IsAlreadyPresent());
  ASSERT_OK(CreateKnownGoodContainerFile(OVERWRITE));
  ASSERT_OK(CreateKnownGoodContainerFile(OVERWRITE));
}

TEST_F(TestPBUtil, TestRedaction) {
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
  TestSecurePrintingPB pb;

  pb.set_insecure1("public 1");
  pb.set_insecure2("public 2");
  pb.set_secure1("private 1");
  pb.set_secure2("private 2");
  pb.add_repeated_secure("private 3");
  pb.add_repeated_secure("private 4");
  pb.set_insecure3("public 3");

  for (auto s : {SecureDebugString(pb), SecureShortDebugString(pb)}) {
    ASSERT_EQ(string::npos, s.find("private"));
    ASSERT_STR_CONTAINS(s, "<redacted>");
    ASSERT_STR_CONTAINS(s, "public 1");
    ASSERT_STR_CONTAINS(s, "public 2");
    ASSERT_STR_CONTAINS(s, "public 3");
  }

  // If we disable redaction, we should see the private fields.
  ASSERT_NE("", gflags::SetCommandLineOption("redact", ""));
  ASSERT_STR_CONTAINS(SecureDebugString(pb), "private");
}

} // namespace pb_util
} // namespace kudu
