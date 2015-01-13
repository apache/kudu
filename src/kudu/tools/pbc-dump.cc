// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <iostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using google::protobuf::Descriptor;
using google::protobuf::DescriptorPool;
using google::protobuf::DynamicMessageFactory;
using google::protobuf::Message;
using kudu::Status;
using std::cerr;
using std::cout;
using std::endl;
using std::string;
using strings::Substitute;

namespace kudu {
namespace pb_util {

Status DumpPBContainerFile(const string& filename) {
  // Load the container file.
  Env* env = Env::Default();
  gscoped_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env->NewRandomAccessFile(filename, &reader));
  ReadablePBContainerFile pb_reader(reader.Pass());
  RETURN_NOT_OK(pb_reader.Init());

  // Use the embedded protobuf information from the container file to
  // create the appropriate kind of protobuf Message.
  DescriptorPool pool;
  if (!pool.BuildFile(*pb_reader.proto())) {
    return Status::Corruption("Descriptor not loaded", Substitute(
        "Could not load descriptor for PB type $0 referenced in container file",
        pb_reader.pb_type()));
  }
  const Descriptor* desc = pool.FindMessageTypeByName(pb_reader.pb_type());
  if (!desc) {
    return Status::NotFound("Descriptor not found", Substitute(
        "Could not find descriptor for PB type $0 referenced in container file",
        pb_reader.pb_type()));
  }
  DynamicMessageFactory factory;
  const Message* prototype = factory.GetPrototype(desc);
  if (!prototype) {
    return Status::NotSupported("Descriptor not supported", Substitute(
        "Descriptor $0 referenced in container file not supported",
        pb_reader.pb_type()));
  }
  gscoped_ptr<Message> msg(prototype->New());

  // Dump each message in the container file.
  int count = 0;
  Status s;
  for (s = pb_reader.ReadNextPB(msg.get());
      s.ok();
      s = pb_reader.ReadNextPB(msg.get())) {
    cout << "Message " << count << endl;
    cout << "-------" << endl;
    cout << msg->DebugString() << endl;
    count++;
  }
  return s.IsEndOfFile() ? s.OK() : s;
}

} // namespace pb_util
} // namespace kudu

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    cerr << "usage: " << argv[0] << " <protobuf container filename>" << endl;
    return 2;
  }

  Status s = kudu::pb_util::DumpPBContainerFile(argv[1]);
  if (s.ok()) {
    return 0;
  } else {
    cerr << s.ToString() << endl;
    return 1;
  }
}
