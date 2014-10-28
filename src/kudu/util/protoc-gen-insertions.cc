// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Simple protoc plugin which inserts some code at the top of each generated protobuf.
// Currently, this just adds an include of protobuf-annotations.h, a file which hooks up
// the protobuf concurrency annotations to our TSAN annotations.
#include <glog/logging.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"

using google::protobuf::io::ZeroCopyOutputStream;
using google::protobuf::io::Printer;

namespace kudu {

static const char* const kIncludeToInsert = "#include \"kudu/util/protobuf-annotations.h\"\n";
static const char* const kProtoExtension = ".proto";

class InsertAnnotations : public ::google::protobuf::compiler::CodeGenerator {
  virtual bool Generate(const google::protobuf::FileDescriptor *file,
                        const std::string &/*param*/,
                        google::protobuf::compiler::GeneratorContext *gen_context,
                        std::string *error) const OVERRIDE {

    // Determine the file name we will substitute into.
    string path_no_extension;
    if (!TryStripSuffixString(file->name(), kProtoExtension, &path_no_extension)) {
      *error = strings::Substitute("file name $0 did not end in $1", file->name(), kProtoExtension);
      return false;
    }
    string pb_file = path_no_extension + ".pb.cc";

    // Actually insert the new #include
    gscoped_ptr<ZeroCopyOutputStream> inserter(gen_context->OpenForInsert(pb_file, "includes"));
    Printer printer(inserter.get(), '$');
    printer.Print(kIncludeToInsert);

    if (printer.failed()) {
      *error = "Failed to print to output file";
      return false;
    }

    return true;
  }
};

} // namespace kudu

int main(int argc, char *argv[]) {
  kudu::InsertAnnotations generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
