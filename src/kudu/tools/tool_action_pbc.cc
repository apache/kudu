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

#include <cstdio>
#include <cstdlib>
#include <fstream>  // IWYU pragma: keep
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/error/en.h>
#include <rapidjson/error/error.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;
using std::cout;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_bool(oneline, false, "Print each protobuf on a single line");
TAG_FLAG(oneline, stable);

DEFINE_bool(json, false, "Print protobufs in JSON format");
TAG_FLAG(json, stable);

DEFINE_bool(json_pretty, false, "Print or edit protobufs in JSON pretty format");
TAG_FLAG(json_pretty, evolving);

DEFINE_bool(debug, false, "Print extra debugging information about each protobuf");
TAG_FLAG(debug, stable);

DEFINE_bool(backup, true, "Write a backup file");

bool ValidatePBCFlags() {
  int count = 0;
  if (FLAGS_debug) count++;
  if (FLAGS_oneline) count++;
  if (FLAGS_json) count++;
  if (FLAGS_json_pretty) count++;
  if (count > 1) {
    LOG(ERROR) << "only one of --debug, --oneline, --json or --json_pretty "
                  "can be provided at most";
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(validate_pbc_flags, ValidatePBCFlags);

namespace kudu {

using pb_util::ReadablePBContainerFile;
using strings::Substitute;

namespace tools {

namespace {

const char* const kPathArg = "path";

bool IsFileEncrypted(Env* env, const std::string& fname) {
  if (!env->IsEncryptionEnabled()) {
    return false;
  }
  RandomAccessFileOptions opts;
  opts.is_sensitive = true;
  unique_ptr<RandomAccessFile> reader;
  return env->NewRandomAccessFile(opts, fname, &reader).ok();
}

Status DumpPBContainerFile(const RunnerContext& context) {
  const string& path = FindOrDie(context.required_args, kPathArg);
  auto format = ReadablePBContainerFile::Format::DEFAULT;
  if (FLAGS_json) {
    format = ReadablePBContainerFile::Format::JSON;
  } else if (FLAGS_json_pretty) {
    format = ReadablePBContainerFile::Format::JSON_PRETTY;
  } else if (FLAGS_oneline) {
    format = ReadablePBContainerFile::Format::ONELINE;
  } else if (FLAGS_debug) {
    format = ReadablePBContainerFile::Format::DEBUG;
  }

  RETURN_NOT_OK(SetServerKey());
  Env* env = Env::Default();
  unique_ptr<RandomAccessFile> reader;
  RandomAccessFileOptions opts;
  opts.is_sensitive = IsFileEncrypted(env, path);
  RETURN_NOT_OK(env->NewRandomAccessFile(opts, path, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));
  RETURN_NOT_OK(pb_reader.Open());
  RETURN_NOT_OK(pb_reader.Dump(&std::cout, format));

  return Status::OK();
}

// Run the user's configured editor on 'path'.
Status RunEditor(const string& path) {
  const char* editor = getenv("EDITOR");
  if (!editor) {
    editor = "vi";
  }
  Subprocess editor_proc({editor, path});
  editor_proc.ShareParentStdin();
  editor_proc.ShareParentStdout();
  editor_proc.ShareParentStderr();
  RETURN_NOT_OK_PREPEND(editor_proc.Start(), "couldn't start editor");
  int ret = 0;
  RETURN_NOT_OK_PREPEND(editor_proc.Wait(&ret), "edit failed");
  if (ret != 0) {
    return Status::Aborted("editor returned non-zero exit code");
  }
  return Status::OK();
}

Status EditFile(const RunnerContext& context) {
  RETURN_NOT_OK(SetServerKey());
  Env* env = Env::Default();
  const string& path = FindOrDie(context.required_args, kPathArg);
  const string& dir = DirName(path);

  // Open the original file.
  unique_ptr<RandomAccessFile> reader;
  RandomAccessFileOptions reader_opts;
  reader_opts.is_sensitive = IsFileEncrypted(env, path);
  RETURN_NOT_OK(env->NewRandomAccessFile(reader_opts, path, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));
  RETURN_NOT_OK(pb_reader.Open());

  // Make a new RWFile where we'll write the changed PBC file.
  // Do this up front so that we fail early if the user doesn't have appropriate permissions.
  const string tmp_out_path = path + ".new";
  unique_ptr<RWFile> out_rwfile;
  RWFileOptions out_rwfile_opts;
  out_rwfile_opts.is_sensitive = IsFileEncrypted(env, path);
  RETURN_NOT_OK_PREPEND(env->NewRWFile(out_rwfile_opts, tmp_out_path, &out_rwfile),
                        "couldn't open output PBC file");
  auto delete_tmp_output = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env->DeleteFile(tmp_out_path),
                "Could not delete file " + tmp_out_path);
  });

  // Also make a tmp file where we'll write the PBC in JSON format for
  // easy editing. Encryption needs to be disabled for the tmp file.
  unique_ptr<WritableFile> tmp_json_file;
  string tmp_json_path;
  WritableFileOptions tmp_json_opts;
  tmp_json_opts.is_sensitive = false;
  const string tmp_template = Substitute("pbc-edit$0.XXXXXX", kTmpInfix);
  RETURN_NOT_OK_PREPEND(env->NewTempWritableFile(tmp_json_opts,
                                                 JoinPathSegments(dir, tmp_template),
                                                 &tmp_json_path, &tmp_json_file),
                        "couldn't create temporary file");
  auto delete_tmp_json = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env->DeleteFile(tmp_json_path),
                "Could not delete file " + tmp_json_path);
  });

  // Dump the contents in JSON to the temporary file.
  {
    // It is quite difficult to get a C++ ostream pointed at a temporary file,
    // so we just dump to a string and then write it to a file.
    std::ostringstream stream;
    ReadablePBContainerFile::Format format =
        FLAGS_json_pretty ? ReadablePBContainerFile::Format::JSON_PRETTY :
                            ReadablePBContainerFile::Format::JSON;
    RETURN_NOT_OK(pb_reader.Dump(&stream, format));
    RETURN_NOT_OK_PREPEND(tmp_json_file->Append(stream.str()), "couldn't write to temporary file");
    RETURN_NOT_OK_PREPEND(tmp_json_file->Close(), "couldn't close temporary file");
  }

  // Open the temporary file in the editor for the user to edit, and load the content
  // back into a list of lines.
  RETURN_NOT_OK(RunEditor(tmp_json_path));

  {
    const google::protobuf::Message* prototype;
    RETURN_NOT_OK_PREPEND(pb_reader.GetPrototype(&prototype),
                          "couldn't load message prototype from file");

    pb_util::WritablePBContainerFile pb_writer(std::shared_ptr<RWFile>(out_rwfile.release()));
    RETURN_NOT_OK_PREPEND(pb_writer.CreateNew(*prototype), "couldn't init PBC writer");

    // Parse the edited file.
    unique_ptr<google::protobuf::Message> m(prototype->New());

    FILE* fp = nullptr;
    POINTER_RETRY_ON_EINTR(fp, fopen(tmp_json_path.c_str(), "r"));
    if (fp == nullptr) {
      return Status::IOError(Substitute("open file ($0) failed", tmp_json_path));
    }
    SCOPED_CLEANUP({ fclose(fp); });

    char read_buffer[65536];
    rapidjson::FileReadStream in_stream(fp, read_buffer, sizeof(read_buffer));

    JsonParseOptions opts;
    opts.case_insensitive_enum_parsing = true;
    do {
      // The file may contains multiple JSON objects, parse one object once.
      rapidjson::Document document;
      document.ParseStream<rapidjson::kParseStopWhenDoneFlag>(in_stream);
      if (document.HasParseError()) {
        auto code = document.GetParseError();
        if (code != rapidjson::kParseErrorDocumentEmpty) {
          return Status::Corruption("JSON text is corrupt",
                                    rapidjson::GetParseError_En(code));
        }
        // No more JSON object left.
        break;
      }

      // Convert one JSON object to protobuf object once.
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      document.Accept(writer);
      m->Clear();
      auto str = buffer.GetString();
      const auto& google_status = JsonStringToMessage(str, m.get(), opts);
      if (!google_status.ok()) {
        return Status::InvalidArgument(
            Substitute("Unable to parse JSON text: $0", str),
            google_status.error_message().ToString());
      }

      // Append the protobuf object to writer.
      RETURN_NOT_OK_PREPEND(pb_writer.Append(*m), "unable to append PB to output");
    } while (true);
    RETURN_NOT_OK_PREPEND(pb_writer.Sync(), "failed to sync output");
    RETURN_NOT_OK_PREPEND(pb_writer.Close(), "failed to close output");
  }

  // We successfully wrote the new file.
  if (FLAGS_backup) {
    // Move the old file to a backup location.
    string backup_path = Substitute("$0.bak.$1", path, GetCurrentTimeMicros());
    RETURN_NOT_OK_PREPEND(env->RenameFile(path, backup_path),
                          "couldn't back up original file");
    LOG(INFO) << "Moved original file to " << backup_path;
  }
  // Move the new file to the final location.
  RETURN_NOT_OK_PREPEND(env->RenameFile(tmp_out_path, path),
                        "couldn't move new file into place");
  delete_tmp_output.cancel();
  WARN_NOT_OK(env->SyncDir(dir), "couldn't sync directory");

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildPbcMode() {
  unique_ptr<Action> dump =
      ActionBuilder("dump", &DumpPBContainerFile)
      .Description("Dump a PBC (protobuf container) file")
      .AddRequiredParameter({kPathArg, "path to PBC file"})
      .AddOptionalParameter("debug")
      .AddOptionalParameter("oneline")
      .AddOptionalParameter("json")
      .AddOptionalParameter("json_pretty")
      .Build();

  unique_ptr<Action> edit =
      ActionBuilder("edit", &EditFile)
      .Description("Edit a PBC (protobuf container) file")
      .AddRequiredParameter({kPathArg, "path to PBC file"})
      .AddOptionalParameter("backup")
      .AddOptionalParameter("json")
      .AddOptionalParameter("json_pretty")
      .Build();

  return ModeBuilder("pbc")
      .Description("Operate on PBC (protobuf container) files")
      .AddAction(std::move(dump))
      .AddAction(std::move(edit))
      .Build();
}

} // namespace tools
} // namespace kudu
