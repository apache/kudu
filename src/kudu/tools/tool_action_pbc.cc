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

#include "kudu/tools/tool_action.h"

#include <cstdlib>
#include <exception>
#include <fstream>  // IWYU pragma: keep
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

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

using std::cout;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_bool(oneline, false, "print each protobuf on a single line");
TAG_FLAG(oneline, stable);

DEFINE_bool(json, false, "print protobufs in JSON format");
TAG_FLAG(json, stable);

namespace kudu {

using pb_util::ReadablePBContainerFile;
using strings::Substitute;

namespace tools {

namespace {

const char* const kPathArg = "path";

Status DumpPBContainerFile(const RunnerContext& context) {
  if (FLAGS_oneline && FLAGS_json) {
    return Status::InvalidArgument("only one of --json or --oneline may be provided");
  }

  const string& path = FindOrDie(context.required_args, kPathArg);
  auto format = ReadablePBContainerFile::Format::DEFAULT;
  if (FLAGS_json) {
    format = ReadablePBContainerFile::Format::JSON;
  } else if (FLAGS_oneline) {
    format = ReadablePBContainerFile::Format::ONELINE;
  }

  Env* env = Env::Default();
  unique_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &reader));
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

Status LoadFileToLines(const string& path,
                       vector<string>* lines) {
  try {
    string line;
    std::ifstream f(path);
    while (std::getline(f, line)) {
      lines->push_back(line);
    }
  } catch (const std::exception& e) {
    return Status::IOError(e.what());
  }
  return Status::OK();

}

Status EditFile(const RunnerContext& context) {
  Env* env = Env::Default();
  const string& path = FindOrDie(context.required_args, kPathArg);
  const string& dir = DirName(path);

  // Open the original file.
  unique_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &reader));
  ReadablePBContainerFile pb_reader(std::move(reader));
  RETURN_NOT_OK(pb_reader.Open());

  // Make a new RWFile where we'll write the changed PBC file.
  // Do this up front so that we fail early if the user doesn't have appropriate permissions.
  const string tmp_out_path = path + ".new";
  unique_ptr<RWFile> out_rwfile;
  RETURN_NOT_OK_PREPEND(env->NewRWFile(tmp_out_path, &out_rwfile), "couldn't open output PBC file");
  auto delete_tmp_output = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env->DeleteFile(tmp_out_path),
                "Could not delete file " + tmp_out_path);
  });

  // Also make a tmp file where we'll write the PBC in JSON format for
  // easy editing.
  unique_ptr<WritableFile> tmp_json_file;
  string tmp_json_path;
  const string tmp_template = Substitute("pbc-edit$0.XXXXXX", kTmpInfix);
  RETURN_NOT_OK_PREPEND(env->NewTempWritableFile(WritableFileOptions(),
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
    RETURN_NOT_OK(pb_reader.Dump(&stream, ReadablePBContainerFile::Format::JSON));
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
    vector<string> lines;
    RETURN_NOT_OK(LoadFileToLines(tmp_json_path, &lines));
    for (const string& l : lines) {
      m->Clear();
      const auto& google_status = google::protobuf::util::JsonStringToMessage(l, m.get());
      if (!google_status.ok()) {
        return Status::InvalidArgument(
            Substitute("Unable to parse JSON line: $0", l),
            google_status.error_message().ToString());
      }
      RETURN_NOT_OK_PREPEND(pb_writer.Append(*m), "unable to append PB to output");
    }
    RETURN_NOT_OK_PREPEND(pb_writer.Sync(), "failed to sync output");
    RETURN_NOT_OK_PREPEND(pb_writer.Close(), "failed to close output");
  }
  // We successfully wrote the new file. Move the old file to a backup location,
  // and move the new one to the final location.
  string backup_path = Substitute("$0.bak.$1", path, GetCurrentTimeMicros());
  RETURN_NOT_OK_PREPEND(env->RenameFile(path, backup_path),
                        "couldn't back up original file");
  LOG(INFO) << "Moved original file to " << backup_path;
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
      .AddOptionalParameter("oneline")
      .AddOptionalParameter("json")
      .AddRequiredParameter({kPathArg, "path to PBC file"})
      .Build();

  unique_ptr<Action> edit =
      ActionBuilder("edit", &EditFile)
      .Description("Edit a PBC (protobuf container) file")
      .AddRequiredParameter({kPathArg, "path to PBC file"})
      .Build();

  return ModeBuilder("pbc")
      .Description("Operate on PBC (protobuf container) files")
      .AddAction(std::move(dump))
      .AddAction(std::move(edit))
      .Build();
}

} // namespace tools
} // namespace kudu
