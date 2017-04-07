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

#include "kudu/util/flags.h"

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>

#include <gflags/gflags.h>
#include <gperftools/heap-profiler.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/os-util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/string_case.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/version_info.h"

using google::CommandLineFlagInfo;

using std::cout;
using std::endl;
using std::string;
using std::stringstream;
using std::unordered_set;

using strings::Substitute;

// Because every binary initializes its flags here, we use it as a convenient place
// to offer some global flags as well.
DEFINE_bool(dump_metrics_json, false,
            "Dump a JSON document describing all of the metrics which may be emitted "
            "by this binary.");
TAG_FLAG(dump_metrics_json, hidden);

DEFINE_bool(enable_process_lifetime_heap_profiling, false, "Enables heap "
    "profiling for the lifetime of the process. Profile output will be stored in the "
    "directory specified by -heap_profile_path. Enabling this option will disable the "
    "on-demand/remote server profile handlers.");
TAG_FLAG(enable_process_lifetime_heap_profiling, stable);
TAG_FLAG(enable_process_lifetime_heap_profiling, advanced);

DEFINE_string(heap_profile_path, "", "Output path to store heap profiles. If not set " \
    "profiles are stored in /tmp/<process-name>.<pid>.<n>.heap.");
TAG_FLAG(heap_profile_path, stable);
TAG_FLAG(heap_profile_path, advanced);

DEFINE_bool(disable_core_dumps, false, "Disable core dumps when this process crashes.");
TAG_FLAG(disable_core_dumps, advanced);
TAG_FLAG(disable_core_dumps, evolving);

DEFINE_string(umask, "077",
              "The umask that will be used when creating files and directories. "
              "Permissions of top-level data directories will also be modified at "
              "start-up to conform to the given umask. Changing this value may "
              "enable unauthorized local users to read or modify data stored by Kudu.");
TAG_FLAG(umask, advanced);

static bool ValidateUmask(const char* /*flagname*/, const string& value) {
  uint32_t parsed;
  if (!safe_strtou32_base(value.c_str(), &parsed, 8)) {
    LOG(ERROR) << "Invalid umask: must be an octal string";
    return false;
  }

  // Verify that the umask doesn't restrict the permissions of the owner.
  // If it did, we'd end up creating files that we can't read.
  if ((parsed & 0700) != 0) {
    LOG(ERROR) << "Invalid umask value: must not restrict owner permissions";
    return false;
  }
  return true;
}

DEFINE_validator(umask, &ValidateUmask);

DEFINE_bool(unlock_experimental_flags, false,
            "Unlock flags marked as 'experimental'. These flags are not guaranteed to "
            "be maintained across releases of Kudu, and may enable features or behavior "
            "known to be unstable. Use at your own risk.");
TAG_FLAG(unlock_experimental_flags, advanced);
TAG_FLAG(unlock_experimental_flags, stable);

DEFINE_bool(unlock_unsafe_flags, false,
            "Unlock flags marked as 'unsafe'. These flags are not guaranteed to "
            "be maintained across releases of Kudu, and enable features or behavior "
            "known to be unsafe. Use at your own risk.");
TAG_FLAG(unlock_unsafe_flags, advanced);
TAG_FLAG(unlock_unsafe_flags, stable);

DEFINE_string(redact, "all",
              "Comma-separated list of redactions. Supported options are 'flag', "
              "'log', 'all', and 'none'. If 'flag' is specified, configuration flags which may "
              "include sensitive data will be redacted whenever server configuration "
              "is emitted. If 'log' is specified, row data will be redacted from log "
              "and error messages. If 'all' is specified, all of above will be redacted. "
              "If 'none' is specified, no redaction will occur.");
TAG_FLAG(redact, advanced);
TAG_FLAG(redact, evolving);

static bool ValidateRedact(const char* /*flagname*/, const string& value) {
  kudu::g_should_redact_log = false;
  kudu::g_should_redact_flag = false;

  // Flag value is case insensitive.
  string redact_flags;
  kudu::ToUpperCase(value, &redact_flags);

  // 'all', 'none', and '' must be specified without any other option.
  if (redact_flags == "ALL") {
    kudu::g_should_redact_log = true;
    kudu::g_should_redact_flag = true;
    return true;
  }
  if (redact_flags == "NONE" || redact_flags.empty()) {
    return true;
  }

  for (const auto& t : strings::Split(redact_flags, ",", strings::SkipEmpty())) {
    if (t == "LOG") {
      kudu::g_should_redact_log = true;
    } else if (t == "FLAG") {
      kudu::g_should_redact_flag = true;
    } else if (t == "ALL" || t == "NONE") {
      LOG(ERROR) << "Invalid redaction options: "
                 << value << ", '" << t << "' must be specified by itself.";
      return false;
    } else {
      LOG(ERROR) << "Invalid redaction type: " << t <<
                    ". Available types are 'flag', 'log', 'all', and 'none'.";
      return false;
    }
  }
  return true;
}

DEFINE_validator(redact, &ValidateRedact);
// Tag a bunch of the flags that we inherit from glog/gflags.

//------------------------------------------------------------
// GLog flags
//------------------------------------------------------------
// Most of these are considered stable. The ones related to email are
// marked unsafe because sending email inline from a server is a pretty
// bad idea.
DECLARE_string(alsologtoemail);
TAG_FLAG(alsologtoemail, hidden);
TAG_FLAG(alsologtoemail, unsafe);

// --alsologtostderr is deprecated in favor of --stderrthreshold
DECLARE_bool(alsologtostderr);
TAG_FLAG(alsologtostderr, hidden);
TAG_FLAG(alsologtostderr, runtime);

DECLARE_bool(colorlogtostderr);
TAG_FLAG(colorlogtostderr, stable);
TAG_FLAG(colorlogtostderr, runtime);

DECLARE_bool(drop_log_memory);
TAG_FLAG(drop_log_memory, advanced);
TAG_FLAG(drop_log_memory, runtime);

DECLARE_string(log_backtrace_at);
TAG_FLAG(log_backtrace_at, advanced);

DECLARE_string(log_dir);
TAG_FLAG(log_dir, stable);

DECLARE_string(log_link);
TAG_FLAG(log_link, stable);
TAG_FLAG(log_link, advanced);

DECLARE_bool(log_prefix);
TAG_FLAG(log_prefix, stable);
TAG_FLAG(log_prefix, advanced);
TAG_FLAG(log_prefix, runtime);

DECLARE_int32(logbuflevel);
TAG_FLAG(logbuflevel, advanced);
TAG_FLAG(logbuflevel, runtime);
DECLARE_int32(logbufsecs);
TAG_FLAG(logbufsecs, advanced);
TAG_FLAG(logbufsecs, runtime);

DECLARE_int32(logemaillevel);
TAG_FLAG(logemaillevel, hidden);
TAG_FLAG(logemaillevel, unsafe);

DECLARE_string(logmailer);
TAG_FLAG(logmailer, hidden);

DECLARE_bool(logtostderr);
TAG_FLAG(logtostderr, stable);
TAG_FLAG(logtostderr, runtime);

DECLARE_int32(max_log_size);
TAG_FLAG(max_log_size, stable);
TAG_FLAG(max_log_size, runtime);

DECLARE_int32(minloglevel);
TAG_FLAG(minloglevel, stable);
TAG_FLAG(minloglevel, advanced);
TAG_FLAG(minloglevel, runtime);

DECLARE_int32(stderrthreshold);
TAG_FLAG(stderrthreshold, stable);
TAG_FLAG(stderrthreshold, advanced);
TAG_FLAG(stderrthreshold, runtime);

DECLARE_bool(stop_logging_if_full_disk);
TAG_FLAG(stop_logging_if_full_disk, stable);
TAG_FLAG(stop_logging_if_full_disk, advanced);
TAG_FLAG(stop_logging_if_full_disk, runtime);

DECLARE_int32(v);
TAG_FLAG(v, stable);
TAG_FLAG(v, advanced);
TAG_FLAG(v, runtime);

DECLARE_string(vmodule);
TAG_FLAG(vmodule, stable);
TAG_FLAG(vmodule, advanced);

DECLARE_bool(symbolize_stacktrace);
TAG_FLAG(symbolize_stacktrace, stable);
TAG_FLAG(symbolize_stacktrace, runtime);
TAG_FLAG(symbolize_stacktrace, advanced);

//------------------------------------------------------------
// GFlags flags
//------------------------------------------------------------
DECLARE_string(flagfile);
TAG_FLAG(flagfile, stable);

DECLARE_string(fromenv);
TAG_FLAG(fromenv, stable);
TAG_FLAG(fromenv, advanced);

DECLARE_string(tryfromenv);
TAG_FLAG(tryfromenv, stable);
TAG_FLAG(tryfromenv, advanced);

DECLARE_string(undefok);
TAG_FLAG(undefok, stable);
TAG_FLAG(undefok, advanced);

DECLARE_int32(tab_completion_columns);
TAG_FLAG(tab_completion_columns, stable);
TAG_FLAG(tab_completion_columns, hidden);

DECLARE_string(tab_completion_word);
TAG_FLAG(tab_completion_word, stable);
TAG_FLAG(tab_completion_word, hidden);

DECLARE_bool(help);
TAG_FLAG(help, stable);

DECLARE_bool(helpfull);
// We hide -helpfull because it's the same as -help for now.
TAG_FLAG(helpfull, stable);
TAG_FLAG(helpfull, hidden);

DECLARE_string(helpmatch);
TAG_FLAG(helpmatch, stable);
TAG_FLAG(helpmatch, advanced);

DECLARE_string(helpon);
TAG_FLAG(helpon, stable);
TAG_FLAG(helpon, advanced);

DECLARE_bool(helppackage);
TAG_FLAG(helppackage, stable);
TAG_FLAG(helppackage, advanced);

DECLARE_bool(helpshort);
TAG_FLAG(helpshort, stable);
TAG_FLAG(helpshort, advanced);

DECLARE_bool(helpxml);
TAG_FLAG(helpxml, stable);
TAG_FLAG(helpxml, advanced);

DECLARE_bool(version);
TAG_FLAG(version, stable);

namespace kudu {

// After flags have been parsed, the umask value is filled in here.
uint32_t g_parsed_umask = -1;

namespace {

void AppendXMLTag(const char* tag, const string& txt, string* r) {
  strings::SubstituteAndAppend(r, "<$0>$1</$0>", tag, EscapeForHtmlToString(txt));
}

static string DescribeOneFlagInXML(const CommandLineFlagInfo& flag) {
  unordered_set<string> tags;
  GetFlagTags(flag.name, &tags);

  string r("<flag>");
  AppendXMLTag("file", flag.filename, &r);
  AppendXMLTag("name", flag.name, &r);
  AppendXMLTag("meaning", flag.description, &r);
  AppendXMLTag("default", flag.default_value, &r);
  AppendXMLTag("current", flag.current_value, &r);
  AppendXMLTag("type", flag.type, &r);
  AppendXMLTag("tags", JoinStrings(tags, ","), &r);
  r += "</flag>";
  return r;
}

void DumpFlagsXML() {
  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);

  cout << "<?xml version=\"1.0\"?>" << endl;
  cout << "<AllFlags>" << endl;
  cout << strings::Substitute(
      "<program>$0</program>",
      EscapeForHtmlToString(BaseName(google::ProgramInvocationShortName()))) << endl;
  cout << strings::Substitute(
      "<usage>$0</usage>",
      EscapeForHtmlToString(google::ProgramUsage())) << endl;

  for (const CommandLineFlagInfo& flag : flags) {
    cout << DescribeOneFlagInXML(flag) << std::endl;
  }

  cout << "</AllFlags>" << endl;
  exit(1);
}

void ShowVersionAndExit() {
  cout << VersionInfo::GetAllVersionInfo() << endl;
  exit(0);
}

// Check that, if any flags tagged with 'tag' have been specified to
// non-default values, that 'unlocked' is true. If so (i.e. if the
// flags have been appropriately unlocked), emits a warning message
// for each flag and returns false. Otherwise, emits an error message
// and returns true.
bool CheckFlagsAndWarn(const string& tag, bool unlocked) {
  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);

  int use_count = 0;
  for (const auto& f : flags) {
    if (f.is_default) continue;
    unordered_set<string> tags;
    GetFlagTags(f.name, &tags);
    if (!ContainsKey(tags, tag)) continue;

    if (unlocked) {
      LOG(WARNING) << "Enabled " << tag << " flag: --" << f.name << "=" << f.current_value;
    } else {
      LOG(ERROR) << "Flag --" << f.name << " is " << tag << " and unsupported.";
      use_count++;
    }
  }

  if (!unlocked && use_count > 0) {
    LOG(ERROR) << use_count << " " << tag << " flag(s) in use.";
    LOG(ERROR) << "Use --unlock_" << tag << "_flags to proceed at your own risk.";
    return true;
  }
  return false;
}

// Check that any flags specified on the command line are allowed
// to be set. This ensures that, if the user is using any unsafe
// or experimental flags, they have explicitly unlocked them.
void CheckFlagsAllowed() {
  bool should_exit = false;
  should_exit |= CheckFlagsAndWarn("unsafe", FLAGS_unlock_unsafe_flags);
  should_exit |= CheckFlagsAndWarn("experimental", FLAGS_unlock_experimental_flags);
  if (should_exit) {
    exit(1);
  }
}

// Redact the flag tagged as 'sensitive', if --redact is set
// with 'flag'. Otherwise, return its value as-is. If EscapeMode
// is set to HTML, return HTML escaped string.
string CheckFlagAndRedact(const CommandLineFlagInfo& flag, EscapeMode mode) {
  string ret_value;
  unordered_set<string> tags;
  GetFlagTags(flag.name, &tags);

  if (ContainsKey(tags, "sensitive") && g_should_redact_flag) {
    ret_value = kRedactionMessage;
  } else {
    ret_value = flag.current_value;
  }
  if (mode == EscapeMode::HTML) {
    ret_value = EscapeForHtmlToString(ret_value);
  }
  return ret_value;
}

void SetUmask() {
  // We already validated with a nice error message using the ValidateUmask
  // FlagValidator above.
  CHECK(safe_strtou32_base(FLAGS_umask.c_str(), &g_parsed_umask, 8));
  uint32_t old_mask = umask(g_parsed_umask);
  if (old_mask != g_parsed_umask) {
    VLOG(2) << "Changed umask from " << StringPrintf("%03o", old_mask) << " to "
            << StringPrintf("%03o", g_parsed_umask);
  }
}

} // anonymous namespace

int ParseCommandLineFlags(int* argc, char*** argv, bool remove_flags) {
  int ret = google::ParseCommandLineNonHelpFlags(argc, argv, remove_flags);
  HandleCommonFlags();
  return ret;
}

void HandleCommonFlags() {
  CheckFlagsAllowed();

  if (FLAGS_helpxml) {
    DumpFlagsXML();
  } else if (FLAGS_dump_metrics_json) {
    MetricPrototypeRegistry::get()->WriteAsJsonAndExit();
  } else if (FLAGS_version) {
    ShowVersionAndExit();
  } else {
    google::HandleCommandLineHelpFlags();
  }

  if (FLAGS_heap_profile_path.empty()) {
    FLAGS_heap_profile_path = strings::Substitute(
        "/tmp/$0.$1", google::ProgramInvocationShortName(), getpid());
  }

  if (FLAGS_disable_core_dumps) {
    DisableCoreDumps();
  }

  SetUmask();

#ifdef TCMALLOC_ENABLED
  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_path.c_str());
  }
#endif
}

string CommandlineFlagsIntoString(EscapeMode mode) {
  string ret_value;
  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);

  for (const auto& f : flags) {
    ret_value += "--";
    if (mode == EscapeMode::HTML) {
      ret_value += EscapeForHtmlToString(f.name);
    } else if (mode == EscapeMode::NONE) {
      ret_value += f.name;
    }
    ret_value += "=";
    ret_value += CheckFlagAndRedact(f, mode);
    ret_value += "\n";
  }
  return ret_value;
}

string GetNonDefaultFlags(const GFlagsMap& default_flags) {
  stringstream args;
  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);
  for (const auto& flag : flags) {
    if (!flag.is_default) {
      // This only means that the flag has been rewritten. It doesn't
      // mean that this has been done in the command line, or even
      // that it's truly different from the default value.
      // Next, we try to check both.
      const auto& default_flag = default_flags.find(flag.name);
      // it's very unlikely, but still possible that we don't have the flag in defaults
      if (default_flag == default_flags.end() ||
          flag.current_value != default_flag->second.current_value) {
        if (!args.str().empty()) {
          args << '\n';
        }

        // Redact the flags tagged as sensitive, if --redact is set
        // with 'flag'.
        string flag_value = CheckFlagAndRedact(flag, EscapeMode::NONE);
        args << "--" << flag.name << '=' << flag_value;
      }
    }
  }
  return args.str();
}

GFlagsMap GetFlagsMap() {
  vector<CommandLineFlagInfo> default_flags;
  GetAllFlags(&default_flags);
  GFlagsMap flags_by_name;
  for (auto& flag : default_flags) {
    flags_by_name.emplace(flag.name, std::move(flag));
  }
  return flags_by_name;
}

} // namespace kudu
