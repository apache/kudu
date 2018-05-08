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


#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h> // IWYU pragma: keep

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#ifdef TCMALLOC_ENABLED
#include <gperftools/heap-profiler.h>
#endif

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
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
using std::ostringstream;
using std::unordered_set;
using std::vector;

using strings::Substitute;

// Because every binary initializes its flags here, we use it as a convenient place
// to offer some global flags as well.
DEFINE_bool(dump_metrics_json, false,
            "Dump a JSON document describing all of the metrics which may be emitted "
            "by this binary.");
TAG_FLAG(dump_metrics_json, hidden);

#ifdef TCMALLOC_ENABLED
DEFINE_bool(enable_process_lifetime_heap_profiling, false, "Enables heap "
    "profiling for the lifetime of the process. Profile output will be stored in the "
    "directory specified by -heap_profile_path.");
TAG_FLAG(enable_process_lifetime_heap_profiling, stable);
TAG_FLAG(enable_process_lifetime_heap_profiling, advanced);

DEFINE_string(heap_profile_path, "", "Output path to store heap profiles. If not set " \
    "profiles are stored in /tmp/<process-name>.<pid>.<n>.heap.");
TAG_FLAG(heap_profile_path, stable);
TAG_FLAG(heap_profile_path, advanced);

DEFINE_int64(heap_sample_every_n_bytes, 0,
             "Enable heap occupancy sampling. If this flag is set to some positive "
             "value N, a memory allocation will be sampled approximately every N bytes. "
             "Lower values of N incur larger overhead but give more accurate results. "
             "A value such as 524288 (512KB) is a reasonable choice with relatively "
             "low overhead.");
TAG_FLAG(heap_sample_every_n_bytes, advanced);
TAG_FLAG(heap_sample_every_n_bytes, experimental);
#endif

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
              "Comma-separated list that controls redaction context. Supported options "
              "are 'all','log', and 'none'. If 'all' is specified, sensitive data "
              "(sensitive configuration flags and row data) will be redacted from "
              "the web UI as well as glog and error messages. If 'log' is specified, "
              "sensitive data will only be redacted from glog and error messages. "
              "If 'none' is specified, no redaction will occur.");
TAG_FLAG(redact, advanced);
TAG_FLAG(redact, evolving);

static bool ValidateRedact(const char* /*flagname*/, const string& value) {
  kudu::g_should_redact = kudu::RedactContext::NONE;

  // Flag value is case insensitive.
  string redact_flags;
  kudu::ToUpperCase(value, &redact_flags);

  // 'all', 'none', and '' must be specified without any other option.
  if (redact_flags == "ALL") {
    kudu::g_should_redact = kudu::RedactContext::ALL;
    return true;
  }
  if (redact_flags == "NONE" || redact_flags.empty()) {
    return true;
  }

  for (const auto& t : strings::Split(redact_flags, ",", strings::SkipEmpty())) {
    if (t == "LOG") {
      kudu::g_should_redact = kudu::RedactContext::LOG;
    } else if (t == "ALL" || t == "NONE") {
      LOG(ERROR) << "Invalid redaction options: "
                 << value << ", '" << t << "' must be specified by itself.";
      return false;
    } else {
      LOG(ERROR) << "Invalid redaction context: " << t <<
                    ". Available types are 'all', 'log', and 'none'.";
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

//------------------------------------------------------------
// TCMalloc flags.
// These are tricky because tcmalloc doesn't use gflags. So we have to
// reach into its internal namespace.
//------------------------------------------------------------
#define TCM_NAMESPACE FLAG__namespace_do_not_use_directly_use_DECLARE_int64_instead
namespace TCM_NAMESPACE {
extern int64_t FLAGS_tcmalloc_sample_parameter;
} // namespace TCM_NAMESPACE

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
    cout << DescribeOneFlagInXML(flag) << endl;
  }

  cout << "</AllFlags>" << endl;
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

// Run 'late phase' custom validators: these can be run only when all flags are
// already parsed and individually validated.
void RunCustomValidators() {
  const auto& validators(GetFlagValidators());
  bool found_inconsistency = false;
  for (const auto& e : validators) {
    found_inconsistency |= !e.second();
  }
  if (found_inconsistency) {
    LOG(ERROR) << "Detected inconsistency in command-line flags; exiting";
    exit(1);
  }
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

// If --redact indicates, redact the flag tagged as 'sensitive'.
// Otherwise, return its value as-is. If EscapeMode is set to HTML,
// return HTML escaped string.
string CheckFlagAndRedact(const CommandLineFlagInfo& flag, EscapeMode mode) {
  string ret_value;
  unordered_set<string> tags;
  GetFlagTags(flag.name, &tags);

  if (ContainsKey(tags, "sensitive") && KUDU_SHOULD_REDACT()) {
    ret_value = kRedactionMessage;
  } else {
    ret_value = flag.current_value;
  }
  if (mode == EscapeMode::HTML) {
    ret_value = EscapeForHtmlToString(ret_value);
  }
  return ret_value;
}

int ParseCommandLineFlags(int* argc, char*** argv, bool remove_flags) {
  // The logbufsecs default is 30 seconds which is a bit too long.
  google::SetCommandLineOptionWithMode("logbufsecs", "5",
                                       google::FlagSettingMode::SET_FLAGS_DEFAULT);

  int ret = google::ParseCommandLineNonHelpFlags(argc, argv, remove_flags);
  HandleCommonFlags();
  return ret;
}

void HandleCommonFlags() {
  if (FLAGS_helpxml) {
    DumpFlagsXML();
    exit(1);
  } else if (FLAGS_dump_metrics_json) {
    MetricPrototypeRegistry::get()->WriteAsJson();
    exit(0);
  } else if (FLAGS_version) {
    cout << VersionInfo::GetAllVersionInfo() << endl;
    exit(0);
  }

  google::HandleCommandLineHelpFlags();
  CheckFlagsAllowed();
  RunCustomValidators();

  if (FLAGS_disable_core_dumps) {
    DisableCoreDumps();
  }

  SetUmask();

#ifdef TCMALLOC_ENABLED
  if (FLAGS_heap_profile_path.empty()) {
    FLAGS_heap_profile_path = strings::Substitute(
        "/tmp/$0.$1", google::ProgramInvocationShortName(), getpid());
  }

  if (FLAGS_enable_process_lifetime_heap_profiling) {
    HeapProfilerStart(FLAGS_heap_profile_path.c_str());
  }
  // Set the internal tcmalloc flag unless it was already set using the built-in
  // environment-variable-based method. It doesn't appear that this is settable
  // in any less hacky fashion.
  if (!getenv("TCMALLOC_SAMPLE_PARAMETER")) {
    TCM_NAMESPACE::FLAGS_tcmalloc_sample_parameter = FLAGS_heap_sample_every_n_bytes;
  } else if (!google::GetCommandLineFlagInfoOrDie("heap_sample_every_n_bytes").is_default) {
    LOG(ERROR) << "Heap sampling configured using both --heap-sample-every-n-bytes and "
               << "TCMALLOC_SAMPLE_PARAMETER. Ignoring command line flag.";
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
  ostringstream args;
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

        // Redact the flags tagged as sensitive, if redaction is enabled.
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

Status ParseTriState(const char* flag_name, const std::string& flag_value,
    TriStateFlag* tri_state) {
  if (boost::iequals(flag_value, "required")) {
    *tri_state = TriStateFlag::REQUIRED;
  } else if (boost::iequals(flag_value, "optional")) {
    *tri_state = TriStateFlag::OPTIONAL;
  } else if (boost::iequals(flag_value, "disabled")) {
    *tri_state = TriStateFlag::DISABLED;
  } else {
    return Status::InvalidArgument(strings::Substitute(
          "$0 flag must be one of 'required', 'optional', or 'disabled'",
          flag_name));
  }
  return Status::OK();
}

} // namespace kudu
