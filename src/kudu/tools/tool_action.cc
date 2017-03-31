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

#include <algorithm>
#include <iomanip>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/url-coding.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

string FakeDescribeOneFlag(const ActionArgsDescriptor::Arg& arg) {
  string res = google::DescribeOneFlag({
    arg.name,        // name
    "string",        // type
    arg.description, // description
    "",              // current_value
    "",              // default_value
    "",              // filename
    false,           // has_validator_fn
    true,            // is_default
    nullptr          // flag_ptr
  });

  // Strip the first dash from the description; this is a positional parameter
  // so let's make sure it looks like one.
  string::size_type first_dash_idx = res.find("-");
  DCHECK_NE(string::npos, first_dash_idx);
  return res.substr(0, first_dash_idx) + res.substr(first_dash_idx + 1);
}

string BuildUsageString(const vector<Mode*>& chain) {
  return JoinMapped(chain, [](Mode* a){ return a->name(); }, " ");
}


// Append 'to_append' to 'dst', but hard-wrapped at 78 columns.
// After any newline, 'continuation_indent' spaces are prepended.
void AppendHardWrapped(StringPiece to_append,
                       int continuation_indent,
                       string* dst) {
  const int kWrapColumns = 78;
  DCHECK_LT(continuation_indent, kWrapColumns);

  // The string we're appending to might not be already at a newline.
  int last_line_length = 0;
  auto newline_pos = dst->rfind('\n');
  if (newline_pos != string::npos) {
    last_line_length = dst->size() - newline_pos;
  }

  // Iterate through the words deciding where to wrap.
  vector<StringPiece> words = strings::Split(to_append, " ");
  if (words.empty()) return;

  for (const auto& word : words) {
    // If the next word won't fit on this line, break before we append it.
    if (last_line_length + word.size() > kWrapColumns) {
      dst->push_back('\n');
      for (int i = 0; i < continuation_indent; i++) {
        dst->push_back(' ');
      }
      last_line_length = continuation_indent;
    }
    word.AppendToString(dst);
    dst->push_back(' ');
    last_line_length += word.size() + 1;
  }

  // Remove the extra space that we added at the end.
  dst->resize(dst->size() - 1);
}

string SpacePad(StringPiece s, int len) {
  if (s.size() >= len) {
    return s.ToString();
  }
  return string(len - s.size(), ' ' ) + s.ToString();
}

} // anonymous namespace

ModeBuilder::ModeBuilder(const string& name)
    : name_(name) {
}

ModeBuilder& ModeBuilder::Description(const string& description) {
  CHECK(description_.empty());
  description_ = description;
  return *this;
}

ModeBuilder& ModeBuilder::AddMode(unique_ptr<Mode> mode) {
  submodes_.push_back(std::move(mode));
  return *this;
}

ModeBuilder& ModeBuilder::AddAction(unique_ptr<Action> action) {
  actions_.push_back(std::move(action));
  return *this;
}

unique_ptr<Mode> ModeBuilder::Build() {
  CHECK(!description_.empty());
  unique_ptr<Mode> mode(new Mode());
  mode->name_ = name_;
  mode->description_ = description_;
  mode->submodes_ = std::move(submodes_);
  mode->actions_ = std::move(actions_);
  return mode;
}

// Get help for this mode, passing in its parent mode chain.
string Mode::BuildHelp(const vector<Mode*>& chain) const {
  string msg;
  msg += Substitute("Usage: $0 <command> [<args>]\n\n",
                    BuildUsageString(chain));
  msg += "<command> can be one of the following:\n";

  vector<pair<string, string>> line_pairs;
  int max_command_len = 0;
  for (const auto& m : modes()) {
    line_pairs.emplace_back(m->name(), m->description());
    max_command_len = std::max<int>(max_command_len, m->name().size());
  }
  for (const auto& a : actions()) {
    line_pairs.emplace_back(a->name(), a->description());
    max_command_len = std::max<int>(max_command_len, a->name().size());
  }

  for (const auto& lp : line_pairs) {
    msg += "  " + SpacePad(lp.first, max_command_len);
    msg += "   ";
    AppendHardWrapped(lp.second, max_command_len + 5, &msg);
    msg += "\n";
  }

  msg += "\n";
  return msg;
}

string Mode::BuildHelpXML(const vector<Mode*>& chain) const {
  string xml;
  xml += "<mode>";
  xml += Substitute("<name>$0</name>", name());
  xml += Substitute("<description>$0</description>",
                    EscapeForHtmlToString(description()));
  for (const auto& a : actions()) {
    xml += a->BuildHelpXML(chain);
  }

  for (const auto& m : modes()) {
    vector<Mode*> m_chain(chain);
    m_chain.push_back(m.get());
    xml += m->BuildHelpXML(m_chain);
  }
  xml += "</mode>";
  return xml;
}

ActionBuilder::ActionBuilder(const string& name, const ActionRunner& runner)
    : name_(name),
      runner_(runner) {
}

ActionBuilder& ActionBuilder::Description(const string& description) {
  CHECK(description_.empty());
  description_ = description;
  return *this;
}

ActionBuilder& ActionBuilder::ExtraDescription(const string& extra_description) {
  CHECK(!extra_description_.is_initialized());
  extra_description_ = extra_description;
  return *this;
}

ActionBuilder& ActionBuilder::AddRequiredParameter(
    const ActionArgsDescriptor::Arg& arg) {
  args_.required.push_back(arg);
  return *this;
}

ActionBuilder& ActionBuilder::AddRequiredVariadicParameter(
    const ActionArgsDescriptor::Arg& arg) {
  DCHECK(!args_.variadic);
  args_.variadic = arg;
  return *this;
}

ActionBuilder& ActionBuilder::AddOptionalParameter(const string& param) {
#ifndef NDEBUG
  // Make sure this gflag exists.
  string option;
  DCHECK(google::GetCommandLineOption(param.c_str(), &option));
#endif
  args_.optional.push_back(param);
  return *this;
}

unique_ptr<Action> ActionBuilder::Build() {
  CHECK(!description_.empty());
  unique_ptr<Action> action(new Action());
  action->name_ = name_;
  action->description_ = description_;
  action->extra_description_ = extra_description_;
  action->runner_ = runner_;
  action->args_ = args_;
  return action;
}

Status Action::Run(const vector<Mode*>& chain,
                   const unordered_map<string, string>& required_args,
                   const vector<string>& variadic_args) const {
  return runner_({ chain, this, required_args, variadic_args });
}

string Action::BuildHelp(const vector<Mode*>& chain) const {
  string usage_msg = Substitute("Usage: $0 $1", BuildUsageString(chain), name());
  string desc_msg;
  for (const auto& param : args_.required) {
    usage_msg += Substitute(" <$0>", param.name);
    desc_msg += FakeDescribeOneFlag(param);
    desc_msg += "\n";
  }
  if (args_.variadic) {
    const ActionArgsDescriptor::Arg& param = args_.variadic.get();
    usage_msg += Substitute(" <$0>...", param.name);
    desc_msg += FakeDescribeOneFlag(param);
    desc_msg += "\n";
  }
  for (const auto& param : args_.optional) {
    google::CommandLineFlagInfo gflag_info =
        google::GetCommandLineFlagInfoOrDie(param.c_str());

    if (gflag_info.type == "bool") {
      if (gflag_info.default_value == "false") {
        usage_msg += Substitute(" [-$0]", param);
      } else {
        usage_msg += Substitute(" [-no$0]", param);
      }
    } else {
      string noun;
      string::size_type last_underscore_idx = param.rfind('_');
      if (last_underscore_idx != string::npos &&
          last_underscore_idx != param.size() - 1) {
        noun = param.substr(last_underscore_idx + 1);
      } else {
        noun = param;
      }
      usage_msg += Substitute(" [-$0=<$1>]", param, noun);
    }
    desc_msg += google::DescribeOneFlag(gflag_info);
    desc_msg += "\n";
  }
  string msg;
  AppendHardWrapped(usage_msg, 8, &msg);
  msg += "\n\n";
  AppendHardWrapped(description_, 0, &msg);
  if (extra_description_) {
    msg += "\n\n";
    AppendHardWrapped(extra_description_.get(), 0, &msg);
  }
  msg += "\n\n";
  msg += desc_msg;
  return msg;
}

string Action::BuildHelpXML(const vector<Mode*>& chain) const {
  string usage = Substitute("$0 $1", BuildUsageString(chain), name());
  string xml;
  xml += "<action>";
  xml += Substitute("<name>$0</name>", name());
  xml += Substitute("<description>$0</description>",
                    EscapeForHtmlToString(description()));
  xml += Substitute("<extra_description>$0</extra_description>",
                    EscapeForHtmlToString(extra_description()
                                              .get_value_or("")));
  for (const auto& r : args().required) {
    usage += Substitute(" &lt;$0&gt;", r.name);
    xml += "<argument>";
    xml += "<kind>required</kind>";
    xml += Substitute("<name>$0</name>", r.name);
    xml += Substitute("<description>$0</description>",
                      EscapeForHtmlToString(r.description));
    xml += "<type>string</type>";
    xml += "</argument>";
  }

  if (args().variadic) {
    const ActionArgsDescriptor::Arg& v = args().variadic.get();
    usage += Substitute(" &lt;$0&gt;...", v.name);
    xml += "<argument>";
    xml += "<kind>variadic</kind>";
    xml += Substitute("<name>$0</name>", v.name);
    xml += Substitute("<description>$0</description>",
                      EscapeForHtmlToString(v.description));
    xml += "<type>string</type>";
    xml += "</argument>";
  }

  for (const auto& o : args().optional) {
    google::CommandLineFlagInfo gflag_info =
        google::GetCommandLineFlagInfoOrDie(o.c_str());

    if (gflag_info.type == "bool") {
      if (gflag_info.default_value == "false") {
        usage += Substitute(" [-$0]", o);
      } else {
        usage += Substitute(" [-no$0]", o);
      }
    } else {
      string noun;
      string::size_type last_underscore_idx = o.rfind('_');
      if (last_underscore_idx != string::npos &&
          last_underscore_idx != o.size() - 1) {
        noun = o.substr(last_underscore_idx + 1);
      } else {
        noun = o;
      }
      usage += Substitute(" [-$0=&lt;$1&gt;]", o, noun);
    }

    xml += "<argument>";
    xml += "<kind>optional</kind>";
    xml += Substitute("<name>$0</name>", gflag_info.name);
    xml += Substitute("<description>$0</description>", gflag_info.description);
    xml += Substitute("<type>$0</type>", gflag_info.type);
    xml += Substitute("<default_value>$0</default_value>",
                      gflag_info.default_value);
    xml += "</argument>";
  }
  xml += Substitute("<usage>$0</usage>", EscapeForHtmlToString(usage));
  xml += "</action>";
  return xml;
}

} // namespace tools
} // namespace kudu
