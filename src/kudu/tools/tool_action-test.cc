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

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/tools/tool_action_common.h"
#include "kudu/util/net/net_util.h"

// gflags for optional action parameters
DEFINE_bool(opt_bool, false, "obd");
DEFINE_string(opt_string, "", "osd");

namespace kudu {
namespace tools {

using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

TEST(ToolActionTest, TestActionBuildHelpXML) {
  unique_ptr<Action> action =
      ActionBuilder("sample", nullptr)
          .Description("d")
          .ExtraDescription("ed")
          .AddRequiredParameter({ "required", "rpd" })
          .AddRequiredVariadicParameter({ "variadic", "vpd" })
          .AddOptionalParameter("opt_string")
          .AddOptionalParameter("opt_bool")
          .Build();

  string xml = action->BuildHelpXML(vector<Mode*>());
  SCOPED_TRACE(xml);

  stringstream ss;
  ss << "<action>";
  ss << "<name>sample</name>";
  ss << "<description>d</description>";
  ss << "<extra_description>ed</extra_description>";
  ss << "<argument><kind>required</kind><name>required</name>"
        "<description>rpd</description><type>string</type></argument>";
  ss << "<argument><kind>variadic</kind><name>variadic</name>"
        "<description>vpd</description><type>string</type></argument>";
  ss << "<argument><kind>optional</kind><name>opt_string</name>"
        "<description>osd</description><type>string</type>"
        "<default_value></default_value></argument>";
  ss << "<argument><kind>optional</kind><name>opt_bool</name>"
        "<description>obd</description><type>bool</type>"
        "<default_value>false</default_value></argument>";
  ss << "<usage> sample &amp;lt;required&amp;gt; &amp;lt;variadic&amp;gt;... "
        "[-opt_string=&amp;lt;string&amp;gt;] [-opt_bool]</usage>";
  ss << "</action>";
  string expected_xml = ss.str();

  ASSERT_EQ(expected_xml, xml);
}

TEST(ToolActionTest, TestModeBuildHelpXML) {
  unique_ptr<Action> action =
      ActionBuilder("action", nullptr)
          .Description("ad")
          .AddRequiredParameter({ "required", "rpd" })
          .Build();

  unique_ptr<Mode> submode = ModeBuilder("submode")
      .Description("subd")
      .AddAction(std::move(action))
      .Build();

  unique_ptr<Mode> mode = ModeBuilder("mode")
      .Description("md")
      .AddMode(std::move(submode))
      .Build();

  vector<Mode*> chain = { mode.get() };

  string xml = mode->BuildHelpXML(chain);
  SCOPED_TRACE(xml);

  stringstream ss;
  ss << "<mode><name>mode</name>";
  ss << "<description>md</description>";
  ss << "<mode><name>submode</name>";
  ss << "<description>subd</description>";
  ss << "<action><name>action</name>";
  ss << "<description>ad</description><extra_description></extra_description>";
  ss << "<argument><kind>required</kind><name>required</name>"
        "<description>rpd</description><type>string</type></argument>";
  ss << "<usage>mode submode action &amp;lt;required&amp;gt;</usage>";
  ss << "</action>";
  ss << "</mode>";
  ss << "</mode>";
  string expected_xml = ss.str();

  ASSERT_EQ(expected_xml, xml);
}

TEST(ToolActionTest, TestMasterAddressesToSet) {
  // A standard master address string ordered and including the default port.
  string master_addrs_std = "host-1:7051,host-2:7051,host-3:7051";
  UnorderedHostPortSet std_set;
  MasterAddressesToSet(master_addrs_std, &std_set);
  ASSERT_EQ(3, std_set.size());

  // A messy master address string that is unordered, has no port, and includes duplicates.
  string master_addrs_messy = "host-3,host-1,host-2,host-1:7051,host-2";
  UnorderedHostPortSet messy_set;
  MasterAddressesToSet(master_addrs_messy, &messy_set);
  ASSERT_EQ(3, messy_set.size());

  ASSERT_EQ(std_set, messy_set);

  // A master address string that matches all but one port.
  string master_addrs_bad_port = "host-1:7051,host-2:7050,host-3:7051";
  UnorderedHostPortSet bad_port_set;
  MasterAddressesToSet(master_addrs_bad_port, &bad_port_set);
  ASSERT_EQ(3, bad_port_set.size());

  ASSERT_NE(std_set, bad_port_set);

  // A master address string that matches all but one host.
  string master_addrs_bad_host = "host-1:7051,host-21:7051,host-3:7051";
  UnorderedHostPortSet bad_host_set;
  MasterAddressesToSet(master_addrs_bad_host, &bad_host_set);
  ASSERT_EQ(3, bad_host_set.size());

  ASSERT_NE(std_set, bad_host_set);
}

TEST(ToolActionTest, TestAppendHardWrapped) {
  // Case 1: string is empty.
  const string& msg1 = "";
  string formated_msg1;
  AppendHardWrapped(msg1, 0, &formated_msg1);
  ASSERT_EQ("", formated_msg1);

  // Case 2: string is a blank space.
  const string& msg3 = " ";
  string formated_msg3;
  AppendHardWrapped(msg3, 0, &formated_msg3);
  ASSERT_EQ(" ", formated_msg3);

  // Case 3: string length is less than 78.
  const string& msg2 = "Attempts to create on-disk metadata";
  string formated_msg2;
  AppendHardWrapped(msg2, 0, &formated_msg2);
  ASSERT_EQ("Attempts to create on-disk metadata", formated_msg2);

  // Case 4: string length is more than 78 a little.
  const string& msg4 = "Attempts to create on-disk metadata "
                       "that can be used by a non-replicated master";
  string formated_msg4;
  AppendHardWrapped(msg4, 0, &formated_msg4);
  const string& expected_output4 = "Attempts to create on-disk metadata "
                                   "that can be used by a non-replicated \nmaster";
  // Contains one '\n'.
  ASSERT_EQ(expected_output4, formated_msg4);

  // Case 5: string length is less than 78 with one '\n' in the end.
  const string& msg5 = "Attempts to create on-disk metadata\n";
  string formated_msg5;
  AppendHardWrapped(msg5, 0, &formated_msg5);
  // Contains one '\n'
  ASSERT_EQ("Attempts to create on-disk metadata\n", formated_msg5);

  // Case 6: string length is more than 78 with one '\n' in the middle.
  const string& msg6 = "Attempts to create on-disk metadata \n"
                       "that can be used by a non-replicated master";
  string formated_msg6;
  AppendHardWrapped(msg6, 0, &formated_msg6);
  const string& expected_output6 = "Attempts to create on-disk metadata \n"
                                   "that can be used by a non-replicated master";
  // Contains one '\n'
  ASSERT_EQ(expected_output6, formated_msg6);

  // Case 7: string length is more than 78 with one '\n' in the end.
  const string& msg7 = "Attempts to create on-disk metadata "
                       "that can be used by a non-replicated master\n";
  string formated_msg7;
  AppendHardWrapped(msg7, 0, &formated_msg7);
  const string& expected_output7 = "Attempts to create on-disk metadata "
                                   "that can be used by a non-replicated \nmaster\n";
  // Contains two '\n'
  ASSERT_EQ(expected_output7, formated_msg7);

  // Case 8: 2 '\n' in the end.
  const string& msg8 = "Attempts to create on-disk metadata "
                       "that can be used by a non-replicated master\n\n";
  string formated_msg8;
  AppendHardWrapped(msg8, 0, &formated_msg8);
  const string& expected_output8 = "Attempts to create on-disk metadata "
                                   "that can be used by a non-replicated \nmaster\n\n";
  // contains 3 '\n'.
  ASSERT_EQ(expected_output8, formated_msg8);

  // Case 9: string is '\n'.
  const string& msg9 = "\n";
  string formated_msg9;
  AppendHardWrapped(msg9, 0, &formated_msg9);
  ASSERT_EQ("\n", formated_msg9);

  // Case 10: string length is more than 78,
  // dst is not null and does not contain '\n'.
  // The old string in dst will be a new line.
  const string& msg10 = "New line to create on-disk metadata";
  string formated_msg10 = "Old line to create on-disk metadata. "
                          "that can be used by a non-replicated master";
  AppendHardWrapped(msg10, 0, &formated_msg10);
  const string& expected_output10 = "Old line to create on-disk metadata. "
                                  "that can be used by a non-replicated master\n"
                                  "New line to create on-disk metadata";
  ASSERT_EQ(expected_output10, formated_msg10);

  // Case 11: dst and to_append are all empty.
  const string& msg11 = "";
  string formated_msg11 = "";
  AppendHardWrapped(msg11, 0, &formated_msg11);
  ASSERT_EQ("", formated_msg11);

  // Case 12: string length is more than 78,
  // continue indent is 2.
  const string& msg12 = "Old line to create on-disk metadata. "
                        "that can be used by a non-replicated master";
  string formated_msg12;
  AppendHardWrapped(msg12, 2, &formated_msg12);
  const string& expected_output12 = "Old line to create on-disk metadata. "
                "that can be used by a non-replicated \n  master";
  ASSERT_EQ(expected_output12, formated_msg12);
}
} // namespace tools
} // namespace kudu
