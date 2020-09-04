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

} // namespace tools
} // namespace kudu
