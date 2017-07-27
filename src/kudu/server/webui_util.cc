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

#include "kudu/server/webui_util.h"

#include <sstream>
#include <string>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/monitored_task.h"
#include "kudu/util/url-coding.h"

using strings::Substitute;

namespace kudu {

void HtmlOutputSchemaTable(const Schema& schema,
                           std::ostringstream* output) {
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr>"
          << "<th>Column</th><th>ID</th><th>Type</th>"
          << "<th>Encoding</th><th>Compression</th>"
          << "<th>Read default</th><th>Write default</th>"
          << "</tr></thead>\n";
  *output << "<tbody>";
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    const string& html_escaped_col_name = EscapeForHtmlToString(col.name());
    const string& col_name = schema.is_key_column(col.name()) ?
                                 Substitute("<u>$0</u>", html_escaped_col_name) :
                                 html_escaped_col_name;
    string read_default = "-";
    if (col.has_read_default()) {
      read_default = col.Stringify(col.read_default_value());
    }
    string write_default = "-";
    if (col.has_write_default()) {
      write_default = col.Stringify(col.write_default_value());
    }
    const ColumnStorageAttributes& attrs = col.attributes();
    const string& encoding = EncodingType_Name(attrs.encoding);
    const string& compression = CompressionType_Name(attrs.compression);
    *output << Substitute("<tr><th>$0</th><td>$1</td><td>$2</td><td>$3</td>"
                          "<td>$4</td><td>$5</td><td>$6</td></tr>\n",
                          col_name,
                          schema.column_id(i),
                          col.TypeToString(),
                          EscapeForHtmlToString(encoding),
                          EscapeForHtmlToString(compression),
                          EscapeForHtmlToString(read_default),
                          EscapeForHtmlToString(write_default));
  }
  *output << "</tbody></table>\n";
}

void HtmlOutputImpalaSchema(const std::string& table_name,
                            const Schema& schema,
                            const string& master_addresses,
                            std::ostringstream* output) {
  *output << "<pre><code>\n";

  // Escape table and column names with ` to avoid conflicts with Impala reserved words.
  *output << "CREATE EXTERNAL TABLE " << EscapeForHtmlToString("`" + table_name + "`")
          << " STORED AS KUDU\n";
  *output << "TBLPROPERTIES(\n";
  *output << "  'kudu.table_name' = '";
  *output << EscapeForHtmlToString(table_name) << "',\n";
  *output << "  'kudu.master_addresses' = '";
  *output << EscapeForHtmlToString(master_addresses) << "'";
  *output << ");\n";
  *output << "</code></pre>\n";
}

void HtmlOutputTaskList(const std::vector<scoped_refptr<MonitoredTask> >& tasks,
                        std::ostringstream* output) {
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Task Name</th><th>State</th><th>Time</th><th>Description</th></tr>\n";
  for (const scoped_refptr<MonitoredTask>& task : tasks) {
    string state;
    switch (task->state()) {
      case MonitoredTask::kStatePreparing:
        state = "Preparing";
        break;
      case MonitoredTask::kStateRunning:
        state = "Running";
        break;
      case MonitoredTask::kStateComplete:
        state = "Complete";
        break;
      case MonitoredTask::kStateFailed:
        state = "Failed";
        break;
      case MonitoredTask::kStateAborted:
        state = "Aborted";
        break;
    }

    double running_secs = 0;
    if (task->completion_timestamp().Initialized()) {
      running_secs =
          (task->completion_timestamp() - task->start_timestamp()).ToSeconds();
    } else if (task->start_timestamp().Initialized()) {
      running_secs = (MonoTime::Now() - task->start_timestamp()).ToSeconds();
    }

    *output << Substitute(
        "<tr><th>$0</th><td>$1</td><td>$2</td><td>$3</td></tr>\n",
        EscapeForHtmlToString(task->type_name()),
        EscapeForHtmlToString(state),
        EscapeForHtmlToString(HumanReadableElapsedTime::ToShortString(running_secs)),
        EscapeForHtmlToString(task->description()));
  }
  *output << "</table>\n";
}
} // namespace kudu
