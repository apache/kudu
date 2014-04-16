// Copyright 2014 Cloudera Inc.

#include "server/webui_util.h"

#include <string>

#include "common/schema.h"
#include "gutil/map-util.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/substitute.h"
#include "server/monitored_task.h"
#include "util/url-coding.h"

using strings::Substitute;

namespace kudu {

void HtmlOutputSchemaTable(const Schema& schema,
                           std::stringstream* output) {
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr>"
          << "<th>Column</th><th>ID</th><th>Type</th>"
          << "<th>Read default</th><th>Write default</th>"
          << "</tr>\n";

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    string read_default = "-";
    if (col.has_read_default()) {
      read_default = col.Stringify(col.read_default_value());
    }
    string write_default = "-";
    if (col.has_write_default()) {
      write_default = col.Stringify(col.write_default_value());
    }
    *output << Substitute("<tr><th>$0</th><td>$1</td><td>$2</td><td>$3</td><td>$4</td></tr>\n",
                          EscapeForHtmlToString(col.name()),
                          schema.column_id(i),
                          col.TypeToString(),
                          EscapeForHtmlToString(read_default),
                          EscapeForHtmlToString(write_default));
  }
  *output << "</table>\n";
}

void HtmlOutputImpalaSchema(const std::string& table_name,
                            const Schema& schema,
                            std::stringstream* output) {
  *output << "<code><pre>\n";

  *output << "CREATE TABLE " << EscapeForHtmlToString(table_name) << " (\n";

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);

    *output << EscapeForHtmlToString(col.name()) << " ";
    switch (col.type_info()->type()) {
      case STRING:
        *output << "STRING";
        break;
      case UINT8:
      case INT8:
        *output << "TINYINT";
        break;
      case UINT16:
      case INT16:
        *output << "SMALLINT";
        break;
      case UINT32:
      case INT32:
        *output << "INT";
        break;
      case UINT64:
      case INT64:
        *output << "BIGINT";
        break;
      default:
        *output << "[unsupported type " << col.type_info()->name() << "!]";
        break;
    }
    if (i < schema.num_columns() - 1) {
      *output << ",";
    }
    *output << "\n";
  }
  *output << ")\n";

  *output << "TBLPROPERTIES(\n";
  *output << "  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',\n";
  *output << "   'kudu.table.name' = '" << table_name << "'\n";
  *output << ");\n";
  *output << "</pre></code>\n";
}

string HumanElapsedTime(int64_t msec) {
  if (msec >= 60000000) {
    StringPrintf("%.2fmin", msec / 60000000.0);
  } else if (msec >= 1000000) {
    StringPrintf("%.2fsec", msec / 1000000.0);
  }
  return StringPrintf("%.2fmsec", msec / 1000.0);
}


void HtmlOutputTaskList(const std::vector<scoped_refptr<MonitoredTask> >& tasks,
                        std::stringstream* output) {
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Task Name</th><th>State</th><th>Time</th><th>Description</th></tr>\n";
  BOOST_FOREACH(const scoped_refptr<MonitoredTask>& task, tasks) {
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

    int64_t runningTime = 0;
    if (task->completion_timestamp().Initialized()) {
      runningTime = task->completion_timestamp().GetDeltaSince(
        task->start_timestamp()).ToMilliseconds();
    } else if (task->start_timestamp().Initialized()) {
      runningTime = MonoTime::Now(MonoTime::FINE).GetDeltaSince(
        task->start_timestamp()).ToMilliseconds();
    }

    *output << Substitute(
        "<tr><th>$0</th><td>$1</td><td>$2</td><td>$3</td></tr>\n",
        EscapeForHtmlToString(task->type_name()),
        EscapeForHtmlToString(state),
        EscapeForHtmlToString(HumanElapsedTime(runningTime)),
        EscapeForHtmlToString(task->description()));
  }
  *output << "</table>\n";
}

} // namespace kudu
