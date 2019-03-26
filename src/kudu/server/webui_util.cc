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

#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/monitored_task.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/monotime.h"

using std::string;
using strings::Substitute;

namespace kudu {

void SchemaToJson(const Schema& schema, EasyJson* output) {
  EasyJson schema_json = output->Set("columns", EasyJson::kArray);
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    EasyJson col_json = schema_json.PushBack(EasyJson::kObject);
    col_json["name"] = col.name();
    col_json["is_key"] = schema.is_key_column(i);
    col_json["id"] = Substitute("$0", schema.column_id(i));
    col_json["type"] = col.TypeToString();
    const ColumnStorageAttributes& attrs = col.attributes();
    col_json["encoding"] = EncodingType_Name(attrs.encoding);
    col_json["compression"] = CompressionType_Name(attrs.compression);
    col_json["read_default"] = col.has_read_default() ?
                                   col.Stringify(col.read_default_value()) : "-";
    col_json["write_default"] = col.has_write_default() ?
                                    col.Stringify(col.write_default_value()) : "-";
    col_json["comment"] = col.comment() ? *col.comment() : "-";
  }
}

void TaskListToJson(const std::vector<scoped_refptr<MonitoredTask> >& tasks, EasyJson* output) {
  EasyJson tasks_json = output->Set("tasks", EasyJson::kArray);
  for (const scoped_refptr<MonitoredTask>& task : tasks) {
    EasyJson task_json = tasks_json.PushBack(EasyJson::kObject);
    task_json["name"] = task->type_name();
    switch (task->state()) {
      case MonitoredTask::kStatePreparing:
        task_json["state"] = "Preparing";
        break;
      case MonitoredTask::kStateRunning:
        task_json["state"] = "Running";
        break;
      case MonitoredTask::kStateComplete:
        task_json["state"] = "Complete";
        break;
      case MonitoredTask::kStateFailed:
        task_json["state"] = "Failed";
        break;
      case MonitoredTask::kStateAborted:
        task_json["state"] = "Aborted";
        break;
    }
    double running_secs = 0;
    if (task->completion_timestamp().Initialized()) {
      running_secs =
          (task->completion_timestamp() - task->start_timestamp()).ToSeconds();
    } else if (task->start_timestamp().Initialized()) {
      running_secs = (MonoTime::Now() - task->start_timestamp()).ToSeconds();
    }
    task_json["time"] = HumanReadableElapsedTime::ToShortString(running_secs);
    task_json["description"] = task->description();
  }
}
} // namespace kudu
