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
#ifndef KUDU_SERVER_WEBUI_UTIL_H
#define KUDU_SERVER_WEBUI_UTIL_H

#include <iosfwd>
#include <vector>

template <class T>
class scoped_refptr;

namespace kudu {

class EasyJson;
class Schema;
class MonitoredTask;

// Appends a JSON array describing 'schema' to 'output', under the key "columns".
void SchemaToJson(const Schema& schema, EasyJson* output);

// Appends an HTML table describing 'schema' to 'output'.
// TODO(wdberkeley) Remove this once /tablet is converted to a template.
void HtmlOutputSchemaTable(const Schema& schema, std::ostringstream* output);

// Appends a JSON array describing the tasks in 'tasks' to 'output', under the key "tasks".
void TaskListToJson(const std::vector<scoped_refptr<MonitoredTask>>& tasks, EasyJson* output);

} // namespace kudu

#endif // KUDU_SERVER_WEBUI_UTIL_H
