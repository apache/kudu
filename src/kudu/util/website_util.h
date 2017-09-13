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
#pragma once

#include <string>

namespace kudu {

// Returns a URL for the Kudu website.
std::string KuduUrl();

// Returns the base URL for this Kudu version's documentation.
// Of course, if this version of Kudu isn't released, the link won't work.
std::string KuduDocsUrl();

// Returns a link to this Kudu version's troubleshooting docs. Useful to put in
// error messages for common problems covered in the troubleshooting docs,
// but whose solutions are too complex or varied to put in a log message.
std::string KuduDocsTroubleshootingUrl();

} // namespace kudu
