// Copyright 2012 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef KUDU_SERVER_PPROF_DEFAULT_PATH_HANDLERS_H
#define KUDU_SERVER_PPROF_DEFAULT_PATH_HANDLERS_H

namespace kudu {
class Webserver;

// Adds set of path handlers to support pprof profiling of a remote server.
void AddPprofPathHandlers(Webserver* webserver);
}

#endif // KUDU_SERVER_PPROF_DEFAULT_PATH_HANDLERS_H
