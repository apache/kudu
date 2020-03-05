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

#include "kudu/gutil/ref_counted.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess_proxy.h"

namespace kudu {

class MetricEntity;

namespace subprocess {

class EchoRequestPB;
class EchoResponsePB;

struct EchoSubprocessMetrics : public SubprocessMetrics {
  explicit EchoSubprocessMetrics(const scoped_refptr<MetricEntity>& entity);
};

typedef SubprocessProxy<EchoRequestPB, EchoResponsePB, EchoSubprocessMetrics> EchoSubprocess;

} // namespace subprocess
} // namespace kudu
