#!/usr/local/bin/thrift -java

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

# DO NOT MODIFY! Copied from
# https://raw.githubusercontent.com/apache/sentry/b71a78ed960702536b35e1f048dc40dfc79992d4/sentry-service/sentry-service-api/src/main/resources/sentry_common_service.thrift
#
# With edits:
#   - Change cpp namespace to 'sentry' to match the Kudu codebase style.

namespace java org.apache.sentry.service.thrift
namespace php sentry.service.thrift
namespace cpp sentry

const i32 TSENTRY_SERVICE_V1 = 1;
// Made a backward incompatible change when adding column level privileges.
// We also added generalized model in this version
const i32 TSENTRY_SERVICE_V2 = 2;

const i32 TSENTRY_STATUS_OK = 0;
const i32 TSENTRY_STATUS_ALREADY_EXISTS = 1;
const i32 TSENTRY_STATUS_NO_SUCH_OBJECT = 2;
const i32 TSENTRY_STATUS_RUNTIME_ERROR = 3;
const i32 TSENTRY_STATUS_INVALID_INPUT = 4;
const i32 TSENTRY_STATUS_ACCESS_DENIED = 5;
const i32 TSENTRY_STATUS_THRIFT_VERSION_MISMATCH = 6;

struct TSentryResponseStatus {
1: required i32 value,
// message will be set to empty string when status is OK
2: required string message
3: optional string stack
}
