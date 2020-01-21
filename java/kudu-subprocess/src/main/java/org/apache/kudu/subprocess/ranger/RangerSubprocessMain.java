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

package org.apache.kudu.subprocess.ranger;

import org.apache.kudu.subprocess.SubprocessExecutor;
import org.apache.yetus.audience.InterfaceAudience;

// The Ranger subprocess that wraps the Kudu Ranger plugin. For the
// plugin to successfully connect to the Ranger service, configurations
// such as ranger-kudu-security.xml (and ranger-kudu-policymgr-ssl.xml
// for SSL connection) are required. To enable auditing in Ranger,
// ranger-kudu-security.xml is needed. The plugin also requires
// core-site.xml to use Hadoop UserGroupInformation for user group
// resolution.
@InterfaceAudience.Private
class RangerSubprocessMain {

  public static void main(String[] args) throws Exception {
    SubprocessExecutor subprocessExecutor = new SubprocessExecutor();
    RangerProtocolHandler protocolProcessor = new RangerProtocolHandler();
    subprocessExecutor.run(args, protocolProcessor, /* timeoutMs= */-1);
  }
}
