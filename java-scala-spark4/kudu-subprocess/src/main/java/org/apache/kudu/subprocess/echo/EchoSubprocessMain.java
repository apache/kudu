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

package org.apache.kudu.subprocess.echo;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.subprocess.SubprocessConfiguration;
import org.apache.kudu.subprocess.SubprocessExecutor;

@InterfaceAudience.Private
class EchoSubprocessMain {

  public static void main(String[] args) throws Exception {
    SubprocessExecutor subprocessExecutor = new SubprocessExecutor();
    EchoProtocolHandler protocolHandler = new EchoProtocolHandler();
    subprocessExecutor.run(new SubprocessConfiguration(args), protocolHandler, /* timeoutMs= */-1);
  }
}
