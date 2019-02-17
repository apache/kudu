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

#include <iostream>

#include <glog/logging.h>

#include "kudu/tserver/tablet_server_runner.h"
#include "kudu/util/flags.h"
#include "kudu/util/init.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tserver {

static int TabletServerMain(int argc, char** argv) {
  RETURN_MAIN_NOT_OK(InitKudu(), "InitKudu() failed", 1);
  SetTabletServerFlagDefaults();
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 2;
  }
  InitGoogleLoggingSafe(argv[0]);
  RETURN_MAIN_NOT_OK(RunTabletServer(), "RunTabletServer() failed", 3);
  return 0;
}

} // namespace tserver
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tserver::TabletServerMain(argc, argv);
}
