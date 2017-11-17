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

#include "kudu/client/replica_controller-internal.h"

#include "kudu/client/client.h"
#include "kudu/client/client_builder-internal.h"
#include "kudu/client/replica-internal.h"

namespace kudu {
namespace client {
namespace internal {

ReplicaController::ReplicaController() {}

void ReplicaController::SetVisibility(KuduClientBuilder* builder, Visibility visibility) {
  builder->data_->replica_visibility_ = visibility;
}

bool ReplicaController::is_voter(const KuduReplica& replica) {
  return replica.data_->is_voter_;
}

} // namespace internal
} // namespace client
} // namespace kudu
