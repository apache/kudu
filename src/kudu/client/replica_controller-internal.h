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

#include "kudu/gutil/macros.h"

namespace kudu {
namespace client {

class KuduClientBuilder;
class KuduReplica;

namespace internal {

// This is a class whose sole responsibility is to access tablet replica's
// visibility properties. Those are stored in private members of a few classes
// from the Kudu client API. It's not yet clear whether we want to allow access
// to those properties via the API, so it's safer to not expose them yet.
//
// Having this single class reduces cruft in friend class declarations in the
// client.h file.
class ReplicaController {
 public:
  // Control over tablet replica visibility: expose all or only voter replicas.
  enum class Visibility {
    ALL,    // Expose all replicas: both of voter and non-voter type.
    VOTERS, // Expose only replicas of voter type.
  };

  // Set the specified replica visibility option for the given builder.
  static void SetVisibility(KuduClientBuilder* builder, Visibility visibility);

  static bool is_voter(const KuduReplica& replica);

 private:
  ReplicaController();

  DISALLOW_COPY_AND_ASSIGN(ReplicaController);
};

} // namespace internal
} // namespace client
} // namespace kudu
