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

#include "kudu/client/tablet_info_provider-internal.h"

#include <glog/logging.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/common/partition.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace client {
namespace internal {

Status TabletInfoProvider::GetPartitionInfo(KuduClient* client,
                                            const std::string& tablet_id,
                                            Partition* partition) {
  DCHECK(partition);
  scoped_refptr<RemoteTablet> rt;
  Synchronizer sync;
  client->data_->meta_cache_->LookupTabletById(
      client, tablet_id, MonoTime::Max(), &rt,
      sync.AsStatusCallback());
  RETURN_NOT_OK(sync.Wait());
  *partition = rt->partition();
  return Status::OK();
}

} // namespace internal
} // namespace client
} // namespace kudu
