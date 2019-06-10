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
#ifndef KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H
#define KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/client.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/macros.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;

namespace client {

class KuduColumnSpec;

class KuduTableAlterer::Data {
 public:
  Data(KuduClient* client, std::string name);
  ~Data();
  Status ToRequest(master::AlterTableRequestPB* req);

  KuduClient* const client_;
  const std::string table_name_;

  Status status_;

  struct Step {
    master::AlterTableRequestPB::StepType step_type;

    // Owned by KuduTableAlterer::Data. Only set when the StepType is
    // [ADD|DROP|RENAME|ALTER]_COLUMN.
    KuduColumnSpec *spec;

    // Lower and upper bound partition keys. Only set when the StepType is
    // [ADD|DROP]_RANGE_PARTITION.
    std::unique_ptr<KuduPartialRow> lower_bound;
    std::unique_ptr<KuduPartialRow> upper_bound;
    KuduTableCreator::RangePartitionBound lower_bound_type;
    KuduTableCreator::RangePartitionBound upper_bound_type;
  };
  std::vector<Step> steps_;

  MonoDelta timeout_;

  bool wait_;

  boost::optional<std::string> rename_to_;

  boost::optional<std::map<std::string, std::string>> new_extra_configs_;

  // Set to true if there are alter partition steps.
  bool has_alter_partitioning_steps = false;

  // Schema of add/drop range partition bound rows.
  const Schema* schema_;

  // Whether to apply the alteration to external catalogs, such as the Hive
  // Metastore. The default value is true.
  bool modify_external_catalogs_ = true;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
