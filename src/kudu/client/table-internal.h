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
#ifndef KUDU_CLIENT_TABLE_INTERNAL_H
#define KUDU_CLIENT_TABLE_INTERNAL_H

#include <string>

#include "kudu/client/client.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

namespace client {

class KuduTable::Data {
 public:
  Data(sp::shared_ptr<KuduClient> client,
       std::string name,
       std::string id,
       int num_replicas,
       const KuduSchema& schema,
       PartitionSchema partition_schema);
  ~Data();

  template<class PredicateMakerFunc>
  KuduPredicate* MakePredicate(const Slice& col_name,
                               const PredicateMakerFunc& f) {
    StringPiece name_sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
    int col_idx = schema_.schema_->find_column(name_sp);
    if (col_idx == Schema::kColumnNotFound) {
      // Since the new predicate functions don't return errors, instead we create a special
      // predicate that just returns the errors when we add it to the scanner.
      //
      // This allows the predicate API to be more "fluent".
      return new KuduPredicate(new ErrorPredicateData(
          Status::NotFound("column not found", col_name)));
    }
    return f(schema_.schema_->column(col_idx));
  }

  sp::shared_ptr<KuduClient> client_;

  const std::string name_;
  const std::string id_;
  const int num_replicas_;

  // TODO: figure out how we deal with a schema change from the client perspective.
  // Do we make them call a RefreshSchema() method? Or maybe reopen the table and get
  // a new KuduTable instance (which would simplify the object lifecycle a little?)
  const KuduSchema schema_;
  const PartitionSchema partition_schema_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
