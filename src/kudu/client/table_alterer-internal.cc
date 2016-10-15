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

#include "kudu/client/table_alterer-internal.h"

#include <algorithm>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/client/schema-internal.h"
#include "kudu/client/schema.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/master/master.pb.h"

using std::string;

namespace kudu {
namespace client {

using master::AlterTableRequestPB;

KuduTableAlterer::Data::Data(KuduClient* client, string name)
    : client_(client),
      table_name_(std::move(name)),
      wait_(true),
      schema_(nullptr) {
}

KuduTableAlterer::Data::~Data() {
  for (Step& s : steps_) {
    delete s.spec;
  }
}

Status KuduTableAlterer::Data::ToRequest(AlterTableRequestPB* req) {
  if (!status_.ok()) {
    return status_;
  }

  if (!rename_to_.is_initialized() && steps_.empty()) {
    return Status::InvalidArgument("No alter steps provided");
  }

  req->Clear();
  req->mutable_table()->set_table_name(table_name_);
  if (rename_to_.is_initialized()) {
    req->set_new_table_name(rename_to_.get());
  }

  if (schema_ != nullptr) {
    RETURN_NOT_OK(SchemaToPB(*schema_, req->mutable_schema(),
                             SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_WRITE_DEFAULT));
  }

  for (const Step& s : steps_) {
    AlterTableRequestPB::Step* pb_step = req->add_alter_schema_steps();
    pb_step->set_type(s.step_type);

    switch (s.step_type) {
      case AlterTableRequestPB::ADD_COLUMN:
      {
        KuduColumnSchema col;
        RETURN_NOT_OK(s.spec->ToColumnSchema(&col));
        ColumnSchemaToPB(*col.col_,
                         pb_step->mutable_add_column()->mutable_schema(),
                         SCHEMA_PB_WITHOUT_WRITE_DEFAULT);
        break;
      }
      case AlterTableRequestPB::DROP_COLUMN:
      {
        pb_step->mutable_drop_column()->set_name(s.spec->data_->name);
        break;
      }
      case AlterTableRequestPB::ALTER_COLUMN:
      {
        if (s.spec->data_->has_type ||
            s.spec->data_->has_nullable ||
            s.spec->data_->primary_key) {
          return Status::NotSupported("unsupported alter operation",
                                      s.spec->data_->name);
        }
        if (!s.spec->data_->has_rename_to &&
            !s.spec->data_->has_default &&
            !s.spec->data_->default_val &&
            !s.spec->data_->remove_default &&
            !s.spec->data_->has_encoding &&
            !s.spec->data_->has_compression &&
            !s.spec->data_->has_block_size) {
          return Status::InvalidArgument("no alter operation specified",
                                         s.spec->data_->name);
        }
        // If the alter is solely a column rename, fall back to using
        // RENAME_COLUMN, for backwards compatibility.
        // TODO(wdb) Change this when compat can be broken.
        if (!s.spec->data_->has_default &&
            !s.spec->data_->default_val &&
            !s.spec->data_->remove_default &&
            !s.spec->data_->has_encoding &&
            !s.spec->data_->has_compression &&
            !s.spec->data_->has_block_size) {
          pb_step->set_type(AlterTableRequestPB::RENAME_COLUMN);
          pb_step->mutable_rename_column()->set_old_name(s.spec->data_->name);
          pb_step->mutable_rename_column()->set_new_name(s.spec->data_->rename_to);
          break;
        }
        ColumnSchemaDelta col_delta(s.spec->data_->name);
        RETURN_NOT_OK(s.spec->ToColumnSchemaDelta(&col_delta));
        auto* alter_pb = pb_step->mutable_alter_column();
        ColumnSchemaDeltaToPB(col_delta, alter_pb->mutable_delta());
        pb_step->set_type(AlterTableRequestPB::ALTER_COLUMN);
        break;
      }
      case AlterTableRequestPB::ADD_RANGE_PARTITION:
      {
        RowOperationsPBEncoder encoder(pb_step->mutable_add_range_partition()
                                              ->mutable_range_bounds());
        RowOperationsPB_Type lower_bound_type =
          s.lower_bound_type == KuduTableCreator::INCLUSIVE_BOUND ?
          RowOperationsPB::RANGE_LOWER_BOUND :
          RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND;

        RowOperationsPB_Type upper_bound_type =
          s.upper_bound_type == KuduTableCreator::EXCLUSIVE_BOUND ?
          RowOperationsPB::RANGE_UPPER_BOUND :
          RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND;

        encoder.Add(lower_bound_type, *s.lower_bound);
        encoder.Add(upper_bound_type, *s.upper_bound);
        break;
      }
      case AlterTableRequestPB::DROP_RANGE_PARTITION:
      {
        RowOperationsPBEncoder encoder(pb_step->mutable_drop_range_partition()
                                              ->mutable_range_bounds());
        RowOperationsPB_Type lower_bound_type =
          s.lower_bound_type == KuduTableCreator::INCLUSIVE_BOUND ?
          RowOperationsPB::RANGE_LOWER_BOUND :
          RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND;

        RowOperationsPB_Type upper_bound_type =
          s.upper_bound_type == KuduTableCreator::EXCLUSIVE_BOUND ?
          RowOperationsPB::RANGE_UPPER_BOUND :
          RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND;

        encoder.Add(lower_bound_type, *s.lower_bound);
        encoder.Add(upper_bound_type, *s.upper_bound);
        break;
      }
      default:
        LOG(FATAL) << "unknown step type " << AlterTableRequestPB::StepType_Name(s.step_type);
    }
  }
  return Status::OK();
}

} // namespace client
} // namespace kudu
