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

#ifndef KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_
#define KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_

#include "kudu/common/wire_protocol.h"

#include <map>
#include <string>

#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_operations.h"

namespace kudu {

inline Schema GetSimpleTestSchema() {
  return Schema({ ColumnSchema("key", INT32),
                  ColumnSchema("int_val", INT32),
                  ColumnSchema("string_val", STRING, true) },
                1);
}

inline client::KuduSchema GetAutoIncrementingTestSchema() {
  client::KuduSchema kudu_schema;
  client::KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
  b.AddColumn("int_val")->Type(client::KuduColumnSchema::INT32);
  b.AddColumn("string_val")->Type(client::KuduColumnSchema::STRING)->Nullable();
  CHECK_OK(b.Build(&kudu_schema));
  return kudu_schema;
}

inline void RowAppendColumn(KuduPartialRow* row,
                            const std::map<std::string, std::string>& columns) {
  for (const auto& column : columns) {
    CHECK_OK(row->SetStringCopy(column.first.c_str(), column.second.c_str()));
  }
}

inline void AddTestRowWithNullableStringToPB(RowOperationsPB::Type op_type,
                                             const Schema& schema,
                                             int32_t key,
                                             int32_t int_val,
                                             const char* string_val,
                                             RowOperationsPB* ops) {
  DCHECK(schema.initialized());
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetInt32("key", key));
  CHECK_OK(row.SetInt32("int_val", int_val));
  if (string_val) {
    RowAppendColumn(&row, {{"string_val", std::string(string_val)}});
  }
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}

inline void AddTestRowWithNullableColumnsStringToPB(
    RowOperationsPB::Type op_type, const Schema& schema,
    int32_t key, int32_t int_val, const char* string_val,
    const std::map<std::string, std::string>& columns,
    RowOperationsPB* ops) {
  DCHECK(schema.initialized());
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetInt32("key", key));
  CHECK_OK(row.SetInt32("int_val", int_val));
  if (string_val) {
    RowAppendColumn(&row, {{"string_val", std::string(string_val)}});
  }
  if (!columns.empty()) {
    RowAppendColumn(&row, columns);
  }
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}


inline void AddTestRowToPB(RowOperationsPB::Type op_type,
                           const Schema& schema,
                           int32_t key,
                           int32_t int_val,
                           const std::string& string_val,
                           RowOperationsPB* ops) {
  AddTestRowWithNullableStringToPB(op_type, schema, key, int_val, string_val.c_str(), ops);
}

inline void AddTestRowToPBAppendColumns(RowOperationsPB::Type op_type,
                                        const Schema& schema, int32_t key,
                                        int32_t int_val,
                                        const std::string& string_val,
                                        const std::map<std::string, std::string>& columns,
                                        RowOperationsPB* ops) {
  AddTestRowWithNullableColumnsStringToPB(op_type, schema,
                                          key, int_val, string_val.c_str(),
                                          columns, ops);
}

inline void AddTestKeyToPB(RowOperationsPB::Type op_type,
                    const Schema& schema,
                    int32_t key,
                    RowOperationsPB* ops) {
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetInt32(0, key));
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}

} // namespace kudu

#endif /* KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_ */
