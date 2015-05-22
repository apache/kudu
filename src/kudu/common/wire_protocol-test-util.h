// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_
#define KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_

#include "kudu/common/wire_protocol.h"

#include <boost/assign/list_of.hpp>
#include <string>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_operations.h"

namespace kudu {

inline Schema GetSimpleTestSchema() {
  return Schema(boost::assign::list_of
      (ColumnSchema("key", INT32))
      (ColumnSchema("int_val", INT32))
      (ColumnSchema("string_val", STRING, true)),
      1);
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
    CHECK_OK(row.SetStringCopy("string_val", string_val));
  }
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}

inline void AddTestRowToPB(RowOperationsPB::Type op_type,
                           const Schema& schema,
                           int32_t key,
                           int32_t int_val,
                           const string& string_val,
                           RowOperationsPB* ops) {
  AddTestRowWithNullableStringToPB(op_type, schema, key, int_val, string_val.c_str(), ops);
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
