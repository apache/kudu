// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_
#define KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_

#include "kudu/common/wire_protocol.h"

#include <boost/assign/list_of.hpp>
#include <string>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"

namespace kudu {

Schema GetSimpleTestSchema() {
  return Schema(boost::assign::list_of
      (ColumnSchema("key", UINT32))
      (ColumnSchema("int_val", UINT32))
      (ColumnSchema("string_val", STRING, true)),
      1);
}

void AddTestRowToPB(RowOperationsPB::Type op_type,
                    const Schema& schema,
                    uint32_t key,
                    uint32_t int_val,
                    const string& string_val,
                    RowOperationsPB* ops) {
  DCHECK(schema.initialized());
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetUInt32("key", key));
  CHECK_OK(row.SetUInt32("int_val", int_val));
  CHECK_OK(row.SetStringCopy("string_val", string_val));
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}

void AddTestKeyToPB(RowOperationsPB::Type op_type,
                    const Schema& schema,
                    uint32_t key,
                    RowOperationsPB* ops) {
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetUInt32(0, key));
  RowOperationsPBEncoder enc(ops);
  enc.Add(op_type, row);
}

} // namespace kudu

#endif /* KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_ */
