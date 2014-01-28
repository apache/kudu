// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_
#define KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_

#include "common/wire_protocol.h"

#include <string>

#include "common/partial_row.h"
#include "common/row.h"
#include "common/row_changelist.h"

namespace kudu {

void CreateTestSchema(Schema* schema) {
  CHECK(schema) << "Schema cannot be null.";
  CHECK_OK(schema->Reset(boost::assign::list_of
                         (ColumnSchema("key", UINT32))
                         (ColumnSchema("int_val", UINT32))
                         (ColumnSchema("string_val", STRING, true)), 1));
}

void AddTestRowToPB(const Schema& schema,
                    uint32_t key,
                    uint32_t int_val,
                    const string& string_val,
                    PartialRowsPB* block) {
  PartialRow row(&schema);
  row.SetUInt32("key", key);
  row.SetUInt32("int_val", int_val);
  row.SetStringCopy("string_val", string_val);
  row.AppendToPB(block);
}

void AddTestKeyToBlock(const Schema& key_schema,
                       uint32_t key,
                       RowwiseRowBlockPB* block) {
  RowBuilder rb(key_schema);
  rb.AddUint32(key);
  AddRowToRowBlockPB(rb.row(), block);
}

void AddTestDeletionToRowBlockAndBuffer(const Schema& schema,
                                        uint32_t key,
                                        RowwiseRowBlockPB* block,
                                        faststring* buf) {
  // Write the key.
  AddTestKeyToBlock(schema.CreateKeyProjection(), key, block);

  // Write the mutation.
  faststring tmp;
  RowChangeListEncoder encoder(schema, &tmp);
  encoder.SetToDelete();
  PutFixed32LengthPrefixedSlice(buf, Slice(tmp));
}

void AddTestMutationToRowBlockAndBuffer(const Schema& schema,
                                        uint32_t key,
                                        uint32_t new_int_val,
                                        const Slice& new_string_val,
                                        RowwiseRowBlockPB* block,
                                        faststring* buf) {
  // Write the key.
  AddTestKeyToBlock(schema.CreateKeyProjection(), key, block);

  // Write the mutation.
  faststring tmp;
  RowChangeListEncoder encoder(schema, &tmp);
  encoder.AddColumnUpdate(1, &new_int_val);
  encoder.AddColumnUpdate(2, &new_string_val);
  PutFixed32LengthPrefixedSlice(buf, Slice(tmp));
}

} // namespace kudu

#endif /* KUDU_COMMON_WIRE_PROTOCOL_TEST_UTIL_H_ */
