// Copyright (c) 2014, Cloudera,inc.

#include "client/write_op.h"

#include "client/client.h"
#include "common/encoded_key.h"
#include "common/row.h"

namespace kudu {
namespace client {

// WriteOperation --------------------------------------------------------------

KuduWriteOperation::KuduWriteOperation(KuduTable *table)
  : table_(table),
    row_(table->schema().schema_.get()) {
}

KuduWriteOperation::~KuduWriteOperation() {}

gscoped_ptr<EncodedKey> KuduWriteOperation::CreateKey() const {
  CHECK(row_.IsKeySet()) << "key must be set";

  ConstContiguousRow row(*row_.schema(), row_.row_data_);
  EncodedKeyBuilder kb(row.schema());
  for (int i = 0; i < row.schema().num_key_columns(); i++) {
    kb.AddColumnKey(row.cell_ptr(i));
  }
  gscoped_ptr<EncodedKey> key(kb.BuildEncodedKey());
  return key.Pass();
}

// Insert -----------------------------------------------------------------------

KuduInsert::KuduInsert(KuduTable *table)
  : KuduWriteOperation(table) {
}

KuduInsert::~KuduInsert() {}

// Update -----------------------------------------------------------------------

KuduUpdate::KuduUpdate(KuduTable *table)
  : KuduWriteOperation(table) {
}

KuduUpdate::~KuduUpdate() {}

// Delete -----------------------------------------------------------------------

KuduDelete::KuduDelete(KuduTable *table)
  : KuduWriteOperation(table) {
}

KuduDelete::~KuduDelete() {}

} // namespace client
} // namespace kudu
