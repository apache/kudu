// Copyright (c) 2014, Cloudera,inc.

#include "client/write_op.h"

#include "client/client.h"
#include "common/row.h"

namespace kudu {
namespace client {

// WriteOperation --------------------------------------------------------------

WriteOperation::WriteOperation(KuduTable *table)
  : table_(table),
    row_(table->schema().schema_.get()) {
}

WriteOperation::~WriteOperation() {}

gscoped_ptr<EncodedKey> WriteOperation::CreateKey() const {
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

Insert::Insert(KuduTable *table)
  : WriteOperation(table) {
}

Insert::~Insert() {}

// Update -----------------------------------------------------------------------

Update::Update(KuduTable *table)
  : WriteOperation(table) {
}

Update::~Update() {}

// Delete -----------------------------------------------------------------------

Delete::Delete(KuduTable *table)
  : WriteOperation(table) {
}

Delete::~Delete() {}

} // namespace client
} // namespace kudu
