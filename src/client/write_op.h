// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_WRITE_OP_H
#define KUDU_CLIENT_WRITE_OP_H

#include "common/partial_row.h"
#include "common/encoded_key.h"
#include "gutil/ref_counted.h"

#include <string>

namespace kudu {
namespace client {

class KuduTable;

// A write operation operates on a single KuduTable and single Partial row
// The WriteOperation class itself allows the internal::batcher to get to the
// generic information that it needs to process all write operations.
//
// On its own, the class does not represent any specific change and thus cannot
// be constructed independently.
//
// WriteOperation also holds shared ownership of its KuduTable to allow client's
// scope to end while the WriteOperation is still alive.
class WriteOperation {
 public:
  virtual ~WriteOperation();

  const KuduTable* table() const { return table_.get(); }
  const PartialRow& row() const { return row_; }

  // See PartialRow API for field setters, etc.
  PartialRow* mutable_row() { return &row_; }

  // Create and encode the key for this write (key must be set)
  //
  // Caller takes ownership of the allocated memory.
  gscoped_ptr<EncodedKey> CreateKey() const;

  virtual RowOperationsPB::Type RowOperationType() const = 0;
  virtual std::string ToString() const = 0;
 protected:
  explicit WriteOperation(KuduTable *table);
  scoped_refptr<KuduTable> const table_;
  PartialRow row_;

  DISALLOW_COPY_AND_ASSIGN(WriteOperation);
};

// A single row insert to be sent to the cluster.
// Row operation is defined by what's in the PartialRow instance here.
// Use mutable_row() to change the row being inserted
class Insert : public WriteOperation {
 public:
  virtual ~Insert();

  virtual RowOperationsPB::Type RowOperationType() const {
    return RowOperationsPB::INSERT;
  }
  virtual std::string ToString() const { return "INSERT " + row_.ToString(); }

 private:
  friend class KuduTable;
  explicit Insert(KuduTable* table);
};

} // namespace client
} // namespace kudu

#endif
