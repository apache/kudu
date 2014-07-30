// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_WRITE_OP_H
#define KUDU_CLIENT_WRITE_OP_H

#include <string>

#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {

class EncodedKey;

namespace client {

namespace internal {
class Batcher;
} // namespace internal

class KuduTable;

// A write operation operates on a single table and partial row.
// The KuduWriteOperation class itself allows the batcher to get to the
// generic information that it needs to process all write operations.
//
// On its own, the class does not represent any specific change and thus cannot
// be constructed independently.
//
// KuduWriteOperation also holds shared ownership of its KuduTable to allow client's
// scope to end while the KuduWriteOperation is still alive.
class KuduWriteOperation {
 public:
  virtual ~KuduWriteOperation();

  // See KuduPartialRow API for field setters, etc.
  const KuduPartialRow& row() const { return row_; }
  KuduPartialRow* mutable_row() { return &row_; }

  virtual std::string ToString() const = 0;
 protected:
  explicit KuduWriteOperation(KuduTable *table);
  virtual RowOperationsPB::Type RowOperationType() const = 0;

  scoped_refptr<KuduTable> const table_;
  KuduPartialRow row_;

 private:
  friend class internal::Batcher;

  // Create and encode the key for this write (key must be set)
  //
  // Caller takes ownership of the allocated memory.
  gscoped_ptr<EncodedKey> CreateKey() const;

  const KuduTable* table() const { return table_.get(); }

  DISALLOW_COPY_AND_ASSIGN(KuduWriteOperation);
};

// A single row insert to be sent to the cluster.
// Row operation is defined by what's in the PartialRow instance here.
// Use mutable_row() to change the row being inserted
// An insert requires all key columns from the table schema to be defined.
class KuduInsert : public KuduWriteOperation {
 public:
  virtual ~KuduInsert();

  virtual std::string ToString() const OVERRIDE { return "INSERT " + row_.ToString(); }

 protected:
  virtual RowOperationsPB::Type RowOperationType() const OVERRIDE {
    return RowOperationsPB::INSERT;
  }

 private:
  friend class KuduTable;
  explicit KuduInsert(KuduTable* table);
};


// A single row update to be sent to the cluster.
// Row operation is defined by what's in the PartialRow instance here.
// Use mutable_row() to change the row being updated.
// An update requires the key columns and at least one other column
// in the schema to be defined.
class KuduUpdate : public KuduWriteOperation {
 public:
  virtual ~KuduUpdate();

  virtual std::string ToString() const OVERRIDE { return "UPDATE " + row_.ToString(); }

 protected:
  virtual RowOperationsPB::Type RowOperationType() const OVERRIDE {
    return RowOperationsPB::UPDATE;
  }

 private:
  friend class KuduTable;
  explicit KuduUpdate(KuduTable* table);
};


// A single row delete to be sent to the cluster.
// Row operation is defined by what's in the PartialRow instance here.
// Use mutable_row() to change the row being deleted
// A delete requires just the key columns to be defined.
class KuduDelete : public KuduWriteOperation {
 public:
  virtual ~KuduDelete();

  virtual std::string ToString() const OVERRIDE { return "DELETE " + row_.ToString(); }

 protected:
  virtual RowOperationsPB::Type RowOperationType() const OVERRIDE {
    return RowOperationsPB::DELETE;
  }

 private:
  friend class KuduTable;
  explicit KuduDelete(KuduTable* table);
};

} // namespace client
} // namespace kudu

#endif
