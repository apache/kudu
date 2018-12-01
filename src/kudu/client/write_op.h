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
#ifndef KUDU_CLIENT_WRITE_OP_H
#define KUDU_CLIENT_WRITE_OP_H

#include <stdint.h>

#include <string>

#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/partial_row.h"
#include "kudu/util/kudu_export.h"

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

namespace kudu {

class EncodedKey;

namespace client {

namespace internal {
class Batcher;
class ErrorCollector;
class WriteRpc;
} // namespace internal

class KuduTable;

/// @brief A single-row write operation to be sent to a Kudu table.
///
/// This is the abstract base class from which the particular row operations
/// (KuduInsert, KuduUpdate, etc) are derived. These subclasses are instantiated
/// by KuduTable::NewInsert(), etc.
///
/// The row key, as well as the columns to be inserted or updated are set using
/// the embedded KuduPartialRow object accessible via mutable_row().
///
/// Typical usage example:
/// @code
///   KuduInsert* t = table->NewInsert();
///   KUDU_CHECK_OK(t->mutable_row()->SetInt32("key", 1234));
///   KUDU_CHECK_OK(t->mutable_row()->SetStringCopy("foo", "bar"));
///   session->Apply(t);
/// @endcode
class KUDU_EXPORT KuduWriteOperation {
 public:
  /// @brief Write operation types.
  enum Type {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    UPSERT = 4,
    INSERT_IGNORE = 5
  };
  virtual ~KuduWriteOperation();

  /// @note To work with a row, use the KuduPartialRow API for field getters,
  ///   etc.
  /// @return Immutable reference to the corresponding row-like object.
  const KuduPartialRow& row() const { return row_; }

  /// @note To work with a row, use the KuduPartialRow API for field setters,
  ///   etc.
  /// @return Pointer to the corresponding row-like object.
  KuduPartialRow* mutable_row() { return &row_; }

  /// @return String representation of the operation.
  ///
  /// @internal
  /// @note this method does note redact row values. The
  ///   caller must handle redaction themselves, if necessary.
  virtual std::string ToString() const = 0;
 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// Create a write operation on the specified table.
  ///
  /// @param [in] table
  ///   Smart pointer to the target table.
  explicit KuduWriteOperation(const sp::shared_ptr<KuduTable>& table);

  /// @return Type of the operation.
  virtual Type type() const = 0;

  /// KuduWriteOperation holds shared ownership of its KuduTable
  /// to allow the client's scope to end while the KuduWriteOperation
  /// is still alive.
  sp::shared_ptr<KuduTable> const table_;

  /// The partial row to access schema-related properties.
  KuduPartialRow row_;

  /// @endcond

 private:
  friend class internal::Batcher;
  friend class internal::WriteRpc;
  friend class internal::ErrorCollector;
  friend class KuduSession;

  // Create and encode the key for this write (key must be set)
  //
  // Caller takes ownership of the allocated memory.
  EncodedKey* CreateKey() const;

  const KuduTable* table() const { return table_.get(); }

  // Return the number of bytes required to buffer this operation,
  // including direct and indirect data. Once called, the result is cached
  // so subsequent calls will return the size previously computed.
  int64_t SizeInBuffer() const;

  mutable int64_t size_in_buffer_;

  DISALLOW_COPY_AND_ASSIGN(KuduWriteOperation);
};


/// @brief A single row insert to be sent to the cluster.
///
/// @pre An insert requires all key columns to be set, as well as all
///   columns which do not have default values.
class KUDU_EXPORT KuduInsert : public KuduWriteOperation {
 public:
  virtual ~KuduInsert();

  /// @copydoc KuduWriteOperation::ToString()
  virtual std::string ToString() const OVERRIDE { return "INSERT " + row_.ToString(); }

 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// @copydoc KuduWriteOperation::type()
  virtual Type type() const OVERRIDE {
    return INSERT;
  }

  /// @endcond

 private:
  friend class KuduTable;
  explicit KuduInsert(const sp::shared_ptr<KuduTable>& table);
};


/// @brief A single row insert ignore to be sent to the cluster, duplicate row errors are ignored.
///
/// @pre An insert ignore requires all key columns to be set, as well as all
///   columns which do not have default values.
class KUDU_EXPORT KuduInsertIgnore : public KuduWriteOperation {
 public:
  virtual ~KuduInsertIgnore();

  /// @copydoc KuduWriteOperation::ToString()
  virtual std::string ToString() const OVERRIDE { return "INSERT IGNORE " + row_.ToString(); }

 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// @copydoc KuduWriteOperation::type()
  virtual Type type() const OVERRIDE {
    return INSERT_IGNORE;
  }

  /// @endcond

 private:
  friend class KuduTable;
  explicit KuduInsertIgnore(const sp::shared_ptr<KuduTable>& table);
};


/// @brief A single row upsert to be sent to the cluster.
///
/// See KuduInsert for more details.
class KUDU_EXPORT KuduUpsert : public KuduWriteOperation {
 public:
  virtual ~KuduUpsert();

  /// @copydoc KuduWriteOperation::ToString()
  virtual std::string ToString() const OVERRIDE { return "UPSERT " + row_.ToString(); }

 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// @copydoc KuduWriteOperation::type()
  virtual Type type() const OVERRIDE {
    return UPSERT;
  }

  /// @endcond

 private:
  friend class KuduTable;
  explicit KuduUpsert(const sp::shared_ptr<KuduTable>& table);
};


/// @brief A single row update to be sent to the cluster.
///
/// @pre An update requires the key columns and at least one other column
///   in the schema to be set in the embedded KuduPartialRow object.
class KUDU_EXPORT KuduUpdate : public KuduWriteOperation {
 public:
  virtual ~KuduUpdate();

  /// @copydoc KuduWriteOperation::ToString()
  virtual std::string ToString() const OVERRIDE { return "UPDATE " + row_.ToString(); }

 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// @copydoc KuduWriteOperation::type()
  virtual Type type() const OVERRIDE {
    return UPDATE;
  }

  /// @endcond

 private:
  friend class KuduTable;
  explicit KuduUpdate(const sp::shared_ptr<KuduTable>& table);
};


/// @brief A single row delete to be sent to the cluster.
///
/// @pre A delete requires the key columns to be set in the embedded
///   KuduPartialRow object.
class KUDU_EXPORT KuduDelete : public KuduWriteOperation {
 public:
  virtual ~KuduDelete();

  /// @copydoc KuduWriteOperation::ToString()
  virtual std::string ToString() const OVERRIDE { return "DELETE " + row_.ToString(); }

 protected:
  /// @cond PROTECTED_MEMBERS_DOCUMENTED

  /// @copydoc KuduWriteOperation::type()
  virtual Type type() const OVERRIDE {
    return DELETE;
  }

  /// @endcond

 private:
  friend class KuduTable;
  explicit KuduDelete(const sp::shared_ptr<KuduTable>& table);
};

} // namespace client
} // namespace kudu

#endif
